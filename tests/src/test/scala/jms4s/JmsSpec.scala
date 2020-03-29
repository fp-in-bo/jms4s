package jms4s

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ Blocker, IO, Resource }
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import jms4s.config._
import jms4s.ibmmq.ibmMQ._
import jms4s.model.{ SessionType, TransactionResult }
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  val blocker: Resource[IO, Blocker] = Blocker.apply

  val connectionRes: Resource[IO, JmsConnection[IO]] = blocker.flatMap(
    blocker =>
      makeConnection[IO](
        Config(
          qm = QueueManager("QM1"),
          endpoints = NonEmptyList.one(Endpoint("localhost", 1414)),
          // the current docker image seems to be misconfigured, so I need to use admin channel/auth in order to test topic
          // but maybe it's just me not understanding something properly.. as usual
          //          channel = Channel("DEV.APP.SVRCONN"),
          //          username = Some(Username("app")),
          //          password = None,
          channel = Channel("DEV.ADMIN.SVRCONN"),
          username = Some(Username("admin")),
          password = Some(Password("passw0rd")),
          clientId = "jms-specs"
        ),
        blocker
      )
  )

  val nMessages: Int              = 50
  val poolSize: Int               = 4
  val timeout: FiniteDuration     = 2.seconds
  val delay: FiniteDuration       = 500.millis
  val topicName: TopicName        = TopicName("DEV.BASE.TOPIC")
  val inputQueueName: QueueName   = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName = QueueName("DEV.QUEUE.3")

  "Basic jms ops" - {
    val res = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      queue         <- Resource.liftF(session.createQueue(inputQueueName))
      topic         <- Resource.liftF(session.createTopic(topicName))
      queueConsumer <- session.createConsumer(queue)
      topicConsumer <- session.createConsumer(topic)
      queueProducer <- session.createProducer(queue)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage("body"))
    } yield (queueConsumer, topicConsumer, queueProducer, topicProducer, msg)

    val topicRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      topic         <- Resource.liftF(session.createTopic(topicName))
      topicConsumer <- session.createConsumer(topic)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage("body"))
    } yield (topicConsumer, topicProducer, msg)

    "publish to a queue and then receive" in {
      res.use {
        case (queueConsumer, _, queueProducer, _, msg) =>
          for {
            _        <- queueProducer.send(msg)
            received <- queueConsumer.receiveJmsMessage
            text     <- received.asJmsTextMessage.flatMap(_.getText)
          } yield text.shouldBe("body")
      }
    }
    "publish and then receive with a delay" in {
      def receiveAfter(received: Ref[IO, Set[String]], duration: FiniteDuration) =
        for {
          _        <- IO.sleep(duration / 2)
          tooEarly <- received.get
          _ <- if (tooEarly.nonEmpty)
                IO.raiseError(new RuntimeException("Delay has not been respected!"))
              else
                IO.unit
          _      <- IO.sleep(duration / 2)
          gotcha <- received.get
        } yield gotcha

      res.use {
        case (queueConsumer, _, queueProducer, _, msg) =>
          for {
            _        <- queueProducer.setDeliveryDelay(delay)
            _        <- queueProducer.send(msg)
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- (for {
                              msg  <- queueConsumer.receiveJmsMessage
                              tm   <- msg.asJmsTextMessage
                              body <- tm.getText
                              _    <- received.update(_ + body)
                            } yield ()).start
            gotcha <- receiveAfter(received, delay).guarantee(consumerFiber.cancel)
          } yield gotcha.shouldBe(Set("body"))
      }
    }
    "publish to a topic and then receive" in {
      topicRes.use {
        case (topicConsumer, topicProducer, msg) =>
          for {
            _ <- (IO.delay(10.millis) >> topicProducer.send(msg)).start
            rec <- topicConsumer.receiveJmsMessage
                    .flatMap(_.asJmsTextMessage)
                    .flatMap(_.getText)
          } yield rec.shouldBe("body")
      }
    }
  }

  "High level api" - {
    s"publish $nMessages messages a nd then consume them concurrently with local transactions" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection         <- connectionRes
        session            <- connection.createSession(SessionType.AutoAcknowledge)
        queue              <- Resource.liftF(session.createQueue(inputQueueName))
        producer           <- session.createProducer(queue)
        bodies             = (0 until nMessages).map(i => s"$i")
        messages           <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createQueueTransactedConsumer(connection, inputQueueName, poolSize)
      } yield (transactedConsumer, producer, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- transactedConsumer.handle { rec =>
                              for {
                                tm   <- rec.message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield TransactionResult.Commit
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield receivedMessages.shouldBe(bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to an other queue" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createQueueTransactedConsumerToProducer(
                               connection,
                               inputQueueName,
                               outputQueueName1,
                               poolSize
                             )
      } yield (transactedConsumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- transactedConsumer.handle { rec =>
                                        rec.resource.producing
                                          .publish(rec.message)
                                          .as(TransactionResult.Commit)
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield receivedMessages.shouldBe(bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to other queues" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection      <- connectionRes
        session         <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue      <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue1    <- Resource.liftF(session.createQueue(outputQueueName1))
        outputQueue2    <- Resource.liftF(session.createQueue(outputQueueName2))
        inputProducer   <- session.createProducer(inputQueue)
        outputConsumer1 <- session.createConsumer(outputQueue1)
        outputConsumer2 <- session.createConsumer(outputQueue2)
        bodies          = (0 until nMessages).map(i => s"$i")
        messages        <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createQueueTransactedConsumerToProducers(
                               connection,
                               inputQueueName,
                               NonEmptyList.of(outputQueueName1, outputQueueName2),
                               poolSize
                             )
      } yield (transactedConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- transactedConsumer.handle { r =>
                                        for {
                                          tm   <- r.message.asJmsTextMessage
                                          text <- tm.getText
                                          _ <- if (text.toInt % 2 == 0)
                                                r.resource.producing.lookup(outputQueueName1).get.publish(tm)
                                              else
                                                r.resource.producing.lookup(outputQueueName2).get.publish(tm)
                                        } yield TransactionResult.Commit
                                      }.start
            _         <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queues...")
            received1 <- Ref.of[IO, Set[String]](Set())
            received2 <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- ((
                                 receiveUntil(outputConsumer1, received1, nMessages / 2),
                                 receiveUntil(outputConsumer2, received2, nMessages / 2)
                               ).parTupled.timeout(timeout) >> (received1.get, received2.get).mapN(_ ++ _))
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield receivedMessages.shouldBe(bodies)
      }
    }
  }

  private def receiveUntil(
    consumer: JmsMessageConsumer[IO],
    received: Ref[IO, Set[String]],
    nMessages: Int
  ): IO[Set[String]] =
    (consumer.receiveJmsMessage.flatMap { msg =>
      msg.asJmsTextMessage.flatMap(tm => tm.getText.flatMap(body => received.update(_ + body)))
    } *> received.get).iterateUntil(_.size == nMessages)
}
