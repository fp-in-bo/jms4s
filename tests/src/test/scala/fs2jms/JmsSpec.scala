package fs2jms

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ Blocker, IO, Resource }
import cats.implicits._
import fs2jms.JmsConsumerPool.Received.{ ReceivedTextMessage, ReceivedUnsupportedMessage }
import fs2jms.JmsMessageConsumer.UnsupportedMessage
import fs2jms.config._
import fs2jms.ibmmq.ibmMQ._
import fs2jms.model.{ SessionType, TransactionResult }
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
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
          channel = Channel("DEV.APP.SVRCONN"),
          username = Some(Username("app")),
          password = None
        ),
        blocker
      )
  )

  val nMessages: Int              = 100
  val poolSize: Int               = 10
  val timeout: FiniteDuration     = 1.seconds
  val delay: FiniteDuration       = 20.millis
  val inputQueueName: QueueName   = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName = QueueName("DEV.QUEUE.3")

  "Basic jms ops" - {
    val res = for {
      connection <- connectionRes
      session    <- connection.createSession(SessionType.AutoAcknowledge)
      queue      <- Resource.liftF(session.createQueue(inputQueueName))
      consumer   <- session.createConsumer(queue)
      producer   <- session.createProducer(queue)
      msg        <- Resource.liftF(session.createTextMessage("body"))
    } yield (consumer, producer, msg)

    "publish and then receive" in {
      res.use {
        case (consumer, producer, msg) =>
          for {
            _        <- producer.send(msg)
            received <- consumer.receiveTextMessage
            text <- received match {
                     case Left(_)   => IO.raiseError(new RuntimeException("Received something bad!"))
                     case Right(tm) => tm.getText
                   }
          } yield text.shouldBe("body")
      }
    }

    "publish and then receive with a delay" in {
      def receiveAfter(received: Ref[IO, Set[String]], duration: FiniteDuration) =
        for {
          tooEarly <- received.get
          _ <- if (tooEarly.nonEmpty)
                IO.raiseError(new RuntimeException("Delay has not been respected!"))
              else
                IO.unit
          _      <- IO.sleep(duration)
          gotcha <- received.get
        } yield gotcha

      res.use {
        case (consumer, producer, msg) =>
          for {
            _        <- producer.setDeliveryDelay(delay)
            _        <- producer.send(msg)
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- consumer.receiveTextMessage.flatMap {
                              case Left(_)   => IO.raiseError(new RuntimeException("Received something bad!"))
                              case Right(tm) => tm.getText.flatMap(body => received.update(_ + body))

                            }.start
            gotcha <- receiveAfter(received, delay).guarantee(consumerFiber.cancel)
          } yield gotcha.shouldBe(Set("body"))
      }
    }
  }

  "High level api" - {
    s"publish $nMessages messages and then consume them concurrently with local transactions" in {
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
            consumerFiber <- transactedConsumer.handle {
                              case ReceivedUnsupportedMessage(_, _) =>
                                received
                                  .update(_ + "") // failing..
                                  .as(TransactionResult.Commit)
                              case ReceivedTextMessage(tm, _) =>
                                tm.getText
                                  .flatMap(body => received.update(_ + body)) // accumulate received messages
                                  .as(TransactionResult.Commit)
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
            consumerToProducerFiber <- transactedConsumer.handle {
                                        case ReceivedUnsupportedMessage(_, _) =>
                                          IO.pure(TransactionResult.Commit)
                                        case ReceivedTextMessage(tm, res) =>
                                          res.producing.publish(tm).as(TransactionResult.Commit)
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
            consumerToProducerFiber <- transactedConsumer.handle {
                                        case ReceivedUnsupportedMessage(_, _) =>
                                          IO.pure(TransactionResult.Commit)
                                        case ReceivedTextMessage(tm, res) =>
                                          tm.getText
                                            .flatMap(
                                              text =>
                                                if (text.toInt % 2 == 0)
                                                  res.producing.lookup(outputQueueName1).get.publish(tm)
                                                else
                                                  res.producing.lookup(outputQueueName2).get.publish(tm)
                                            )
                                            .as(TransactionResult.Commit)
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
    (consumer.receiveTextMessage.flatMap { // receive
      case Left(UnsupportedMessage(_)) =>
        received.update(_ + "") // failing..
      case Right(tm) => // accumulate messages from output queue
        tm.getText.flatMap(body => received.update(_ + body))
    } *> received.get).iterateUntil(_.size == nMessages)
}
