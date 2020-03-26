package fs2jms

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ Blocker, IO, Resource }
import cats.implicits._
import fs2jms.JmsPool.Received.{ ReceivedTextMessage, ReceivedUnsupportedMessage }
import fs2jms.config._
import fs2jms.ibmmq.ibmMQ._
import fs2jms.model.{ SessionType, TransactionResult }
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.Failed
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  val blocker: Resource[IO, Blocker] = Blocker.apply

  val connectionRes: Resource[IO, JmsQueueConnection[IO]] = blocker.flatMap(
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

  "Basic jms ops" - {
    val queueName = QueueName("DEV.QUEUE.1")

    "publish and then receive" in {
      val res = for {
        connection <- connectionRes
        session    <- connection.createQueueSession(SessionType.AutoAcknowledge)
        queue      <- Resource.liftF(session.createQueue(queueName))
        consumer   <- session.createConsumer(queue)
        producer   <- session.createProducer(queue)
        msg        <- Resource.liftF(session.createTextMessage("body"))
      } yield (consumer, producer, msg)

      res.use {
        case (consumer, producer, msg) =>
          for {
            _        <- producer.send(msg)
            received <- consumer.receiveTextMessage
            assertion <- received match {
                          case Left(_)   => IO.delay(Failed(s"Received not a TextMessage!").toSucceeded)
                          case Right(tm) => tm.getText.map(_.shouldBe("body"))
                        }
          } yield assertion
      }
    }

    "publish 100 messages and then consume them concurrently with local transactions" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection         <- connectionRes
        session            <- connection.createQueueSession(SessionType.AutoAcknowledge)
        queue              <- Resource.liftF(session.createQueue(queueName))
        producer           <- session.createProducer(queue)
        bodies             = (0 until 100).map(i => s"$i")
        messages           <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createTransactedQueueConsumer(connection, queueName, 10)
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
                                  .flatMap(body => received.update(_ + body))
                                  .as(TransactionResult.Commit)
                            }.start
            receivedMessages <- IO
                                 .race(
                                   received.get.iterateUntil(_.eqv(bodies)),
                                   IO.sleep(2.seconds) >> received.get
                                 )
                                 .map(_.fold(identity, identity))
                                 .guarantee(consumerFiber.cancel)
          } yield receivedMessages.shouldBe(bodies)
      }
    }
  }
}
