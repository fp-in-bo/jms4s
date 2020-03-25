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
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ Assertion, Failed }

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

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

    "publish and then consume with local transaction" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection         <- connectionRes
        session            <- connection.createQueueSession(SessionType.AutoAcknowledge)
        queue              <- Resource.liftF(session.createQueue(queueName))
        producer           <- session.createProducer(queue)
        msg                <- Resource.liftF(session.createTextMessage("body"))
        transactedConsumer <- jmsClient.createTransactedQueueConsumer(connection, queueName, 1)
      } yield (transactedConsumer, producer, msg)

      res.use {
        case (transactedConsumer, producer, msg) =>
          for {
            _            <- producer.send(msg)
            assertionRef <- Ref.of[IO, Option[Assertion]](None)
            _ <- transactedConsumer.handle {
                  case ReceivedUnsupportedMessage(_, _) =>
                    assertionRef
                      .set(Some(Failed(s"Received not a TextMessage!").toSucceeded))
                      .as(TransactionResult.Commit)
                  case ReceivedTextMessage(tm, _) =>
                    tm.getText
                      .flatMap(
                        body =>
                          assertionRef
                            .set(Some(body.shouldBe("body")))
                      )
                      .as(TransactionResult.Commit)
                }
            assertion <- assertionRef.get
          } yield assertion.get
      }
    }
  }
}
