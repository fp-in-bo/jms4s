package fs2jms

import cats.data.NonEmptyList
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource }
import fs2jms.config._
import fs2jms.ibmmq.ibmMQ._
import fs2jms.model.SessionType
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.Failed
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  val connectionRes: Resource[IO, JmsQueueConnection[IO]] = makeConnection(
    Config(
      qm = QueueManager("QM1"),
      endpoints = NonEmptyList.one(Endpoint("localhost", 1414)),
      channel = Channel("DEV.APP.SVRCONN"),
      username = Some(Username("app")),
      password = None
    )
  )

  "Basic jms ops" - {
    "publish and then receive" in {
      val res = for {
        connection <- connectionRes
        session    <- connection.createQueueSession(SessionType.AutoAcknowledge)
        queue      <- Resource.liftF(session.createQueue(QueueName("DEV.QUEUE.1")))
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
  }
}
