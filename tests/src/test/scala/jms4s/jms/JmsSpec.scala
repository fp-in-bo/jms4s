package jms4s.jms

import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource }
import cats.implicits._
import jms4s.Jms4sBaseSpec
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers with Jms4sBaseSpec {
  "Basic jms ops" - {
    val queueRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      queue         <- Resource.liftF(session.createQueue(inputQueueName))
      queueConsumer <- session.createConsumer(queue)
      queueProducer <- session.createProducer(queue)
      msg           <- Resource.liftF(session.createTextMessage("body"))
    } yield (queueConsumer, queueProducer, msg)

    val topicRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      topic         <- Resource.liftF(session.createTopic(topicName))
      topicConsumer <- session.createConsumer(topic)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage("body"))
    } yield (topicConsumer, topicProducer, msg)

    "publish to a queue and then receive" in {
      queueRes.use {
        case (queueConsumer, queueProducer, msg) =>
          for {
            _    <- queueProducer.send(msg)
            text <- receiveBodyAsTextOrFail(queueConsumer)
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

      queueRes.use {
        case (queueConsumer, queueProducer, msg) =>
          for {
            _        <- queueProducer.setDeliveryDelay(delay)
            _        <- queueProducer.send(msg)
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- (for {
                              body <- receiveBodyAsTextOrFail(queueConsumer)
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
            _   <- (IO.delay(10.millis) >> topicProducer.send(msg)).start
            rec <- receiveBodyAsTextOrFail(topicConsumer)
          } yield rec.shouldBe("body")
      }
    }
  }
}
