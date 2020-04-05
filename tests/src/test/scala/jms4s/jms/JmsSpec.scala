package jms4s.jms

import java.util.concurrent.TimeUnit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource, Timer }
import cats.implicits._
import jms4s.basespec.Jms4sBaseSpec
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration._

trait JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  val expectedBody = "body"
  "Basic jms ops" - {
    val queueRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      queue         <- Resource.liftF(session.createQueue(inputQueueName))
      queueConsumer <- session.createConsumer(queue)
      queueProducer <- session.createProducer(queue)
      msg           <- Resource.liftF(session.createTextMessage(expectedBody))
    } yield (queueConsumer, queueProducer, msg)

    val topicRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      topic         <- Resource.liftF(session.createTopic(topicName))
      topicConsumer <- session.createConsumer(topic)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage(expectedBody))
    } yield (topicConsumer, topicProducer, msg)

    "publish to a queue and then receive" in {
      queueRes.use {
        case (queueConsumer, queueProducer, msg) =>
          for {
            _    <- queueProducer.send(msg)
            text <- receiveBodyAsTextOrFail(queueConsumer)
          } yield assert(text == expectedBody)
      }
    }
    "publish and then receive with a delay" in {
      queueRes.use {
        case (consumer, producer, msg) =>
          for {
            _                 <- producer.setDeliveryDelay(delay)
            producerTimestamp <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
            _                 <- producer.send(msg)
            msg               <- consumer.receiveJmsMessage
            tm                <- msg.asJmsTextMessage
            body              <- tm.getText
            jmsDeliveryTime   <- tm.getJMSDeliveryTime
            producerDelay     = jmsDeliveryTime - producerTimestamp
          } yield assert(producerDelay >= delay.toMillis && body == expectedBody)
      }
    }
    "publish to a topic and then receive" in {
      topicRes.use {
        case (topicConsumer, topicProducer, msg) =>
          for {
            _   <- (IO.delay(10.millis) >> topicProducer.send(msg)).start
            rec <- receiveBodyAsTextOrFail(topicConsumer)
          } yield assert(rec == expectedBody)
      }
    }
  }
}
