package jms4s.jms

import java.util.concurrent.TimeUnit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource, Timer }
import cats.implicits._
import jms4s.basespec.Jms4sBaseSpec
import jms4s.model.SessionType.AutoAcknowledge
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration._

trait JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  val queueRes = for {
    connection    <- connectionRes
    session       <- connection.createSession(AutoAcknowledge)
    queue         <- Resource.liftF(session.createQueue(inputQueueName))
    queueConsumer <- connection.createSession(AutoAcknowledge).flatMap(_.createConsumer(queue))
    queueProducer <- connection.createSession(AutoAcknowledge).flatMap(_.createProducer(queue))
    msg           <- Resource.liftF(session.createTextMessage(body))
  } yield (queueConsumer, queueProducer, msg)

  val topicRes = for {
    connection    <- connectionRes
    session       <- connection.createSession(AutoAcknowledge)
    topic         <- Resource.liftF(session.createTopic(topicName))
    topicConsumer <- connection.createSession(AutoAcknowledge).flatMap(_.createConsumer(topic))
    topicProducer <- connection.createSession(AutoAcknowledge).flatMap(_.createProducer(topic))
    msg           <- Resource.liftF(session.createTextMessage(body))
  } yield (topicConsumer, topicProducer, msg)

  "publish to a queue and then receive" in {
    queueRes.use {
      case (queueConsumer, queueProducer, msg) =>
        for {
          _    <- queueProducer.send(msg)
          text <- receiveBodyAsTextOrFail(queueConsumer)
        } yield assert(text == body)
    }
  }
  "publish and then receive with a delay" in {
    queueRes.use {
      case (consumer, producer, msg) =>
        for {
          producerTimestamp <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          _                 <- producer.setDeliveryDelay(delay)
          _                 <- producer.send(msg)
          msg               <- consumer.receiveJmsMessage
          deliveryTime      <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          tm                <- msg.asJmsTextMessage
          actualBody        <- tm.getText
          actualDelay       = (deliveryTime - producerTimestamp).millis
        } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
    }
  }
  "publish to a topic and then receive" in {
    topicRes.use {
      case (topicConsumer, topicProducer, msg) =>
        for {
          _   <- (IO.delay(10.millis) >> topicProducer.send(msg)).start
          rec <- receiveBodyAsTextOrFail(topicConsumer)
        } yield assert(rec == body)
    }
  }
}
