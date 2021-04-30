package jms4s.jms

import java.util.concurrent.TimeUnit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource, Timer }
import cats.implicits._
import jms4s.basespec.Jms4sBaseSpec
import jms4s.config.DestinationName
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration._

trait JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  private def contexts(destination: DestinationName) =
    for {
      client          <- jmsClientRes
      context         = client.context
      receiveConsumer <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(destination))
      sendContext     <- context.createContext(SessionType.AutoAcknowledge)
      msg             <- Resource.eval(context.createTextMessage(body))
    } yield (receiveConsumer, sendContext, msg)

  "publish to a queue and then receive" in {
    contexts(inputQueueName).use {
      case (receiveConsumer, sendContext, msg) =>
        for {
          _    <- sendContext.send(inputQueueName, msg)
          text <- receiveBodyAsTextOrFail(receiveConsumer)
        } yield assert(text == body)
    }
  }
  "publish and then receive with a delay" in {
    contexts(inputQueueName).use {
      case (consumer, sendContext, msg) =>
        for {
          producerTimestamp <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          _                 <- sendContext.send(inputQueueName, msg, delay)
          msg               <- consumer.receiveJmsMessage
          deliveryTime      <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          actualBody        <- msg.asTextF[IO]
          actualDelay       = (deliveryTime - producerTimestamp).millis
        } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
    }
  }
  "publish to a topic and then receive" in {
    contexts(topicName1).use {
      case (consumer, sendContext, msg) =>
        for {
          _   <- (IO.delay(10.millis) >> sendContext.send(topicName1, msg)).start
          rec <- receiveBodyAsTextOrFail(consumer)
        } yield assert(rec == body)
    }
  }
}
