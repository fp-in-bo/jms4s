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

  private val contexts = for {
    context        <- contextRes
    receiveContext <- context.createContext(SessionType.AutoAcknowledge)
    sendContext    <- context.createContext(SessionType.AutoAcknowledge)
    msg            <- Resource.liftF(context.createTextMessage(body))
  } yield (receiveContext, sendContext, msg)

  "publish to a queue and then receive" in {
    contexts.use {
      case (receiveContext, sendContext, msg) =>
        for {
          _    <- sendContext.send(inputQueueName, msg)
          text <- receiveBodyAsTextOrFail(inputQueueName, receiveContext)
        } yield assert(text == body)
    }
  }
  "publish and then receive with a delay" in {
    contexts.use {
      case (receiveContext, sendContext, msg) =>
        for {
          producerTimestamp <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          _                 <- sendContext.send(inputQueueName, msg, delay)
          msg               <- receiveContext.receive(inputQueueName)
          deliveryTime      <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          tm                <- msg.asJmsTextMessage
          actualBody        <- tm.getText
          actualDelay       = (deliveryTime - producerTimestamp).millis
        } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
    }
  }
  "publish to a topic and then receive" in {
    contexts.use {
      case (receiveContext, sendContext, msg) =>
        for {
          _   <- (IO.delay(10.millis) >> sendContext.send(topicName1, msg)).start
          rec <- receiveBodyAsTextOrFail(topicName1, receiveContext)
        } yield assert(rec == body)
    }
  }
}
