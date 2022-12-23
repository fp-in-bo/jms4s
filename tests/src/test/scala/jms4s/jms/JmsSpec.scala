/*
 * Copyright (c) 2020 Functional Programming in Bologna
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package jms4s.jms

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ Clock, IO, Resource }
import jms4s.basespec.Jms4sBaseSpec
import jms4s.config.{ DestinationName, TemporaryDestination }
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration.DurationInt

trait JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  private def contexts(destination: DestinationName) =
    for {
      client  <- jmsClientRes
      context = client.context
      receiveConsumer <- context
                          .createContext(SessionType.AutoAcknowledge)
                          .flatMap(_.createJmsConsumer(destination, 50.millis))
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      msg         <- Resource.eval(context.createTextMessage(body)) // TODO create from sendContext?
    } yield (receiveConsumer, sendContext, msg)

  private def contexts(destination: TemporaryDestination) =
    for {
      client               <- jmsClientRes
      temporaryDestination <- Resource.eval(client.createDestination(destination))
      context              = client.context
      receiveConsumer <- context
                          .createContext(SessionType.AutoAcknowledge)
                          .flatMap(_.createJmsConsumer(temporaryDestination, 50.millis))
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      msg         <- Resource.eval(context.createTextMessage(body)) // TODO create from sendContext?
    } yield (receiveConsumer, temporaryDestination, sendContext, msg)

  "publish to a queue and then receive" in {
    contexts(inputQueueName).use {
      case (receiveConsumer, sendContext, msg) =>
        for {
          _    <- sendContext.send(inputQueueName, msg)
          text <- receiveBodyAsTextOrFail(receiveConsumer)
        } yield assert(text == body)
    }
  }
  "publish to a queue and then receive with a delay" in {
    contexts(inputQueueName).use {
      case (consumer, sendContext, msg) =>
        for {
          producerTimestamp <- Clock[IO].realTime
          _                 <- sendContext.send(inputQueueName, msg, delay)
          msg               <- consumer.receiveJmsMessage
          deliveryTime      <- Clock[IO].realTime
          actualBody        <- msg.asTextF[IO]
          actualDelay       = (deliveryTime - producerTimestamp)
        } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
    }
  }
  "publish to a topic and then receive" in {
    contexts(topicName1).use {
      case (consumer, sendContext, msg) =>
        for {
          _   <- sendContext.send(topicName1, msg)
          rec <- receiveBodyAsTextOrFail(consumer)
        } yield assert(rec == body)
    }
  }

  "publish to a temporary queue and then receive" in {
    newTemporaryInputQueue.flatMap { temporaryInputQueue =>
      contexts(temporaryInputQueue).use {
        case (receiveConsumer, jmsDestination, sendContext, msg) =>
          for {
            _    <- sendContext.send(jmsDestination, msg)
            text <- receiveBodyAsTextOrFail(receiveConsumer)
          } yield assert(text == body)
      }
    }
  }
  "publish to a temporary queue and then receive with a delay" in {
    newTemporaryInputQueue.flatMap { temporaryInputQueue =>
      contexts(temporaryInputQueue).use {
        case (consumer, jmsDestination, sendContext, msg) =>
          for {
            producerTimestamp <- Clock[IO].realTime
            _                 <- sendContext.send(jmsDestination, msg, delay)
            msg               <- consumer.receiveJmsMessage
            deliveryTime      <- Clock[IO].realTime
            actualBody        <- msg.asTextF[IO]
            actualDelay       = (deliveryTime - producerTimestamp)
          } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
      }
    }
  }
  "publish to a tempoary topic and then receive" in {
    newTemporaryTopic.flatMap { temporaryTopic =>
      contexts(temporaryTopic).use {
        case (consumer, jmsDestination, sendContext, msg) =>
          for {
            _   <- sendContext.send(jmsDestination, msg)
            rec <- receiveBodyAsTextOrFail(consumer)
          } yield assert(rec == body)
      }
    }
  }

  "update and get a JMSMessage property" in {
    contexts(topicName1).use {
      case (_, _, msg) =>
        for {
          _ <- IO.fromTry(msg.setJMSType("newType"))
          t = msg.getJMSType
        } yield assert(t.contains("newType"))
    }
  }
}
