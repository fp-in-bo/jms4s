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

package jms4s

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect._
import cats.syntax.all._
import jms4s.basespec.Jms4sBaseSpec
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

trait JmsTopicClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  s"send $nMessages messages in a Topic with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer <- context
                   .createContext(SessionType.AutoAcknowledge)
                   .flatMap(_.createJmsConsumer(topicName1, pollingInterval))
      messages <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet, messages)

    res.use {
      case (producer, consumer, bodies, messages) =>
        for {
          _                <- messages.parTraverse_(msg => producer.send(messageFactory(msg, topicName1)))
          _                <- logger.info(s"Pushed ${messages.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages).timeout(timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"sendN $nMessages messages in a Topic with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer <- context
                   .createContext(SessionType.AutoAcknowledge)
                   .flatMap(_.createJmsConsumer(topicName1, pollingInterval))
      messages <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet, messages)

    res.use {
      case (producer, consumer, bodies, messages) =>
        for {
          _                <- messages.toNel.fold(IO.unit)(ms => producer.sendN(messageFactory(ms, topicName1)))
          _                <- logger.info(s"Pushed ${messages.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages).timeout(timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"send $nMessages messages in two Topics with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- context
                    .createContext(SessionType.AutoAcknowledge)
                    .flatMap(_.createJmsConsumer(topicName1, pollingInterval))
      consumer2 <- context
                    .createContext(SessionType.AutoAcknowledge)
                    .flatMap(_.createJmsConsumer(topicName2, pollingInterval))
    } yield (producer, consumer1, consumer2, bodies.toSet, messages)

    res.use {
      case (producer, consumer1, consumer2, bodies, messages) =>
        for {
          _ <- messages.parTraverse_(msg =>
                producer.send(messageFactory(msg, topicName1)) *> producer.send(messageFactory(msg, topicName2))
              )
          _                   <- logger.info(s"Pushed ${messages.size} messages.")
          _                   <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          firstBatch          <- Ref.of[IO, Set[String]](Set())
          firstBatchMessages  <- receiveUntil(consumer1, firstBatch, nMessages).timeout(timeout) >> firstBatch.get
          secondBatch         <- Ref.of[IO, Set[String]](Set())
          secondBatchMessages <- receiveUntil(consumer2, secondBatch, nMessages).timeout(timeout) >> secondBatch.get
        } yield assert(firstBatchMessages == bodies && secondBatchMessages == bodies)
    }
  }

}
