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
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.basespec.Jms4sBaseSpec
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

trait JmsTransactedQueueClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  s"publish $nMessages messages and then consume them concurrently with local transactions" in {
    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createTransactedConsumer(inputQueueName, poolSize, pollingInterval)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
    } yield (consumer, sendContext, bodies.toSet, messages)

    res.use {
      case (consumer, context, bodies, messages) =>
        for {
          _        <- messages.traverse_(msg => context.send(inputQueueName, msg))
          _        <- logger.info(s"Pushed ${messages.size} messages.")
          received <- Ref.of[IO, Set[String]](Set())
          consumerFiber <- consumer.handle { (message, _) =>
                            for {
                              body <- message.asTextF[IO]
                              _    <- received.update(_ + body)
                            } yield TransactionAction.commit
                          }.start
          _ <- logger.info(s"Consumer started. Collecting messages from the queue...")
          receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)) >> received.get)
                               .timeout(timeout)
                               .guarantee(consumerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to other queues" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createTransactedConsumer(inputQueueName, poolSize, pollingInterval)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
      consumer1 <- context
                    .createContext(SessionType.AutoAcknowledge)
                    .flatMap(_.createJmsConsumer(outputQueueName1, pollingInterval))
      consumer2 <- context
                    .createContext(SessionType.AutoAcknowledge)
                    .flatMap(_.createJmsConsumer(outputQueueName2, pollingInterval))
    } yield (consumer, sendContext, consumer1, consumer2, bodies.toSet, messages)

    res.use {
      case (consumer, sendContext, consumer1, consumer2, bodies, messages) =>
        for {
          _ <- messages.traverse_(msg => sendContext.send(inputQueueName, msg))
          _ <- logger.info(s"Pushed ${messages.size} messages.")
          consumerToProducerFiber <- consumer.handle { (message, mf) =>
                                      for {
                                        text <- message.asTextF[IO]
                                        newm <- mf.makeTextMessage(text)
                                      } yield
                                        if (text.toInt % 2 == 0)
                                          TransactionAction.send[IO](newm, outputQueueName1)
                                        else
                                          TransactionAction.send[IO](newm, outputQueueName2)
                                    }.start
          _         <- logger.info(s"Consumer to Producer started. Collecting messages from output queues...")
          received1 <- Ref.of[IO, Set[String]](Set())
          received2 <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- ((
                               receiveUntil(consumer1, received1, nMessages / 2),
                               receiveUntil(consumer2, received2, nMessages / 2)
                             ).parTupled.timeout(timeout) >> (received1.get, received2.get).mapN(_ ++ _))
                               .guarantee(consumerToProducerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish a message, consume it with a local transaction, update it(cloning) and then republishing to an other queue" in {

    val res = for {
      jmsClient <- jmsClientRes
      producer  <- jmsClient.createProducer(1)
      consumer  <- jmsClient.createTransactedConsumer(inputQueueName, poolSize, pollingInterval)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize, pollingInterval)
    } yield (consumer, producer, consumer1)

    res.use {
      case (consumer, producer, downstreamConsumer) =>
        for {
          _ <- producer.send { mf =>
                mf.makeTextMessage("msg")
                  .flatTap(_.setStringProperty("custom_prop1", "custom_value").liftTo[IO])
                  .map((_, inputQueueName))
              }
          _    <- logger.info(s"Pushed ${bodies.size} messages.")
          sent <- Deferred[IO, JmsTextMessage]
          consumerToProducerFiber <- consumer.handle { (message, mf) =>
                                      for {
                                        textMessage <- message.asJmsTextMessageF[IO]
                                        _           <- sent.complete(textMessage)
                                        newm        <- mf.attemptCloneMessage(textMessage).flatMap(_.liftTo[IO])
                                        _           <- newm.setStringProperty("custom_prop2", "value2").liftTo[IO]
                                      } yield TransactionAction.send[IO](newm, outputQueueName1)
                                    }.start
          _ <- logger.info(s"Consumer to Producer started. Collecting messages from output queues...")
          receivedMessages <- receiveUntil(downstreamConsumer, nMessages = 1)
                               .timeout(timeout)
                               .guarantee(consumerToProducerFiber.cancel)
          x <- sent.get
        } yield (x, receivedMessages.head)
    }.asserting {
      case (original, received) =>
        assert(received.getText.contains_("msg"))
        assert(original.getStringProperty("custom_prop1") == received.getStringProperty("custom_prop1"))
        assert(received.getStringProperty("custom_prop2").contains("value2"))
    }

  }

  //  s"publish a message, consume it with a local transaction, clone it, and then republish delayed to the same queue (retry) " in {
  //    def sendToDownstream(mf: MessageFactory[IO], message: JmsTextMessage, q: QueueName): IO[TransactionAction[IO]] =
  //      for {
  //        text   <- message.asTextF[IO]
  //        newMsg <- mf.makeTextMessage(text.toUpperCase)
  //      } yield TransactionAction.send[IO](newMsg, q)
  //
  //    def cloneAndRetry(mf: MessageFactory[IO], message: JmsTextMessage, q: QueueName): IO[TransactionAction[IO]] =
  //      for {
  //        newMsg <- mf.cloneMessageF(message)
  //        _      <- newMsg.setBooleanProperty("visited", true).liftTo[IO]
  //      } yield TransactionAction.sendWithDelay[IO](newMsg, q, Some(100.millis))
  //
  //    val res = for {
  //      jmsClient <- jmsClientRes
  //      producer  <- jmsClient.createProducer(1)
  //      consumer  <- jmsClient.createTransactedConsumer(inputQueueName, poolSize, pollingInterval)
  //      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize, pollingInterval)
  //    } yield (consumer, producer, consumer1)
  //
  //    res.use {
  //      case (consumer, producer, downstreamConsumer) =>
  //        for {
  //          _ <- producer.send { mf =>
  //                mf.makeTextMessage("msg")
  //                  .flatTap(_.setStringProperty("custom_prop1", "custom_value").liftTo[IO])
  //                  .map((_, inputQueueName))
  //              }
  //          _ <- logger.info(s"Pushed ${bodies.size} messages.")
  //          consumerToProducerFiber <- consumer.handle { (message, mf) =>
  //                                      for {
  //                                        textMessage <- message.asJmsTextMessageF[IO]
  //                                        action <- textMessage.getBooleanProperty("visited") match {
  //                                                   case Some(true) =>
  //                                                     sendToDownstream(mf, textMessage, outputQueueName1)
  //                                                   case _ =>
  //                                                     cloneAndRetry(mf, textMessage, inputQueueName)
  //                                                 }
  //                                      } yield action
  //                                    }.start
  //          _ <- logger.info(s"Consumer to Producer started. Collecting messages from output queues...")
  //          receivedMessages <- receiveUntil(downstreamConsumer, nMessages = 1)
  //                               .timeout(timeout)
  //                               .guarantee(consumerToProducerFiber.cancel)
  //        } yield receivedMessages.head
  //    }.asserting(received => assert(received.getText.contains_("MSG")))
  //
  //  }

}
