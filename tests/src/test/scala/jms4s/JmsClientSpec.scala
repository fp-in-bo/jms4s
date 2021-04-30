package jms4s

import java.util.concurrent.TimeUnit

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource }
import cats.implicits._
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.basespec.Jms4sBaseSpec
import jms4s.jms.JmsMessage
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration._
import cats.effect.{ Ref, Temporal }

trait JmsClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  s"publish $nMessages messages and then consume them concurrently with local transactions" in {
    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createTransactedConsumer(inputQueueName, poolSize)
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
                               .guarantee(consumerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to other queues" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createTransactedConsumer(inputQueueName, poolSize)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
      consumer1   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      consumer2   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName2))
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

  s"publish $nMessages messages and then consume them concurrently with acknowledge" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createAcknowledgerConsumer(inputQueueName, poolSize)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
    } yield (consumer, sendContext, bodies.toSet, messages)

    res.use {
      case (consumer, sendContext, bodies, messages) =>
        for {
          _        <- messages.traverse_(msg => sendContext.send(inputQueueName, msg))
          _        <- logger.info(s"Pushed ${messages.size} messages.")
          received <- Ref.of[IO, Set[String]](Set())
          consumerFiber <- consumer.handle { (message, _) =>
                            for {
                              body <- message.asTextF[IO]
                              _    <- received.update(_ + body)
                            } yield AckAction.ack
                          }.start
          _ <- logger.info(s"Consumer started. Collecting messages from the queue...")
          receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                               .guarantee(consumerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages, consume them concurrently and then republishing to other queues, with acknowledge" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createAcknowledgerConsumer(inputQueueName, poolSize)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
      consumer1   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      consumer2   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName2))
    } yield (consumer, sendContext, consumer1, consumer2, bodies.toSet, messages)

    res.use {
      case (consumer, sendContext, consumer1, consumer2, bodies, messages) =>
        for {
          _ <- messages.traverse_(msg => sendContext.send(inputQueueName, msg))
          _ <- logger.info(s"Pushed ${messages.size} messages.")
          consumerToProducerFiber <- consumer.handle { (message, mf) =>
                                      for {
                                        text                            <- message.asTextF[IO]
                                        newm: JmsMessage.JmsTextMessage <- mf.makeTextMessage(text)
                                      } yield
                                        if (text.toInt % 2 == 0)
                                          AckAction.send[IO](newm, outputQueueName1)
                                        else
                                          AckAction.send[IO](newm, outputQueueName2)
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

  s"publish $nMessages messages and then consume them concurrently with auto-acknowledge" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createAutoAcknowledgerConsumer(inputQueueName, poolSize)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
    } yield (consumer, sendContext, bodies.toSet, messages)

    res.use {
      case (consumer, sendContext, bodies, messages) =>
        for {
          _        <- messages.traverse_(msg => sendContext.send(inputQueueName, msg))
          _        <- logger.info(s"Pushed ${messages.size} messages.")
          received <- Ref.of[IO, Set[String]](Set())
          consumerFiber <- consumer.handle { (message, _) =>
                            for {
                              body <- message.asTextF[IO]
                              _    <- received.update(_ + body)
                            } yield AutoAckAction.noOp
                          }.start
          _ <- logger.info(s"Consumer started. Collecting messages from the queue...")
          receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                               .guarantee(consumerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages, consume them concurrently and then republishing to other queues, with auto-acknowledge" in {

    val res = for {
      jmsClient   <- jmsClientRes
      context     = jmsClient.context
      consumer    <- jmsClient.createAutoAcknowledgerConsumer(inputQueueName, poolSize)
      sendContext <- context.createContext(SessionType.AutoAcknowledge)
      messages    <- Resource.eval(bodies.traverse(i => sendContext.createTextMessage(i)))
      consumer1   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      consumer2   <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName2))

    } yield (consumer, sendContext, consumer1, consumer2, bodies.toSet, messages)

    res.use {
      case (consumer, sendContext, consumer1, consumer2, bodies, messages) =>
        for {
          _ <- messages.traverse_(msg => sendContext.send(inputQueueName, msg))
          _ <- logger.info(s"Pushed ${messages.size} messages.")
          consumerToProducerFiber <- consumer.handle { (message, _) =>
                                      for {
                                        tm   <- message.asJmsTextMessageF[IO]
                                        text <- tm.asTextF[IO]
                                      } yield
                                        if (text.toInt % 2 == 0)
                                          AutoAckAction.send[IO](tm, outputQueueName1)
                                        else AutoAckAction.send[IO](tm, outputQueueName2)
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

  s"send $nMessages messages in a Queue with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer  <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet, messages)

    res.use {
      case (producer, consumer, bodies, messages) =>
        for {
          _                <- messages.parTraverse_(msg => producer.send(messageFactory(msg, outputQueueName1)))
          _                <- logger.info(s"Pushed ${messages.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages).timeout(timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"sendN $nMessages messages in a Queue with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer  <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet, messages)

    res.use {
      case (producer, consumer, bodies, messages) =>
        for {
          _                <- messages.toNel.traverse_(msg => producer.sendN(messageFactory(msg, outputQueueName1)))
          _                <- logger.info(s"Pushed ${messages.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages).timeout(timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"send $nMessages messages in a Topic with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer  <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(topicName1))
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
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
      consumer  <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(topicName1))
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
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

  s"send $nMessages messages in two Queues with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      consumer2 <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName2))
    } yield (producer, consumer1, consumer2, bodies.toSet, messages)

    res.use {
      case (producer, consumer1, consumer2, bodies, messages) =>
        for {
          _ <- messages.parTraverse_(msg =>
                producer.send(messageFactory(msg, outputQueueName1)) *> producer.send(
                  messageFactory(msg, outputQueueName2)
                )
              )
          _                  <- logger.info(s"Pushed ${messages.size} messages.")
          _                  <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          firstBatch         <- Ref.of[IO, Set[String]](Set())
          firstBatchMessages <- receiveUntil(consumer1, firstBatch, nMessages).timeout(timeout) >> firstBatch.get
          secondBatch        <- Ref.of[IO, Set[String]](Set())
          secondBatchMessages <- receiveUntil(consumer2, secondBatch, nMessages).timeout(
                                  timeout
                                ) >> secondBatch.get
        } yield assert(firstBatchMessages == bodies && secondBatchMessages == bodies)
    }
  }

  s"send $nMessages messages in two Topics with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      messages  <- Resource.eval(bodies.traverse(i => context.createTextMessage(i)))
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(topicName1))
      consumer2 <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(topicName2))
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

  s"sendN $nMessages messages with delay in a Queue with pooled producer and consume them" in {
    val res = for {
      jmsClient <- jmsClientRes
      context   = jmsClient.context
      consumer  <- context.createContext(SessionType.AutoAcknowledge).flatMap(_.createJmsConsumer(outputQueueName1))
      message   <- Resource.eval(context.createTextMessage(body))
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, message)

    res.use {
      case (producer, consumer, message) =>
        for {
          producerTimestamp <- Temporal[IO].clock.realTime(TimeUnit.MILLISECONDS)
          _                 <- producer.sendWithDelay(messageWithDelayFactory((message, (outputQueueName1, Some(delay)))))
          _                 <- logger.info(s"Pushed message with body: $body.")
          _                 <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          receivedMessage   <- receiveMessage(consumer).timeout(timeout)
          deliveryTime      <- Temporal[IO].clock.realTime(TimeUnit.MILLISECONDS)
          actualBody        <- receivedMessage.asTextF[IO]
          actualDelay       = (deliveryTime - producerTimestamp).millis
        } yield assert(actualDelay >= delayWithTolerance && actualBody == body)
    }
  }
}
