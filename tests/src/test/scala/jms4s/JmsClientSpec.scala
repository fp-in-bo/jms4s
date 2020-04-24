package jms4s

import java.util.concurrent.TimeUnit

import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Timer }
import cats.implicits._
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.basespec.Jms4sBaseSpec
import org.scalatest.freespec.AsyncFreeSpec
import scala.concurrent.duration._

trait JmsClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  s"publish $nMessages messages and then consume them concurrently with local transactions" in {
    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createTransactedConsumer(inputQueueName, poolSize)
      producwr  <- jmsClient.createProducer(poolSize)
    } yield (consumer, producwr, bodies.toSet)

    res.use {
      case (consumer, producer, bodies) =>
        for {
          _        <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _        <- logger.info(s"Pushed ${bodies.size} messages.")
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
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createTransactedConsumer(inputQueueName, poolSize)
      producwr  <- jmsClient.createProducer(poolSize)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      consumer2 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName2, poolSize)
    } yield (consumer, producwr, consumer1, consumer2, bodies.toSet)

    res.use {
      case (consumer, producer, consumer1, consumer2, bodies) =>
        for {
          _ <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _ <- logger.info(s"Pushed ${bodies.size} messages.")
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
                               receiveUntil(consumer1, received1, nMessages / 2, timeout),
                               receiveUntil(consumer2, received2, nMessages / 2, timeout)
                             ).parTupled >> (received1.get, received2.get).mapN(_ ++ _))
                               .guarantee(consumerToProducerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages and then consume them concurrently with acknowledge" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAcknowledgerConsumer(inputQueueName, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (consumer, producer, bodies.toSet)

    res.use {
      case (consumer, producer, bodies) =>
        for {
          _        <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _        <- logger.info(s"Pushed ${bodies.size} messages.")
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
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAcknowledgerConsumer(inputQueueName, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      consumer2 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName2, poolSize)
    } yield (consumer, producer, consumer1, consumer2, bodies.toSet)

    res.use {
      case (consumer, producer, consumer1, consumer2, bodies) =>
        for {
          _ <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _ <- logger.info(s"Pushed ${bodies.size} messages.")
          consumerToProducerFiber <- consumer.handle { (message, mf) =>
                                      for {
                                        text <- message.asTextF[IO]
                                        newm <- mf.makeTextMessage(text)
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
                               receiveUntil(consumer1, received1, nMessages / 2, timeout),
                               receiveUntil(consumer2, received2, nMessages / 2, timeout)
                             ).parTupled.timeout(timeout) >> (received1.get, received2.get).mapN(_ ++ _))
                               .guarantee(consumerToProducerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"publish $nMessages messages and then consume them concurrently with auto-acknowledge" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(inputQueueName, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (consumer, producer, bodies.toSet)

    res.use {
      case (consumer, producer, bodies) =>
        for {
          _        <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _        <- logger.info(s"Pushed ${bodies.size} messages.")
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
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(inputQueueName, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      consumer2 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName2, poolSize)

    } yield (consumer, producer, consumer1, consumer2, bodies.toSet)

    res.use {
      case (consumer, producer, consumer1, consumer2, bodies) =>
        for {
          _ <- producer.sendN(messageFactory(bodies.toList, inputQueueName))
          _ <- logger.info(s"Pushed ${bodies.size} messages.")
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
                               receiveUntil(consumer1, received1, nMessages / 2, timeout),
                               receiveUntil(consumer2, received2, nMessages / 2, timeout)
                             ).parTupled >> (received1.get, received2.get).mapN(_ ++ _))
                               .guarantee(consumerToProducerFiber.cancel)
        } yield assert(receivedMessages == bodies)
    }
  }

  s"send $nMessages messages in a Queue with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet)

    res.use {
      case (producer, consumer, bodies) =>
        for {
          _                <- producer.sendN(messageFactory(bodies.toList, outputQueueName1))
          _                <- logger.info(s"Pushed ${bodies.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages, timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"sendN $nMessages messages in a Queue with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet)

    res.use {
      case (producer, consumer, bodies) =>
        for {
          _                <- producer.sendN(messageFactory(bodies.toList, outputQueueName1))
          _                <- logger.info(s"Pushed ${bodies.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages, timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"send $nMessages messages in a Topic with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(topicName1, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet)

    res.use {
      case (producer, consumer, bodies) =>
        for {
          _                <- producer.sendN(messageFactory(bodies.toList, topicName1))
          _                <- logger.info(s"Pushed ${bodies.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages, timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"sendN $nMessages messages in a Topic with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(topicName1, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer, bodies.toSet)

    res.use {
      case (producer, consumer, bodies) =>
        for {
          _                <- producer.sendN(messageFactory(bodies.toList, topicName1))
          _                <- logger.info(s"Pushed ${bodies.size} messages.")
          _                <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
          received         <- Ref.of[IO, Set[String]](Set())
          receivedMessages <- receiveUntil(consumer, received, nMessages, timeout) >> received.get
        } yield assert(receivedMessages == bodies)
    }
  }

  s"send $nMessages messages in two Queues with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      consumer2 <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName2, poolSize)
    } yield (producer, consumer1, consumer2, bodies.toSet)

    res.use {
      case (producer, consumer1, consumer2, bodies) =>
        for {
          _ <- producer.sendN(messageFactory(bodies.toList, outputQueueName1)) *> producer.sendN(
                messageFactory(bodies.toList, outputQueueName2)
              )
          _                   <- logger.info(s"Pushed ${bodies.size} messages.")
          _                   <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          firstBatch          <- Ref.of[IO, Set[String]](Set())
          firstBatchMessages  <- receiveUntil(consumer1, firstBatch, nMessages, timeout) >> firstBatch.get
          secondBatch         <- Ref.of[IO, Set[String]](Set())
          secondBatchMessages <- receiveUntil(consumer2, secondBatch, nMessages, timeout) >> secondBatch.get
        } yield assert(firstBatchMessages == bodies && secondBatchMessages == bodies)
    }
  }

  s"send $nMessages messages in two Topics with pooled producer and consume them" in {

    val res = for {
      jmsClient <- jmsClientRes
      producer  <- jmsClient.createProducer(poolSize)
      consumer1 <- jmsClient.createAutoAcknowledgerConsumer(topicName1, poolSize)
      consumer2 <- jmsClient.createAutoAcknowledgerConsumer(topicName2, poolSize)
    } yield (producer, consumer1, consumer2, bodies.toSet)

    res.use {
      case (producer, consumer1, consumer2, bodies) =>
        for {
          _ <- producer.sendN(messageFactory(bodies.toList, topicName1)) *> producer.sendN(
                messageFactory(bodies.toList, topicName2)
              )
          _                   <- logger.info(s"Pushed ${bodies.size} messages.")
          _                   <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          firstBatch          <- Ref.of[IO, Set[String]](Set())
          firstBatchMessages  <- receiveUntil(consumer1, firstBatch, nMessages, timeout) >> firstBatch.get
          secondBatch         <- Ref.of[IO, Set[String]](Set())
          secondBatchMessages <- receiveUntil(consumer2, secondBatch, nMessages, timeout) >> secondBatch.get
        } yield assert(firstBatchMessages == bodies && secondBatchMessages == bodies)
    }
  }

  s"sendN $nMessages messages with delay in a Queue with pooled producer and consume them" in {
    val res = for {
      jmsClient <- jmsClientRes
      consumer  <- jmsClient.createAutoAcknowledgerConsumer(outputQueueName1, poolSize)
      producer  <- jmsClient.createProducer(poolSize)
    } yield (producer, consumer)

    res.use {
      case (producer, consumer) =>
        for {
          producerTimestamp <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          _                 <- producer.sendWithDelay(messageWithDelayFactory((body, (outputQueueName1, Some(delay)))))
          _                 <- logger.info(s"Pushed message with body: $body.")
          _                 <- logger.info(s"Consumer to Producer started. Collecting messages from output queue...")
          text              <- receive(consumer, timeout)
          deliveryTime      <- Timer[IO].clock.realTime(TimeUnit.MILLISECONDS)
          actualDelay       = (deliveryTime - producerTimestamp).millis
        } yield assert(actualDelay >= delayWithTolerance && text == body)
    }
  }
}
