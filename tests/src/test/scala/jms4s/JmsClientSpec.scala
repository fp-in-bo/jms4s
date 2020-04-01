package jms4s

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ IO, Resource }
import cats.implicits._
import jms4s.JmsTransactedConsumer.TransactionResult._
import jms4s.JmsAcknowledgerConsumer.AckResult._
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

class JmsClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {
  "High level api" - {
    s"publish $nMessages messages and then consume them concurrently with local transactions" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection         <- connectionRes
        session            <- connection.createSession(SessionType.AutoAcknowledge)
        queue              <- Resource.liftF(session.createQueue(inputQueueName))
        producer           <- session.createProducer(queue)
        messages           <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createTransactedConsumer(connection, inputQueueName, poolSize)
      } yield (transactedConsumer, producer, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- transactedConsumer.handle { message =>
                              for {
                                tm   <- message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield commit
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to an other queue" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createTransactedConsumerToProducer(
                               connection,
                               inputQueueName,
                               outputQueueName1,
                               poolSize
                             )
      } yield (transactedConsumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- transactedConsumer.handle { _ =>
                                        IO(sendTo(outputQueueName1))
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to other queues" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection      <- connectionRes
        session         <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue      <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue1    <- Resource.liftF(session.createQueue(outputQueueName1))
        outputQueue2    <- Resource.liftF(session.createQueue(outputQueueName2))
        inputProducer   <- session.createProducer(inputQueue)
        outputConsumer1 <- session.createConsumer(outputQueue1)
        outputConsumer2 <- session.createConsumer(outputQueue2)
        bodies          = (0 until nMessages).map(i => s"$i")
        messages        <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        transactedConsumer <- jmsClient.createTransactedConsumerToProducers(
                               connection,
                               inputQueueName,
                               NonEmptyList.of(outputQueueName1, outputQueueName2),
                               poolSize
                             )
      } yield (transactedConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (transactedConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- transactedConsumer.handle { message =>
                                        for {
                                          tm   <- message.asJmsTextMessage
                                          text <- tm.getText
                                        } yield
                                          if (text.toInt % 2 == 0) sendTo(outputQueueName1)
                                          else sendTo(outputQueueName2)
                                      }.start
            _         <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queues...")
            received1 <- Ref.of[IO, Set[String]](Set())
            received2 <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- ((
                                 receiveUntil(outputConsumer1, received1, nMessages / 2),
                                 receiveUntil(outputConsumer2, received2, nMessages / 2)
                               ).parTupled.timeout(timeout) >> (received1.get, received2.get).mapN(_ ++ _))
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages and then consume them concurrently with acknowledge" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection           <- connectionRes
        session              <- connection.createSession(SessionType.AutoAcknowledge)
        queue                <- Resource.liftF(session.createQueue(inputQueueName))
        producer             <- session.createProducer(queue)
        messages             <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        acknowledgerConsumer <- jmsClient.createAcknowledgerConsumer(connection, inputQueueName, poolSize)
      } yield (acknowledgerConsumer, producer, bodies.toSet, messages)

      res.use {
        case (acknowledgerConsumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- acknowledgerConsumer.handle { message =>
                              for {
                                tm   <- message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield ack
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to an other queue, with acknowledge" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        acknowledgerConsumer <- jmsClient.createAcknowledgerToProducer(
                                 connection,
                                 inputQueueName,
                                 outputQueueName1,
                                 poolSize
                               )
      } yield (acknowledgerConsumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (acknowledgerConsumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- acknowledgerConsumer.handle { _ =>
                                        IO(sendToAndAck(outputQueueName1))
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to other queues, with acknowledge" in {
      val jmsClient = new JmsClient[IO]

      val res = for {
        connection      <- connectionRes
        session         <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue      <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue1    <- Resource.liftF(session.createQueue(outputQueueName1))
        outputQueue2    <- Resource.liftF(session.createQueue(outputQueueName2))
        inputProducer   <- session.createProducer(inputQueue)
        outputConsumer1 <- session.createConsumer(outputQueue1)
        outputConsumer2 <- session.createConsumer(outputQueue2)
        bodies          = (0 until nMessages).map(i => s"$i")
        messages        <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        acknowledgerConsumer <- jmsClient.createAcknowledgerToProducers(
                                 connection,
                                 inputQueueName,
                                 NonEmptyList.of(outputQueueName1, outputQueueName2),
                                 poolSize
                               )
      } yield (acknowledgerConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (acknowledgerConsumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- acknowledgerConsumer.handle { message =>
                                        for {
                                          tm   <- message.asJmsTextMessage
                                          text <- tm.getText
                                        } yield
                                          if (text.toInt % 2 == 0) sendToAndAck(outputQueueName1)
                                          else sendToAndAck(outputQueueName2)
                                      }.start
            _         <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queues...")
            received1 <- Ref.of[IO, Set[String]](Set())
            received2 <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- ((
                                 receiveUntil(outputConsumer1, received1, nMessages / 2),
                                 receiveUntil(outputConsumer2, received2, nMessages / 2)
                               ).parTupled.timeout(timeout) >> (received1.get, received2.get).mapN(_ ++ _))
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

  }
}
