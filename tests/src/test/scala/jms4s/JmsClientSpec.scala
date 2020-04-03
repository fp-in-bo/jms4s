package jms4s

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Resource}
import cats.implicits._
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsAutoAcknowledgerConsumer.Action
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.jms.JmsMessage
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

class JmsClientSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {
  private val jmsClient = new JmsClient[IO]

  "High level api" - {
    s"publish $nMessages messages and then consume them concurrently with local transactions" in {
      val res = for {
        connection <- connectionRes
        session    <- connection.createSession(SessionType.AutoAcknowledge)
        queue      <- Resource.liftF(session.createQueue(inputQueueName))
        producer   <- session.createProducer(queue)
        messages   <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer   <- jmsClient.createTransactedConsumer(connection, inputQueueName, poolSize)
      } yield (consumer, producer, bodies.toSet, messages)

      res.use {
        case (consumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- consumer.handle { message =>
                              for {
                                tm   <- message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield TransactionAction.commit
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to an other queue" in {
      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer <- jmsClient.createTransactedConsumerToProducer(
                     connection,
                     inputQueueName,
                     outputQueueName1,
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { received: JmsMessage[IO] =>
                                        received.asJmsTextMessage.map(
                                          message =>
                                            TransactionAction.send[IO](messageFactory(message, outputQueueName1))
                                        )
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently with local transactions and then republishing to other queues" in {

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
        consumer <- jmsClient.createTransactedConsumerToProducers(
                     connection,
                     inputQueueName,
                     NonEmptyList.of(outputQueueName1, outputQueueName2),
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { message =>
                                        for {
                                          tm   <- message.asJmsTextMessage
                                          text <- tm.getText
                                        } yield
                                          if (text.toInt % 2 == 0)
                                            TransactionAction.send[IO](messageFactory(tm, outputQueueName1))
                                          else TransactionAction.send[IO](messageFactory(tm, outputQueueName2))
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

      val res = for {
        connection <- connectionRes
        session    <- connection.createSession(SessionType.AutoAcknowledge)
        queue      <- Resource.liftF(session.createQueue(inputQueueName))
        producer   <- session.createProducer(queue)
        messages   <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer   <- jmsClient.createAcknowledgerConsumer(connection, inputQueueName, poolSize)
      } yield (consumer, producer, bodies.toSet, messages)

      res.use {
        case (consumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- consumer.handle { message =>
                              for {
                                tm   <- message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield AckAction.ack
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to an other queue, with acknowledge" in {

      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer <- jmsClient.createAcknowledgerToProducer(
                     connection,
                     inputQueueName,
                     outputQueueName1,
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { received =>
                                        received.asJmsTextMessage.map(
                                          textMessage =>
                                            AckAction.send[IO](messageFactory(textMessage, outputQueueName1))
                                        )
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to other queues, with acknowledge" in {

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
        consumer <- jmsClient.createAcknowledgerToProducers(
                     connection,
                     inputQueueName,
                     NonEmptyList.of(outputQueueName1, outputQueueName2),
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { message =>
                                        for {
                                          tm   <- message.asJmsTextMessage
                                          text <- tm.getText
                                        } yield
                                          if (text.toInt % 2 == 0)
                                            AckAction.send[IO](messageFactory(tm, outputQueueName1))
                                          else AckAction.send[IO](messageFactory(tm, outputQueueName2))
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

    s"publish $nMessages messages and then consume them concurrently with auto-acknowledge" in {

      val res = for {
        connection <- connectionRes
        session    <- connection.createSession(SessionType.AutoAcknowledge)
        queue      <- Resource.liftF(session.createQueue(inputQueueName))
        producer   <- session.createProducer(queue)
        messages   <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer   <- jmsClient.createAutoAcknowledgerConsumer(connection, inputQueueName, poolSize)
      } yield (consumer, producer, bodies.toSet, messages)

      res.use {
        case (consumer, producer, bodies, messages) =>
          for {
            _        <- messages.traverse_(msg => producer.send(msg))
            _        <- logger.info(s"Pushed ${messages.size} messages.")
            received <- Ref.of[IO, Set[String]](Set())
            consumerFiber <- consumer.handle { message =>
                              for {
                                tm   <- message.asJmsTextMessage
                                body <- tm.getText
                                _    <- received.update(_ + body)
                              } yield Action.noOp
                            }.start
            _ <- logger.info(s"Consumer started.\nCollecting messages from the queue...")
            receivedMessages <- (received.get.iterateUntil(_.eqv(bodies)).timeout(timeout) >> received.get)
                                 .guarantee(consumerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to an other queue, with auto-acknowledge" in {

      val res = for {
        connection     <- connectionRes
        session        <- connection.createSession(SessionType.AutoAcknowledge)
        inputQueue     <- Resource.liftF(session.createQueue(inputQueueName))
        outputQueue    <- Resource.liftF(session.createQueue(outputQueueName1))
        inputProducer  <- session.createProducer(inputQueue)
        outputConsumer <- session.createConsumer(outputQueue)
        bodies         = (0 until nMessages).map(i => s"$i")
        messages       <- Resource.liftF(bodies.toList.traverse(i => session.createTextMessage(i)))
        consumer <- jmsClient.createAutoAcknowledgerToProducer(
                     connection,
                     inputQueueName,
                     outputQueueName1,
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { _ =>
                                        IO(Action.send(outputQueueName1))
                                      }.start
            _        <- logger.info(s"Consumer to Producer started.\nCollecting messages from output queue...")
            received <- Ref.of[IO, Set[String]](Set())
            receivedMessages <- (receiveUntil(outputConsumer, received, nMessages).timeout(timeout) >> received.get)
                                 .guarantee(consumerToProducerFiber.cancel)
          } yield assert(receivedMessages == bodies)
      }
    }

    s"publish $nMessages messages, consume them concurrently and then republishing to other queues, with auto-acknowledge" in {

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
        consumer <- jmsClient.createAutoAcknowledgerToProducers(
                     connection,
                     inputQueueName,
                     NonEmptyList.of(outputQueueName1, outputQueueName2),
                     poolSize
                   )
      } yield (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies.toSet, messages)

      res.use {
        case (consumer, inputProducer, outputConsumer1, outputConsumer2, bodies, messages) =>
          for {
            _ <- messages.traverse_(msg => inputProducer.send(msg))
            _ <- logger.info(s"Pushed ${messages.size} messages.")
            consumerToProducerFiber <- consumer.handle { message =>
                                        for {
                                          tm   <- message.asJmsTextMessage
                                          text <- tm.getText
                                        } yield
                                          if (text.toInt % 2 == 0) Action.send(outputQueueName1)
                                          else Action.send(outputQueueName2)
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
