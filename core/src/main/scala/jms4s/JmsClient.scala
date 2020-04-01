package jms4s

import cats.data._
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.concurrent.Queue
import jms4s.JmsTransactedConsumer.JmsConsumerPool
import jms4s.JmsTransactedConsumer.JmsConsumerPool.JmsResource
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType
import jms4s.model.SessionType.Transacted

import scala.concurrent.duration.{ FiniteDuration, _ }

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createTransactedConsumer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      input <- Resource.liftF(connection.createSession(Transacted).use(_.createDestination(inputDestinationName)))
      pool  <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(input)
              _        <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, Map.empty)))
            } yield ()
          }
    } yield JmsTransactedConsumer.make(new JmsConsumerPool(pool), concurrencyLevel)

  def createTransactedConsumerToProducers(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestinations <- Resource.liftF(
                             outputDestinationNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(SessionType.Transacted)
                                     .use(_.createDestination(outputDestinationName))
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(inputDestination)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producers.toSortedMap)))
            } yield ()
          }
    } yield JmsTransactedConsumer.make(new JmsConsumerPool(pool), concurrencyLevel)

  def createTransactedConsumerToProducer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(SessionType.Transacted)
                              .use(_.createDestination(outputDestinationName))
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(SessionType.Transacted)
              consumer    <- session.createConsumer(inputDestination)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _           <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producer)))
            } yield ()
          }
    } yield JmsTransactedConsumer.make(new JmsConsumerPool(pool), concurrencyLevel)
}

class JmsProducer[F[_]: Sync: ContextShift] private[jms4s] (private[jms4s] val producer: JmsMessageProducer[F]) {

  def publish(message: JmsMessage[F]): F[Unit] =
    producer.send(message)

  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)
}

class JmsUnidentifiedProducer[F[_]: Sync: ContextShift] private[jms4s] (
  private[jms4s] val producer: JmsUnidentifiedMessageProducer[F]
) {

  def publish(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
    producer.send(destination, message)

  def publish(destination: JmsDestination, message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(destination, message) >> producer.setDeliveryDelay(0.millis)
}
