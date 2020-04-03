package jms4s

import cats.data._
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.concurrent.Queue
import jms4s.JmsProducer.JmsProducerPool
import jms4s.JmsUnidentifiedProducer.JmsUnidentifiedProducerPool
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms._
import jms4s.model.SessionType

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createTransactedConsumer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(connection, inputDestinationName, concurrencyLevel)

  def createTransactedConsumerToProducers(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(connection, inputDestinationName, outputDestinationNames, concurrencyLevel)

  def createTransactedConsumerToProducer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(connection, inputDestinationName, outputDestinationName, concurrencyLevel)

  def createAcknowledgerConsumer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    JmsAcknowledgerConsumer.make(connection, inputDestinationName, concurrencyLevel)

  def createAcknowledgerToProducers(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    JmsAcknowledgerConsumer.make(connection, inputDestinationName, outputDestinationNames, concurrencyLevel)

  def createAcknowledgerToProducer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    JmsAcknowledgerConsumer.make(connection, inputDestinationName, outputDestinationName, concurrencyLevel)

  def createAutoAcknowledgerConsumer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    JmsAutoAcknowledgerConsumer.make(connection, inputDestinationName, concurrencyLevel)

  def createAutoAcknowledgerToProducers(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    JmsAutoAcknowledgerConsumer.make(connection, inputDestinationName, outputDestinationNames, concurrencyLevel)

  def createAutoAcknowledgerToProducer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    JmsAutoAcknowledgerConsumer.make(connection, inputDestinationName, outputDestinationName, concurrencyLevel)

  def createProducer(
    connection: JmsConnection[F],
    destinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsPooledProducer[F]] =
    for {

      pool <- Resource.liftF(
               Queue.bounded[F, JmsProducerPool.JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session <- connection.createSession(SessionType.AutoAcknowledge)
              destination <- Resource.liftF(destinationName match {
                              case q: QueueName => session.createQueue(q).widen[JmsDestination]
                              case t: TopicName => session.createTopic(t).widen[JmsDestination]
                            })
              producer <- session.createProducer(destination)
              _        <- Resource.liftF(pool.enqueue1(JmsProducerPool.JmsResource(session, producer)))
            } yield ()
          }
    } yield new JmsPooledProducer(new JmsProducerPool(pool))

  def createProducer(
    connection: JmsConnection[F],
    concurrencyLevel: Int
  ): Resource[F, JmsUnidentifiedPooledProducer[F]] =
    for {
      pool <- Resource.liftF(
               Queue.bounded[F, JmsUnidentifiedProducerPool.JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.AutoAcknowledge)
              producer <- session.createUnidentifiedProducer
              _        <- Resource.liftF(pool.enqueue1(JmsUnidentifiedProducerPool.JmsResource(session, producer)))
            } yield ()
          }
    } yield new JmsUnidentifiedPooledProducer(new JmsUnidentifiedProducerPool(pool))
}
