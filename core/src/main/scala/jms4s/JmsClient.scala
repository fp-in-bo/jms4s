package jms4s

import cats.data._
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import jms4s.config.DestinationName
import jms4s.jms._

import scala.concurrent.duration.{FiniteDuration, _}

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
