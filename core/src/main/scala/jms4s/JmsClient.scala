package jms4s

import cats.data._
import cats.effect.{ Concurrent, ContextShift, Resource }
import jms4s.config.DestinationName
import jms4s.jms._

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createTransactedConsumerProgram(
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createTransactedConsumer(
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(connection, inputDestinationName, concurrencyLevel)

  def createTransactedConsumer(
    connection: JmsContext[F],
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
    JmsPooledProducer.make(connection, destinationName, concurrencyLevel)

  def createProducer(
    connection: JmsConnection[F],
    concurrencyLevel: Int
  ): Resource[F, JmsUnidentifiedPooledProducer[F]] =
    JmsUnidentifiedPooledProducer.make(connection, concurrencyLevel)

}
