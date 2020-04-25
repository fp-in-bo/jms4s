package jms4s

import cats.effect.{ Concurrent, ContextShift, Resource }
import jms4s.config.DestinationName
import jms4s.jms._

class JmsClient[F[_]: ContextShift: Concurrent] private[jms4s] (private[jms4s] val context: JmsContext[F]) {

  def createTransactedConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createAutoAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    JmsAutoAcknowledgerConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    JmsAcknowledgerConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createProducer(
    concurrencyLevel: Int
  ): Resource[F, JmsProducer[F]] =
    JmsProducer.make(context, concurrencyLevel)

}
