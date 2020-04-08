package jms4s.jms

import cats.effect.{ Blocker, ContextShift, Sync }
import javax.jms.MessageProducer

import scala.concurrent.duration.FiniteDuration

class JmsMessageProducer[F[_]: Sync: ContextShift] private[jms4s] (
  private[jms4s] val value: MessageProducer,
  private[jms4s] val blocker: Blocker
) {

  def send(message: JmsMessage[F]): F[Unit] =
    blocker.delay(value.send(message.wrapped))

  def setDeliveryDelay(deliveryDelay: FiniteDuration): F[Unit] =
    blocker.delay(value.setDeliveryDelay(deliveryDelay.toMillis))

}

class JmsUnidentifiedMessageProducer[F[_]: Sync: ContextShift] private[jms4s] (
  private[jms4s] val value: MessageProducer,
  private[jms4s] val blocker: Blocker
) {

  def send(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
    blocker.delay(value.send(destination.wrapped, message.wrapped))

  def setDeliveryDelay(deliveryDelay: FiniteDuration): F[Unit] =
    blocker.delay(value.setDeliveryDelay(deliveryDelay.toMillis))

}
