package jms4s.jms

import cats.effect.Sync
import javax.jms.MessageProducer

import scala.concurrent.duration.FiniteDuration

class JmsMessageProducer[F[_]: Sync] private[jms4s] (private[jms4s] val value: MessageProducer) {

  def send(message: JmsMessage[F]): F[Unit] =
    Sync[F].delay(value.send(message.wrapped))

  def setDeliveryDelay(deliveryDelay: FiniteDuration): F[Unit] =
    Sync[F].delay(value.setDeliveryDelay(deliveryDelay.toMillis))

}

class JmsUnidentifiedMessageProducer[F[_]: Sync] private[jms4s] (private[jms4s] val value: MessageProducer) {

  def send(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
    Sync[F].delay(value.send(destination.wrapped, message.wrapped))

  def setDeliveryDelay(deliveryDelay: FiniteDuration): F[Unit] =
    Sync[F].delay(value.setDeliveryDelay(deliveryDelay.toMillis))

}
