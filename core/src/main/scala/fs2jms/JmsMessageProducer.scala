package fs2jms

import cats.effect.Sync
import javax.jms.MessageProducer

import scala.concurrent.duration.FiniteDuration

class JmsMessageProducer[F[_]: Sync] private[fs2jms] (private[fs2jms] val value: MessageProducer) {

  def send(message: JmsMessage[F]): F[Unit] =
    Sync[F].delay(value.send(message.wrapped))

  def setDeliveryDelay(deliveryDelay: FiniteDuration): F[Unit] =
    Sync[F].delay(value.setDeliveryDelay(deliveryDelay.toMillis))
}
