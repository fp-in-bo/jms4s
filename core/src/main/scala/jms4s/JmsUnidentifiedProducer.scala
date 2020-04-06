package jms4s

import cats.effect.{ ContextShift, Sync }
import jms4s.jms.{ JmsDestination, JmsMessage, JmsUnidentifiedMessageProducer }
import cats.implicits._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class JmsUnidentifiedProducer[F[_]: Sync: ContextShift] private[jms4s] (
  private[jms4s] val producer: JmsUnidentifiedMessageProducer[F]
) {

  def publish(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
    producer.send(destination, message)

  def publish(destination: JmsDestination, message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(destination, message) >> producer.setDeliveryDelay(0.millis)
}
