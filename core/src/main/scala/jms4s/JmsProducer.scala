package jms4s

import cats.effect.{ ContextShift, Sync }
import cats.implicits._
import jms4s.jms.{ JmsMessage, JmsMessageProducer }

import scala.concurrent.duration.{ FiniteDuration, _ }

class JmsProducer[F[_]: Sync: ContextShift] private[jms4s] (private[jms4s] val producer: JmsMessageProducer[F]) {

  def publish(message: JmsMessage[F]): F[Unit] =
    producer.send(message)

  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)
}
