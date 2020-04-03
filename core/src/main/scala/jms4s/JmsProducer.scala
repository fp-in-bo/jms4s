package jms4s

import cats.effect.{ ContextShift, Sync }
import jms4s.jms.{ JmsMessage, JmsMessageProducer }
import cats.implicits._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class JmsProducer[F[_]: Sync: ContextShift] private[jms4s] (private[jms4s] val producer: JmsMessageProducer[F]) {

  def publish(message: JmsMessage[F]): F[Unit] =
    producer.send(message)

  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)
}
