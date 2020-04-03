package jms4s

import cats.effect.{ Concurrent, ContextShift, Sync }
import fs2.concurrent.Queue
import jms4s.config.DestinationName
import jms4s.jms.{ JmsDestination, JmsMessage, JmsSession, JmsUnidentifiedMessageProducer }
import cats.implicits._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

trait JmsUnidentifiedProducer[F[_]] {
  def publish(message: JmsMessage[F]): F[Unit]
  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit]
}

object JmsUnidentifiedProducer {

  class JmsUnidentifiedProducer[F[_]: Sync: ContextShift] private[jms4s] (
    private[jms4s] val producer: JmsUnidentifiedMessageProducer[F]
  ) {

    def publish(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
      producer.send(destination, message)

    def publish(destination: JmsDestination, message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
      producer.setDeliveryDelay(delay) >> producer.send(destination, message) >> producer.setDeliveryDelay(0.millis)
  }

  object JmsUnidentifiedProducerPool {
    case class JmsResource[F[_]] private[jms4s] (session: JmsSession[F], producer: JmsUnidentifiedMessageProducer[F])
  }

  class JmsUnidentifiedProducerPool[F[_]: Concurrent: ContextShift] private[jms4s] (
    private val pool: Queue[F, JmsUnidentifiedProducerPool.JmsResource[F]]
  ) {

    def send(messagesWithQueues: List[(JmsMessage[F], DestinationName)]): F[Unit] =
      for {
        resource <- pool.dequeue1
        _ <- messagesWithQueues.traverse(
              messageWithQueue =>
                for {
                  destination <- resource.session.createDestination(messageWithQueue._2)
                  _           <- resource.producer.send(destination, messageWithQueue._1)
                } yield ()
            )
        _ <- pool.enqueue1(resource)
      } yield ()

    def sendWithDelay(messagesWithDuration: List[((JmsMessage[F], DestinationName), FiniteDuration)]): F[Unit] =
      for {
        resource <- pool.dequeue1
        _ <- messagesWithDuration.traverse_(
              messageWithDuration =>
                for {
                  destination <- resource.session.createDestination(messageWithDuration._1._2)
                  _ <- resource.producer.setDeliveryDelay(messageWithDuration._2) *>
                        resource.producer.send(destination, messageWithDuration._1._1)
                } yield ()
            )
        _ <- resource.producer.setDeliveryDelay(0.millis)
        _ <- pool.enqueue1(resource)
      } yield ()
  }
}
