package jms4s

import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import fs2.concurrent.Queue
import jms4s.jms._
import cats.implicits._
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

trait JmsPooledProducer[F[_]] {
  def publish(message: JmsMessage[F]): F[Unit]
  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit]
}

object JmsPooledProducer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    destinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsPooledProducer[F]] =
    for {

      pool <- Resource.liftF(
               Queue.bounded[F, JmsProducerPool.JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session <- connection.createSession(SessionType.AutoAcknowledge)
              destination <- Resource.liftF(destinationName match {
                              case q: QueueName => session.createQueue(q).widen[JmsDestination]
                              case t: TopicName => session.createTopic(t).widen[JmsDestination]
                            })
              producer <- session.createProducer(destination)
              _        <- Resource.liftF(pool.enqueue1(JmsProducerPool.JmsResource(session, producer)))
            } yield ()
          }
    } yield new JmsPooledProducer(new JmsProducerPool(pool))

  class JmsProducer[F[_]: Sync: ContextShift] private[jms4s] (private[jms4s] val producer: JmsMessageProducer[F]) {

    def publish(message: JmsMessage[F]): F[Unit] =
      producer.send(message)

    def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
      producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)
  }

  class JmsProducerPool[F[_]: Concurrent: ContextShift] private[jms4s] (
    private val pool: Queue[F, JmsProducerPool.JmsResource[F]]
  ) {

    def send(messages: List[JmsMessage[F]]): F[Unit] =
      for {
        resource <- pool.dequeue1
        _        <- messages.traverse_(resource.producer.send)
        _        <- pool.enqueue1(resource)
      } yield ()

    def sendWithDelay(messagesWithDuration: List[(JmsMessage[F], FiniteDuration)]): F[Unit] =
      for {
        resource <- pool.dequeue1
        _ <- messagesWithDuration.traverse_(
              messageWithDuration =>
                resource.producer.setDeliveryDelay(messageWithDuration._2) *> resource.producer
                  .send(messageWithDuration._1)
            )
        _ <- resource.producer.setDeliveryDelay(0.millis)
        _ <- pool.enqueue1(resource)
      } yield ()
  }

  object JmsProducerPool {

    case class JmsResource[F[_]] private[jms4s] (session: JmsSession[F], producer: JmsMessageProducer[F])

  }

}
