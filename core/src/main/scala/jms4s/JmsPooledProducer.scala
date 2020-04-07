package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import fs2.concurrent.Queue
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType
import cats.implicits._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

trait JmsPooledProducer[F[_]] {

  def sendN(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F])]]
  ): F[Unit]

  def sendNWithDelay(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], Option[FiniteDuration])]]
  ): F[Unit]

  def sendWithDelay(
    messageFactory: MessageFactory[F] => F[(JmsMessage[F], Option[FiniteDuration])]
  ): F[Unit]

  def send(messageFactory: MessageFactory[F] => F[JmsMessage[F]]): F[Unit]

}

object JmsPooledProducer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    queue: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsPooledProducer[F]] =
    for {
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(SessionType.AutoAcknowledge)
              destination <- Resource.liftF(session.createDestination(queue))
              producer    <- session.createProducer(destination)
              _           <- Resource.liftF(pool.enqueue1(JmsResource(session, producer, new MessageFactory(session))))
            } yield ()
          }
    } yield new JmsPooledProducer[F] {
      override def sendN(
        f: MessageFactory[F] => F[NonEmptyList[JmsMessage[F]]]
      ): F[Unit] =
        for {
          resources <- pool.dequeue1
          messages  <- f(resources.messageFactory)
          _ <- messages.traverse_(
                message =>
                  for {
                    _ <- resources.producer
                          .send(message)
                  } yield ()
              )
          _ <- pool.enqueue1(resources)
        } yield ()

      override def sendNWithDelay(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], Option[FiniteDuration])]]
      ): F[Unit] =
        for {
          resources      <- pool.dequeue1
          messagesDelays <- f(resources.messageFactory)
          _ <- messagesDelays.traverse_(
                messageWithDelay =>
                  for {
                    _ <- messageWithDelay._2 match {
                          case Some(delay) => resources.producer.setDeliveryDelay(delay)
                          case None        => Sync[F].unit
                        }
                    _ <- resources.producer.send(messageWithDelay._1)
                    _ <- resources.producer.setDeliveryDelay(0.millis)
                  } yield ()
              )
          _ <- pool.enqueue1(resources)
        } yield ()

      override def sendWithDelay(
        f: MessageFactory[F] => F[(JmsMessage[F], Option[FiniteDuration])]
      ): F[Unit] =
        for {
          resources         <- pool.dequeue1
          messagesWithDelay <- f(resources.messageFactory)
          _ <- messagesWithDelay._2 match {
                case Some(delay) => resources.producer.setDeliveryDelay(delay)
                case None        => Sync[F].unit
              }
          _ <- resources.producer.send(messagesWithDelay._1)
          _ <- resources.producer.setDeliveryDelay(0.millis)
          _ <- pool.enqueue1(resources)

        } yield ()

      override def send(f: MessageFactory[F] => F[JmsMessage[F]]): F[Unit] =
        for {
          resources <- pool.dequeue1
          message   <- f(resources.messageFactory)
          _ <- for {
                _ <- resources.producer
                      .send(message)
              } yield ()
          _ <- pool.enqueue1(resources)
        } yield ()
    }

  case class JmsResource[F[_]] private[jms4s] (
    session: JmsSession[F],
    producer: JmsMessageProducer[F],
    messageFactory: MessageFactory[F]
  )

}
