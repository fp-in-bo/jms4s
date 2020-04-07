package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.concurrent.Queue
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType.AutoAcknowledge

import scala.concurrent.duration.{ FiniteDuration, _ }

trait JmsUnidentifiedPooledProducer[F[_]] {

  def sendN(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
  ): F[Unit]

  def sendNWithDelay(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
  ): F[Unit]

  def sendWithDelay(
    messageFactory: MessageFactory[F] => F[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
  ): F[Unit]

  def send(messageFactory: MessageFactory[F] => F[(JmsMessage[F], DestinationName)]): F[Unit]

}

object JmsUnidentifiedPooledProducer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    concurrencyLevel: Int
  ): Resource[F, JmsUnidentifiedPooledProducer[F]] =
    for {
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(AutoAcknowledge)
              producer <- session.createUnidentifiedProducer
              _        <- Resource.liftF(pool.enqueue1(JmsResource(session, producer, new MessageFactory(session))))
            } yield ()
          }
    } yield new JmsUnidentifiedPooledProducer[F] {
      override def sendN(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
      ): F[Unit] =
        for {
          resources                <- pool.dequeue1
          messagesWithDestinations <- f(resources.messageFactory)
          _ <- messagesWithDestinations.traverse_(
                messageWithDestination =>
                  for {
                    jmsDestination <- resources.session.createDestination(messageWithDestination._2)
                    _ <- resources.producer
                          .send(jmsDestination, messageWithDestination._1)
                  } yield ()
              )
          _ <- pool.enqueue1(resources)
        } yield ()

      override def sendNWithDelay(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
      ): F[Unit] =
        for {
          resources                          <- pool.dequeue1
          messagesWithDestinationsAndDelayes <- f(resources.messageFactory)
          _ <- messagesWithDestinationsAndDelayes.traverse_(
                messageWithDestinationAndDelay =>
                  for {
                    jmsDestination <- resources.session.createDestination(messageWithDestinationAndDelay._2._1)
                    _ <- messageWithDestinationAndDelay._2._2 match {
                          case Some(delay) => resources.producer.setDeliveryDelay(delay)
                          case None        => Sync[F].unit
                        }
                    _ <- resources.producer.send(jmsDestination, messageWithDestinationAndDelay._1)
                    _ <- resources.producer.setDeliveryDelay(0.millis)
                  } yield ()
              )
          _ <- pool.enqueue1(resources)
        } yield ()

      override def sendWithDelay(
        f: MessageFactory[F] => F[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
      ): F[Unit] =
        for {
          resources                       <- pool.dequeue1
          messagesWithDestinationAndDelay <- f(resources.messageFactory)
          jmsDestination                  <- resources.session.createDestination(messagesWithDestinationAndDelay._2._1)
          _ <- messagesWithDestinationAndDelay._2._2 match {
                case Some(a) => resources.producer.setDeliveryDelay(a)
                case None    => Sync[F].unit
              }
          _ <- resources.producer.send(jmsDestination, messagesWithDestinationAndDelay._1)
          _ <- resources.producer.setDeliveryDelay(0.millis)
          _ <- pool.enqueue1(resources)

        } yield ()

      override def send(f: MessageFactory[F] => F[(JmsMessage[F], DestinationName)]): F[Unit] =
        for {
          resources              <- pool.dequeue1
          messageWithDestination <- f(resources.messageFactory)
          _ <- for {
                jmsDestination <- resources.session.createDestination(messageWithDestination._2)
                _ <- resources.producer
                      .send(jmsDestination, messageWithDestination._1)
              } yield ()
          _ <- pool.enqueue1(resources)
        } yield ()
    }

  case class JmsResource[F[_]] private[jms4s] (
    session: JmsSession[F],
    producer: JmsUnidentifiedMessageProducer[F],
    messageFactory: MessageFactory[F]
  )

}
