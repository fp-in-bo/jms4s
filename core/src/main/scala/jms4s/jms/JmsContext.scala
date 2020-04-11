package jms4s.jms

import cats.effect.{ Blocker, ContextShift, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSContext
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

class JmsContext[F[_]: Sync: Logger: ContextShift](
  private val context: JMSContext,
  private[jms4s] val blocker: Blocker
) {

  def createContext(sessionType: SessionType): Resource[F, JmsContext[F]] =
    Resource
      .make(
        Logger[F].info("Creating context") *>
          blocker.delay(context.createContext(sessionType.rawAcknowledgeMode))
      )(
        context =>
          Logger[F].info("Releasing context") *>
            blocker.delay(context.close())
      )
      .map(context => new JmsContext(context, blocker))

  def send(destinationName: DestinationName, message: JmsMessage[F]): F[Unit] =
    createDestination(destinationName)
      .flatMap(
        destination => blocker.delay(context.createProducer().send(destination.wrapped, message.wrapped))
      )
      .map(_ => ())

  def send(destinationName: DestinationName, message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    for {
      destination <- createDestination(destinationName)
      p           <- Sync[F].delay(context.createProducer())
      _           <- Sync[F].delay(p.setDeliveryDelay(delay.toMillis))
      _           <- blocker.delay(p.send(destination.wrapped, message.wrapped))
    } yield ()

  def receive(destinationName: DestinationName): F[JmsMessage[F]] =
    createDestination(destinationName).flatMap(
      destination =>
        blocker
          .delay(
            context.createConsumer(destination.wrapped).receive()
          )
          .map(m => new JmsMessage[F](m))
    )

  def createTextMessage(value: String): F[JmsTextMessage[F]] =
    Sync[F].delay(new JmsTextMessage(context.createTextMessage(value)))

  def commit: F[Unit] = blocker.delay(context.commit())

  def rollback: F[Unit] = blocker.delay(context.rollback())

  private def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(context.createQueue(queue.value)))

  private def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(context.createTopic(topicName.value)))

  private def createDestination(destination: DestinationName): F[JmsDestination] = destination match {
    case q: QueueName => createQueue(q).widen[JmsDestination]
    case t: TopicName => createTopic(t).widen[JmsDestination]
  }

}
