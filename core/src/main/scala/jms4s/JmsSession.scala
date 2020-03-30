package jms4s

import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Session
import jms4s.config.{ QueueName, TopicName }

class JmsSession[F[_]: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: Session,
  private val blocker: Blocker
) {

  def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(wrapped.createQueue(queue.value)))

  def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(wrapped.createTopic(topicName.value)))

  def createConsumer(
    jmsDestination: JmsDestination
  )(implicit CS: ContextShift[F], C: Concurrent[F]): Resource[F, JmsMessageConsumer[F]] =
    for {
      consumer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageConsumer for ${jmsDestination.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createConsumer(jmsDestination.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageConsumer for ${jmsDestination.wrapped}, session: $wrapped."))
    } yield new JmsMessageConsumer[F](consumer)

  def createProducer(jmsDestination: JmsDestination): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageProducer for ${jmsDestination.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(jmsDestination.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for ${jmsDestination.wrapped}, session: $wrapped."))
    } yield new JmsMessageProducer(producer)

  val createTextMessage: F[JmsTextMessage[F]] =
    Sync[F].delay(new JmsTextMessage(wrapped.createTextMessage()))

  def createTextMessage(string: String): F[JmsTextMessage[F]] =
    Sync[F].delay(new JmsTextMessage(wrapped.createTextMessage(string)))

  def commit(implicit CS: ContextShift[F]): F[Unit] =
    blocker.blockOn(Sync[F].delay(wrapped.commit()))

  def rollback(implicit CS: ContextShift[F]): F[Unit] =
    blocker.blockOn(Sync[F].delay(wrapped.rollback()))
}
