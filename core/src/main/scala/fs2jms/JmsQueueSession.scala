package fs2jms

import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2jms.config.{ QueueName, TopicName }
import io.chrisdavenport.log4cats.Logger
import javax.jms.{ QueueSession, Session }

class JmsQueueSession[F[_]: Sync: Logger] private[fs2jms] (
  private[fs2jms] val wrapped: QueueSession,
  private val blocker: Blocker
) {

  def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(wrapped.createQueue(queue.value)))

  def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(wrapped.createTopic(topicName.value)))

  def createConsumer(
    queue: JmsQueue
  )(implicit CS: ContextShift[F], C: Concurrent[F]): Resource[F, JmsMessageConsumer[F]] =
    for {
      consumer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageConsumer for ${queue.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createConsumer(queue.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageConsumer for ${queue.wrapped}, session: $wrapped."))
    } yield new JmsMessageConsumer[F](consumer)

  def createProducer(queue: JmsQueue): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageProducer for queue ${queue.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(queue.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for queue ${queue.wrapped}, session: $wrapped."))
    } yield new JmsMessageProducer(producer)

  def createProducer(topic: JmsTopic): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageProducer for topic ${topic.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(topic.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for topic ${topic.wrapped}, session: $wrapped."))
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

class JmsSession[F[_]: Sync: Logger] private[fs2jms] (
  private[fs2jms] val wrapped: Session,
  private val blocker: Blocker
) {

  def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(wrapped.createQueue(queue.value)))

  def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(wrapped.createTopic(topicName.value)))

  def createConsumer(
    queue: JmsQueue
  )(implicit CS: ContextShift[F], C: Concurrent[F]): Resource[F, JmsMessageConsumer[F]] =
    for {
      consumer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageConsumer for ${queue.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createConsumer(queue.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageConsumer for ${queue.wrapped}, session: $wrapped."))
    } yield new JmsMessageConsumer[F](consumer)

  def createProducer(queue: JmsQueue): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageProducer for queue ${queue.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(queue.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for queue ${queue.wrapped}, session: $wrapped."))
    } yield new JmsMessageProducer(producer)

  def createProducer(topic: JmsTopic): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.fromAutoCloseable(
                   Logger[F].info(s"Opening MessageProducer for topic ${topic.wrapped}, session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(topic.wrapped))
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for topic ${topic.wrapped}, session: $wrapped."))
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
