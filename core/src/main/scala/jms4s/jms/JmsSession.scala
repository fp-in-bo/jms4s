package jms4s.jms

import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Session
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage

class JmsSession[F[_]: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: Session,
  private val blocker: Blocker
) {

  def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(wrapped.createQueue(queue.value)))

  def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(wrapped.createTopic(topicName.value)))

  def createDestination(destination: DestinationName): F[JmsDestination] = destination match {
    case q: QueueName => createQueue(q).widen[JmsDestination]
    case t: TopicName => createTopic(t).widen[JmsDestination]
  }

  def createConsumer(
    jmsDestination: JmsDestination
  )(implicit CS: ContextShift[F], C: Concurrent[F]): Resource[F, JmsMessageConsumer[F]] =
    for {
      consumer <- Resource.make(
                   Logger[F].info(
                     s"Opening MessageConsumer for Destination ${jmsDestination.wrapped}, Session: $wrapped..."
                   ) *>
                     Sync[F].delay(wrapped.createConsumer(jmsDestination.wrapped))
                 )(
                   c =>
                     Logger[F].info(
                       s"Closing MessageConsumer $c for Destination ${jmsDestination.wrapped}, Session: $wrapped..."
                     ) *>
                       Sync[F].delay(c.close()) *>
                       Logger[F].info(
                         s"Closed MessageConsumer $c for Destination ${jmsDestination.wrapped}, Session: $wrapped."
                       )
                 )
      _ <- Resource.liftF(
            Logger[F].info(
              s"Opened MessageConsumer $consumer for Destination ${jmsDestination.wrapped}, Session: $wrapped."
            )
          )
    } yield new JmsMessageConsumer[F](consumer)

  def createProducer(jmsDestination: JmsDestination): Resource[F, JmsMessageProducer[F]] =
    for {
      producer <- Resource.make(
                   Logger[F].info(
                     s"Opening MessageProducer for Destination ${jmsDestination.wrapped}, Session: $wrapped..."
                   ) *>
                     Sync[F].delay(wrapped.createProducer(jmsDestination.wrapped))
                 )(
                   c =>
                     Logger[F].info(
                       s"Closing MessageProducer $c for Destination ${jmsDestination.wrapped}, Session: $wrapped..."
                     ) *>
                       Sync[F].delay(c.close()) *>
                       Logger[F].info(
                         s"Closed MessageProducer $c for Destination ${jmsDestination.wrapped}, Session: $wrapped."
                       )
                 )
      _ <- Resource.liftF(
            Logger[F]
              .info(s"Opened MessageProducer $producer for Destination ${jmsDestination.wrapped}, Session: $wrapped.")
          )
    } yield new JmsMessageProducer(producer)

  val createUnidentifiedProducer: Resource[F, JmsUnidentifiedMessageProducer[F]] =
    for {
      producer <- Resource.make(
                   Logger[F].info(s"Opening unidentified MessageProducer, Session: $wrapped...") *>
                     Sync[F].delay(wrapped.createProducer(null))
                 )(
                   c =>
                     Logger[F].info(
                       s"Closing unidentified MessageProducer $c, Session: $wrapped..."
                     ) *>
                       Sync[F].delay(c.close()) *>
                       Logger[F].info(
                         s"Closed unidentified MessageProducer $c, Session: $wrapped."
                       )
                 )
      _ <- Resource.liftF(Logger[F].info(s"Opened unidentified MessageProducer $producer, Session: $wrapped."))
    } yield new JmsUnidentifiedMessageProducer(producer)

  val createMessage: F[JmsMessage[F]] =
    Sync[F].delay(new JmsMessage(wrapped.createMessage()))

  val createTextMessage: F[JmsTextMessage[F]] =
    Sync[F].delay(new JmsTextMessage(wrapped.createTextMessage()))

  def createTextMessage(string: String): F[JmsTextMessage[F]] =
    Sync[F].delay(new JmsTextMessage(wrapped.createTextMessage(string)))

  def commit(implicit CS: ContextShift[F]): F[Unit] =
    blocker.blockOn(Sync[F].delay(wrapped.commit()))

  def rollback(implicit CS: ContextShift[F]): F[Unit] =
    blocker.blockOn(Sync[F].delay(wrapped.rollback()))
}
