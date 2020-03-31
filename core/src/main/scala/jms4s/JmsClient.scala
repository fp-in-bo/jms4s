package jms4s

import cats.data._
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsConsumerPool.{ JmsResource, Received }
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms._
import jms4s.model.SessionType.Transacted
import jms4s.model.TransactionResult.Destination
import jms4s.model.{ SessionType, TransactionResult }

import scala.concurrent.duration.{ FiniteDuration, _ }

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createQueueTransactedConsumer(
    connection: JmsConnection[F],
    queueName: QueueName,
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F]] =
    for {
      queue <- Resource.liftF(connection.createSession(Transacted).use(_.createQueue(queueName)))
      pool  <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(queue)
              _        <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, Map.empty)))
            } yield ()
          }
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)

  def createQueueTransactedConsumerToProducers(
    connection: JmsConnection[F],
    inputQueueName: QueueName,
    outputQueueNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F]] =
    for {
      inputQueue <- Resource.liftF(
                     connection.createSession(SessionType.Transacted).use(_.createQueue(inputQueueName))
                   )
      outputDestinations <- Resource.liftF(
                             outputQueueNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(SessionType.Transacted)
                                     .use[JmsDestination] { s =>
                                       outputDestinationName match {
                                         case q @ QueueName(_) => s.createQueue(q).widen[JmsDestination]
                                         case t @ TopicName(_) => s.createTopic(t).widen[JmsDestination]
                                       }
                                     }
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(inputQueue)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producers.toSortedMap)))
            } yield ()
          }
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)

  // TODO evaluate if this can be rewritten in terms of `createQueueTransactedConsumerToProducers`
  // it's pretty much the same, but here it does not make any sense to have a NonEmptyMap[QueueName, JmsQueueProducer[F]]
  // since the producer is only one!
  def createQueueTransactedConsumerToProducer(
    connection: JmsConnection[F],
    inputQueueName: QueueName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F]] =
    for {
      inputQueue <- Resource.liftF(
                     connection.createSession(SessionType.Transacted).use(_.createQueue(inputQueueName))
                   )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(SessionType.Transacted)
                              .use[JmsDestination] { s =>
                                outputDestinationName match {
                                  case q @ QueueName(_) => s.createQueue(q).widen[JmsDestination]
                                  case t @ TopicName(_) => s.createTopic(t).widen[JmsDestination]
                                }
                              }
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(SessionType.Transacted)
              consumer    <- session.createConsumer(inputQueue)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _           <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producer)))
            } yield ()
          }
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)
}

class JmsQueueTransactedConsumer[F[_]: ContextShift] private[jms4s] (
  private val pool: JmsConsumerPool[F],
  private val concurrencyLevel: Int
)(implicit val F: Concurrent[F]) {

  def handle(f: Received[F] => F[TransactionResult]): F[Unit] =
    Stream
      .emits(0 until concurrencyLevel)
      .as(
        Stream.eval(
          for {
            received <- pool.receive
            tResult  <- f(received)
            _ <- tResult match {
                  case TransactionResult.Commit   => pool.commit(received.resource)
                  case TransactionResult.Rollback => pool.rollback(received.resource)
                  case TransactionResult.Send(destinations) =>
                    destinations.traverse_ {
                      case Destination(name, delay) =>
                        delay.fold(
                          received.resource
                            .producers(name)
                            .publish(received.message)
                        )(
                          d =>
                            received.resource
                              .producers(name)
                              .publish(received.message, d)
                        ) *> pool.commit(received.resource)
                    }
                }
          } yield ()
        )
      )
      .parJoin(concurrencyLevel)
      .repeat
      .compile
      .drain
}

class JmsProducer[F[_]: Sync: ContextShift] private[jms4s] (private[jms4s] val producer: JmsMessageProducer[F]) {

  def publish(message: JmsMessage[F]): F[Unit] =
    producer.send(message)

  def publish(message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)
}

// TODO evaluate if we really want this! May be useful when dealing with multiple destination?
class JmsUnidentifiedProducer[F[_]: Sync: ContextShift] private[jms4s] (
  private[jms4s] val producer: JmsUnidentifiedMessageProducer[F]
) {

  def publish(destination: JmsDestination, message: JmsMessage[F]): F[Unit] =
    producer.send(destination, message)

  def publish(destination: JmsDestination, message: JmsMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(destination, message) >> producer.setDeliveryDelay(0.millis)
}

class JmsConsumerPool[F[_]: Concurrent: ContextShift] private[jms4s] (private val pool: Queue[F, JmsResource[F]]) {

  val receive: F[Received[F]] =
    for {
      resource <- pool.dequeue1
      msg      <- resource.consumer.receiveJmsMessage
    } yield Received(msg, resource)

  def commit(resource: JmsResource[F]): F[Unit] =
    for {
      _ <- resource.session.commit
      _ <- pool.enqueue1(resource)
    } yield ()

  def rollback(resource: JmsResource[F]): F[Unit] =
    for {
      _ <- resource.session.rollback
      _ <- pool.enqueue1(resource)
    } yield ()
}

object JmsConsumerPool {

  case class JmsResource[F[_]] private[jms4s] (
    session: JmsSession[F],
    consumer: JmsMessageConsumer[F],
    private[jms4s] val producers: Map[DestinationName, JmsProducer[F]]
  )

  case class Received[F[_]] private (message: JmsMessage[F], resource: JmsResource[F])

}
