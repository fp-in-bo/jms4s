package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAutoAcknowledgerConsumer.Action
import jms4s.JmsAutoAcknowledgerConsumer.Action.Destination
import jms4s.JmsAutoAcknowledgerConsumer.JmsAutoAcknowledgerConsumerPool.JmsResource
import jms4s.config.{ DestinationName, QueueName }
import jms4s.jms.{ JmsConnection, JmsMessage, JmsMessageConsumer }
import jms4s.model.SessionType.AutoAcknowledge

import scala.concurrent.duration.FiniteDuration

trait JmsAutoAcknowledgerConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[Action]): F[Unit]
}

object JmsAutoAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      input <- Resource.liftF(
                connection.createSession(AutoAcknowledge).use(_.createDestination(inputDestinationName))
              )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(AutoAcknowledge)
              consumer <- session.createConsumer(input)
              _        <- Resource.liftF(pool.enqueue1(JmsResource(consumer, Map.empty)))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(AutoAcknowledge)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestinations <- Resource.liftF(
                             outputDestinationNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(AutoAcknowledge)
                                     .use(_.createDestination(outputDestinationName))
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(AutoAcknowledge)
              consumer <- session.createConsumer(inputDestination)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(pool.enqueue1(JmsResource(consumer, producers.toSortedMap)))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(AutoAcknowledge)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(AutoAcknowledge)
                              .use(_.createDestination(outputDestinationName))
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(AutoAcknowledge)
              consumer    <- session.createConsumer(inputDestination)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _           <- Resource.liftF(pool.enqueue1(JmsResource(consumer, producer)))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, JmsResource[F]],
    concurrencyLevel: Int,
    blocker: Blocker
  ): JmsAutoAcknowledgerConsumer[F] =
    (f: JmsMessage[F] => F[Action]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              resource <- pool.dequeue1
              message  <- resource.consumer.receiveJmsMessage
              res      <- f(message)
              _ <- res match {
                    case Action.NoOp => Sync[F].unit
                    case Action.Send(destinations) =>
                      blocker.blockOn(
                        destinations.traverse_ {
                          case Destination(name, delay) =>
                            delay.fold(
                              ifEmpty = resource
                                .producers(name)
                                .publish(message)
                            )(
                              f = d =>
                                resource
                                  .producers(name)
                                  .publish(message, d)
                            )
                        } *> Sync[F].delay(message.wrapped.acknowledge())
                      )
                  }
              _ <- pool.enqueue1(resource)
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  object JmsAutoAcknowledgerConsumerPool {
    private[jms4s] case class JmsResource[F[_]] private[jms4s] (
      consumer: JmsMessageConsumer[F],
      producers: Map[DestinationName, JmsProducer[F]]
    )
  }

  sealed abstract class Action extends Product with Serializable

  object Action {
    private[jms4s] case object NoOp extends Action

    private[jms4s] case class Send(destinations: NonEmptyList[Destination]) extends Action

    private[jms4s] case class Destination(queueName: DestinationName, delay: Option[FiniteDuration])

    val noOp: Action = NoOp

    def send(queueNames: QueueName*): Send =
      Send(NonEmptyList.fromListUnsafe(queueNames.toList.map(name => Destination(name, None))))

    def sendWithDelayTo(queueNames: (QueueName, FiniteDuration)*): Send = Send(
      NonEmptyList.fromListUnsafe(
        queueNames.toList.map(x => Destination(x._1, Some(x._2)))
      )
    )
  }
}
