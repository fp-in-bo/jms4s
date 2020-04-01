package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsTransactedConsumer.JmsTransactedConsumerPool.{ JmsResource, Received }
import jms4s.JmsTransactedConsumer.TransactionResult
import jms4s.JmsTransactedConsumer.TransactionResult.Destination
import jms4s.config.{ DestinationName, QueueName }
import jms4s.jms.{ JmsConnection, JmsMessage, JmsMessageConsumer, JmsSession }
import jms4s.model.SessionType
import jms4s.model.SessionType.Transacted

import scala.concurrent.duration.FiniteDuration

trait JmsTransactedConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[TransactionResult]): F[Unit]
}

object JmsTransactedConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      input <- Resource.liftF(connection.createSession(Transacted).use(_.createDestination(inputDestinationName)))
      pool  <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(input)
              _        <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, Map.empty)))
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestinations <- Resource.liftF(
                             outputDestinationNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(SessionType.Transacted)
                                     .use(_.createDestination(outputDestinationName))
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(inputDestination)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producers.toSortedMap)))
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(SessionType.Transacted)
                              .use(_.createDestination(outputDestinationName))
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(SessionType.Transacted)
              consumer    <- session.createConsumer(inputDestination)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _           <- Resource.liftF(pool.enqueue1(JmsResource(session, consumer, producer)))
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private def build[F[_]: ContextShift: Concurrent](
    pool: JmsTransactedConsumerPool[F],
    concurrencyLevel: Int
  ): JmsTransactedConsumer[F] =
    (f: JmsMessage[F] => F[TransactionResult]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              received <- pool.receive
              tResult  <- f(received.message)
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

  private[jms4s] class JmsTransactedConsumerPool[F[_]: Concurrent: ContextShift](pool: Queue[F, JmsResource[F]]) {

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

  object JmsTransactedConsumerPool {

    private[jms4s] case class JmsResource[F[_]] private[jms4s] (
      session: JmsSession[F],
      consumer: JmsMessageConsumer[F],
      producers: Map[DestinationName, JmsProducer[F]]
    )

    private[jms4s] case class Received[F[_]] private (message: JmsMessage[F], resource: JmsResource[F])

  }

  sealed abstract class TransactionResult extends Product with Serializable

  object TransactionResult {

    private[jms4s] case object Commit extends TransactionResult

    private[jms4s] case object Rollback extends TransactionResult

    private[jms4s] case class Send(destinations: NonEmptyList[Destination]) extends TransactionResult

    private[jms4s] case class Destination(queueName: DestinationName, delay: Option[FiniteDuration])

    val commit: TransactionResult   = Commit
    val rollback: TransactionResult = Rollback

    def sendTo(queueNames: QueueName*): Send =
      Send(NonEmptyList.fromListUnsafe(queueNames.toList.map(name => Destination(name, None))))

    def sendWithDelayTo(queueNames: (QueueName, FiniteDuration)*): Send = Send(
      NonEmptyList.fromListUnsafe(
        queueNames.toList.map(x => Destination(x._1, Some(x._2)))
      )
    )
  }
}
