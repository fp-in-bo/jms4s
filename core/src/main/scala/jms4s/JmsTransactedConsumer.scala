package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsTransactedConsumer.JmsTransactedConsumerPool.Received
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsTransactedConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[TransactionAction[F]]): F[Unit]
}

object JmsTransactedConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      pool <- Resource.liftF(Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F])](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList
            .traverse_(
              _ =>
                for {
                  c        <- context.createContext(SessionType.Transacted)
                  consumer <- c.createJmsConsumer(inputDestinationName)
                  _        <- Resource.liftF(pool.enqueue1((c, consumer)))
                } yield ()
            )
    } yield build(new JmsTransactedConsumerPool[F](pool), concurrencyLevel, MessageFactory[F](context))

  private def build[F[_]: ContextShift: Concurrent](
    pool: JmsTransactedConsumerPool[F],
    concurrencyLevel: Int,
    messageFactory: MessageFactory[F]
  ): JmsTransactedConsumer[F] =
    (f: JmsMessage[F] => F[TransactionAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            fo = for {
              received <- pool.receive
              tResult  <- f(received.message)
              _ <- tResult.fold(
                    ifCommit = pool.commit(received.context, received.consumer),
                    ifRollback = pool.rollback(received.context, received.consumer),
                    ifSend = send => {
                      send
                        .createMessages(messageFactory)
                        .flatMap(
                          toSend =>
                            toSend.messagesAndDestinations.traverse_ {
                              case (message, (name, delay)) =>
                                delay.fold(
                                  received.context.send(name, message)
                                )(
                                  d => received.context.send(name, message, d)
                                ) *> pool.commit(received.context, received.consumer)
                            }
                        )
                    }
                  )
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  private[jms4s] class JmsTransactedConsumerPool[F[_]: Concurrent: ContextShift](
    pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F])]
  ) {

    val receive: F[Received[F]] =
      for {
        (context, consumer) <- pool.dequeue1
        message             <- consumer.receiveJmsMessage
      } yield Received(message, context, consumer)

    def commit(context: JmsContext[F], consumer: JmsMessageConsumer[F]): F[Unit] =
      for {
        _ <- context.commit
        _ <- pool.enqueue1((context, consumer))
      } yield ()

    def rollback(context: JmsContext[F], consumer: JmsMessageConsumer[F]): F[Unit] =
      for {
        _ <- context.rollback
        _ <- pool.enqueue1((context, consumer))
      } yield ()
  }

  object JmsTransactedConsumerPool {
    case class Received[F[_]](message: JmsMessage[F], context: JmsContext[F], consumer: JmsMessageConsumer[F])
  }

  sealed abstract class TransactionAction[F[_]] extends Product with Serializable {
    def fold(ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: TransactionAction.Send[F] => F[Unit]): F[Unit]
  }

  object TransactionAction {

    private[jms4s] case class Commit[F[_]]() extends TransactionAction[F] {
      override def fold(ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifCommit
    }

    private[jms4s] case class Rollback[F[_]]() extends TransactionAction[F] {
      override def fold(ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifRollback
    }

    case class Send[F[_]](
      createMessages: MessageFactory[F] => F[ToSend[F]]
    ) extends TransactionAction[F] {
      override def fold(ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
    )

    def commit[F[_]]: TransactionAction[F] = Commit[F]()

    def rollback[F[_]]: TransactionAction[F] = Rollback[F]()

    def sendN[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
    ): Send[F] =
      Send[F](
        mf => messageFactory(mf).map(nel => nel.map { case (message, name) => (message, (name, None)) }).map(ToSend[F])
      )

    def sendNWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(ToSend[F]))

    def sendWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(x => ToSend[F](NonEmptyList.one(x))))

    def send[F[_]: Functor](messageFactory: MessageFactory[F] => F[(JmsMessage[F], DestinationName)]): Send[F] =
      Send[F](
        mf => messageFactory(mf).map { case (message, name) => ToSend[F](NonEmptyList.one((message, (name, None)))) }
      )
  }
}
