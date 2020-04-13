package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsAcknowledgerConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[AckAction[F]]): F[Unit]
}

object JmsAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    for {
      pool <- Resource.liftF(Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F])](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(SessionType.ClientAcknowledge)
              consumer <- ctx.createJmsConsumer(inputDestinationName)
              _        <- Resource.liftF(pool.enqueue1((ctx, consumer)))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, context.blocker, MessageFactory[F](context))

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F])],
    concurrencyLevel: Int,
    blocker: Blocker,
    messageFactory: MessageFactory[F]
  ): JmsAcknowledgerConsumer[F] =
    (f: JmsMessage[F] => F[AckAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              (context, consumer) <- pool.dequeue1
              message             <- consumer.receiveJmsMessage
              res                 <- f(message)
              _ <- res.fold(
                    ifAck = blocker.delay(message.wrapped.acknowledge()),
                    ifNoAck = Sync[F].unit,
                    ifSend = send =>
                      send
                        .createMessages(messageFactory)
                        .flatMap(toSend =>
                          toSend.messagesAndDestinations.traverse_ {
                            case (message, (name, delay)) =>
                              delay.fold(
                                ifEmpty = context.send(name, message)
                              )(
                                f = d => context.send(name, message, d)
                              )
                          } *> blocker.delay(message.wrapped.acknowledge())
                        )
                  )
              _ <- pool.enqueue1((context, consumer))
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  sealed abstract class AckAction[F[_]] extends Product with Serializable {
    def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: AckAction.Send[F] => F[Unit]): F[Unit]
  }

  object AckAction {

    private[jms4s] case class Ack[F[_]]() extends AckAction[F] {
      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifAck
    }

    // if the client wants to ack groups of messages, it'll pass a sequence of NoAck and then a cumulative Ack
    private[jms4s] case class NoAck[F[_]]() extends AckAction[F] {
      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifNoAck
    }

    case class Send[F[_]](
      createMessages: MessageFactory[F] => F[ToSend[F]]
    ) extends AckAction[F] {

      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
    )

    def ack[F[_]]: AckAction[F] = Ack()

    def noAck[F[_]]: AckAction[F] = NoAck()

    def sendN[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
    ): Send[F] =
      Send[F](mf =>
        messageFactory(mf).map(nel => nel.map { case (message, name) => (message, (name, None)) }).map(ToSend[F])
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
      Send[F](mf =>
        messageFactory(mf).map { case (message, name) => ToSend[F](NonEmptyList.one((message, (name, None)))) }
      )
  }
}
