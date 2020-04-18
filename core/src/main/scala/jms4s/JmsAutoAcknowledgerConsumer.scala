package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsAutoAcknowledgerConsumer[F[_]] {
  def handle(f: JmsMessage => F[AutoAckAction[F]]): F[Unit]
}

object JmsAutoAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      pool <- Resource.liftF(Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F])](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(SessionType.AutoAcknowledge)
              consumer <- ctx.createJmsConsumer(inputDestinationName)
              _        <- Resource.liftF(pool.enqueue1((ctx, consumer)))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, MessageFactory[F](context))

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F])],
    concurrencyLevel: Int,
    messageFactory: MessageFactory[F]
  ): JmsAutoAcknowledgerConsumer[F] =
    (f: JmsMessage => F[AutoAckAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              (context, consumer) <- pool.dequeue1
              message             <- consumer.receiveJmsMessage
              res                 <- f(message)
              _ <- res.fold(
                    ifNoOp = Sync[F].unit,
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
                          }
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

  sealed abstract class AutoAckAction[F[_]] extends Product with Serializable {
    def fold(ifNoOp: => F[Unit], ifSend: AutoAckAction.Send[F] => F[Unit]): F[Unit]
  }

  object AutoAckAction {

    private[jms4s] case class NoOp[F[_]]() extends AutoAckAction[F] {
      override def fold(ifNoOp: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifNoOp
    }

    case class Send[F[_]](
      createMessages: MessageFactory[F] => F[ToSend[F]]
    ) extends AutoAckAction[F] {

      override def fold(ifNoOp: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    )

    def noOp[F[_]]: NoOp[F] = NoOp[F]()

    def sendN[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage, DestinationName)]]
    ): Send[F] =
      Send[F](mf =>
        messageFactory(mf).map(nel => nel.map { case (message, name) => (message, (name, None)) }).map(ToSend[F])
      )

    def sendNWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(ToSend[F]))

    def sendWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(x => ToSend[F](NonEmptyList.one(x))))

    def send[F[_]: Functor](messageFactory: MessageFactory[F] => F[(JmsMessage, DestinationName)]): Send[F] =
      Send[F](mf =>
        messageFactory(mf).map { case (message, name) => ToSend[F](NonEmptyList.one((message, (name, None)))) }
      )
  }
}
