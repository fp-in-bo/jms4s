/*
 * Copyright (c) 2020 Functional Programming in Bologna
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package jms4s

import cats.data.NonEmptyList
import cats.effect.kernel.MonadCancel
import cats.effect.std.Queue
import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import fs2.Stream
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsAcknowledgerConsumer[F[_]] {
  def handle(f: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]): F[Unit]
}

object JmsAcknowledgerConsumer {

  private[jms4s] def make[F[_]: Async](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    for {
      pool <- Resource.eval(
               Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(SessionType.ClientAcknowledge)
              consumer <- ctx.createJmsConsumer(inputDestinationName, pollingInterval)
              _        <- Resource.eval(pool.offer((ctx, consumer, MessageFactory[F](ctx))))
            } yield ()
          }
    } yield build(new JmsAckConsumerPool(pool), concurrencyLevel)

  private def build[F[_]: Async](
    pool: JmsAckConsumerPool[F],
    concurrencyLevel: Int
  ): JmsAcknowledgerConsumer[F] =
    (f: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]) => {
      Stream
        .emit(Stream.eval(pool.receive(f)))
        .repeat
        .parJoin(concurrencyLevel)
        .compile
        .drain
    }

  private[jms4s] class JmsAckConsumerPool[F[_]: Async](
    private val pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])]
  ) {

    def receive(action: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]): F[Unit] =
      MonadCancel[F].bracket(pool.take) {
        case (context, consumer, mf) =>
          for {
            message <- consumer.receiveJmsMessage
            res     <- action(message, mf)
            _ <- res.fold(
                  ifAck = Sync[F].blocking(message.wrapped.acknowledge()),
                  ifNoAck = Sync[F].unit,
                  ifSend = send =>
                    send.messages.messagesAndDestinations.traverse_ {
                      case (message, (name, delay)) =>
                        delay.fold(ifEmpty = context.send(name, message))(
                          f = d => context.send(name, message, d)
                        )
                    } *> Sync[F].blocking(message.wrapped.acknowledge())
                )
          } yield ()
      }(pool.offer)
  }

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

    case class Send[F[_]](messages: ToSend[F]) extends AckAction[F] {

      override def fold(ifAck: => F[Unit], ifNoAck: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    )

    def ack[F[_]]: AckAction[F] = Ack()

    def noAck[F[_]]: AckAction[F] = NoAck()

    def sendN[F[_]](
      messages: NonEmptyList[(JmsMessage, DestinationName)]
    ): Send[F] =
      Send[F](ToSend[F](messages.map { case (message, name) => (message, (name, None)) }))

    def sendNWithDelay[F[_]](
      messages: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    ): Send[F] = Send[F](ToSend(messages))

    def sendWithDelay[F[_]](
      message: JmsMessage,
      destination: DestinationName,
      duration: Option[FiniteDuration]
    ): Send[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, duration)))))

    def send[F[_]](message: JmsMessage, destination: DestinationName): Send[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, None)))))
  }
}
