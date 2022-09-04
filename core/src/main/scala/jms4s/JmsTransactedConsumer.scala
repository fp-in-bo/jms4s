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
import cats.effect.Async
import cats.syntax.all._
import jms4s.JmsTransactedConsumer.TransactionAction
import jms4s.config.DestinationName
import jms4s.jms._

import scala.concurrent.duration.FiniteDuration

trait JmsTransactedConsumer[F[_]] {
  def handle(f: (JmsMessage, MessageFactory[F]) => F[TransactionAction[F]]): F[Unit]
}

object JmsTransactedConsumer {

  private[jms4s] def make[F[_]: Async](rawConsumer: MessageConsumer[F]): JmsTransactedConsumer[F] =
    (f: (JmsMessage, MessageFactory[F]) => F[TransactionAction[F]]) =>
      rawConsumer.consume {
        case (received, context, mf) =>
          for {
            txnAction <- f(received, mf)
            _ <- txnAction.fold(
                  ifCommit = context.commit,
                  ifRollback = context.rollback,
                  ifSend = send =>
                    send.messages.messagesAndDestinations.traverse_ {
                      case (message, (name, delay)) =>
                        delay.fold(
                          context.send(name, message)
                        )(d => context.send(name, message, d))
                    } *> context.commit
                )
          } yield ()
      }.compile.drain

  sealed abstract class TransactionAction[F[_]] extends Product with Serializable {
    def fold(ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: TransactionAction.Send[F] => F[Unit]): F[Unit]
  }

  object TransactionAction {

    private[jms4s] case class Commit[F[_]]() extends TransactionAction[F] {

      override def fold(
        ifCommit: => F[Unit],
        ifRollback: => F[Unit],
        ifSend: TransactionAction.Send[F] => F[Unit]
      ): F[Unit] = ifCommit
    }

    private[jms4s] case class Rollback[F[_]]() extends TransactionAction[F] {

      override def fold(
        ifCommit: => F[Unit],
        ifRollback: => F[Unit],
        ifSend: TransactionAction.Send[F] => F[Unit]
      ): F[Unit] = ifRollback
    }

    case class Send[F[_]](messages: ToSend[F]) extends TransactionAction[F] {

      override def fold(
        ifCommit: => F[Unit],
        ifRollback: => F[Unit],
        ifSend: TransactionAction.Send[F] => F[Unit]
      ): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    )

    def commit[F[_]]: TransactionAction[F] = Commit[F]()

    def rollback[F[_]]: TransactionAction[F] = Rollback[F]()

    def sendN[F[_]](
      messages: NonEmptyList[(JmsMessage, DestinationName)]
    ): TransactionAction[F] =
      Send[F](ToSend[F](messages.map { case (message, name) => (message, (name, None)) }))

    def sendNWithDelay[F[_]](
      messages: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    ): TransactionAction[F] =
      Send[F](ToSend[F](messages.map { case (message, (name, delay)) => (message, (name, delay)) }))

    def sendWithDelay[F[_]](
      message: JmsMessage,
      destination: DestinationName,
      duration: Option[FiniteDuration]
    ): TransactionAction[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, duration)))))

    def send[F[_]](
      message: JmsMessage,
      destination: DestinationName
    ): TransactionAction[F] =
      Send[F](ToSend[F](NonEmptyList.one((message, (destination, None)))))
  }
}
