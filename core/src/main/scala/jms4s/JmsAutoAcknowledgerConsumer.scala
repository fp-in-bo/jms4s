/*
 * Copyright 2021 Alessandro Zoffoli
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Concurrent, Resource, Sync }
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction.Send
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsAutoAcknowledgerConsumer[F[_]] {
  def handle(f: (JmsMessage, MessageFactory[F]) => F[AutoAckAction[F]]): F[Unit]
}

object JmsAutoAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      pool <- Resource.eval(
               Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(SessionType.AutoAcknowledge)
              consumer <- ctx.createJmsConsumer(inputDestinationName)
              _        <- Resource.eval(pool.enqueue1((ctx, consumer, MessageFactory[F](ctx))))
            } yield ()
          }
    } yield build(pool, concurrencyLevel)

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])],
    concurrencyLevel: Int
  ): JmsAutoAcknowledgerConsumer[F] =
    (f: (JmsMessage, MessageFactory[F]) => F[AutoAckAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              (context, consumer, mf) <- pool.dequeue1
              message                 <- consumer.receiveJmsMessage
              res: AutoAckAction[F]   <- f(message, mf)
              _ <- res.fold(
                    ifNoOp = Sync[F].unit,
                    ifSend = (send: Send[F]) =>
                      send.messages.messagesAndDestinations.traverse_ {
                        case (message, (name, delay)) =>
                          delay.fold(
                            ifEmpty = context.send(name, message)
                          )(
                            f = d => context.send(name, message, d)
                          )
                      }
                  )
              _ <- pool.enqueue1((context, consumer, mf))
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

    case class Send[F[_]](messages: ToSend[F]) extends AutoAckAction[F] {

      override def fold(ifNoOp: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    )

    def noOp[F[_]]: NoOp[F] = NoOp[F]()

    def sendN[F[_]: Functor](
      messages: NonEmptyList[(JmsMessage, DestinationName)]
    ): Send[F] =
      Send[F](ToSend[F](messages.map { case (message, name) => (message, (name, None)) }))

    def sendNWithDelay[F[_]: Functor](
      messages: NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
    ): Send[F] =
      Send[F](ToSend[F](messages.map { case (message, (name, delay)) => (message, (name, delay)) }))

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
