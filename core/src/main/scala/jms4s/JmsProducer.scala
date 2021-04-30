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
import cats.effect.std.Queue
import cats.effect.{ Async, Resource }
import cats.syntax.all._
import jms4s.config.DestinationName
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

trait JmsProducer[F[_]] {

  def sendN(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage, DestinationName)]]
  ): F[Unit]

  def sendNWithDelay(
    messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]]
  ): F[Unit]

  def sendWithDelay(
    messageFactory: MessageFactory[F] => F[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
  ): F[Unit]

  def send(messageFactory: MessageFactory[F] => F[(JmsMessage, DestinationName)]): F[Unit]

}

object JmsProducer {

  private[jms4s] def make[F[_]: Async](
    context: JmsContext[F],
    concurrencyLevel: Int
  ): Resource[F, JmsProducer[F]] =
    for {
      pool <- Resource.eval(
               Queue.bounded[F, JmsContext[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              c <- context.createContext(SessionType.AutoAcknowledge)
              _ <- Resource.eval(pool.offer(c))
            } yield ()
          }
      mf = MessageFactory[F](context)
    } yield new JmsProducer[F] {

      override def sendN(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage, DestinationName)]]
      ): F[Unit] =
        for {
          ctx                      <- pool.take
          messagesWithDestinations <- f(mf)
          _ <- messagesWithDestinations.traverse_ {
                case (message, destinationName) => ctx.send(destinationName, message)
              }
          _ <- pool.offer(ctx)
        } yield ()

      override def sendNWithDelay(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]]
      ): F[Unit] =
        for {
          ctx                                <- pool.take
          messagesWithDestinationsAndDelayes <- f(mf)
          _ <- messagesWithDestinationsAndDelayes.traverse_ {
                case (message, (destinatioName, duration)) =>
                  duration.fold(ctx.send(destinatioName, message))(delay => ctx.send(destinatioName, message, delay))
              }
          _ <- pool.offer(ctx)
        } yield ()

      override def sendWithDelay(
        f: MessageFactory[F] => F[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
      ): F[Unit] =
        for {
          ctx                                 <- pool.take
          (message, (destinationName, delay)) <- f(mf)
          _                                   <- delay.fold(ctx.send(destinationName, message))(delay => ctx.send(destinationName, message, delay))
          _                                   <- pool.offer(ctx)
        } yield ()

      override def send(f: MessageFactory[F] => F[(JmsMessage, DestinationName)]): F[Unit] =
        for {
          ctx                    <- pool.take
          (message, destination) <- f(mf)
          _                      <- ctx.send(destination, message)
          _                      <- pool.offer(ctx)
        } yield ()
    }
}
