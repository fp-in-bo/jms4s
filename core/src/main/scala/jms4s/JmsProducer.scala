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

import cats.data.NonEmptyList
import cats.effect.{ Concurrent, ContextShift, Resource }
import cats.syntax.all._
import fs2.concurrent.Queue
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

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
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
              _ <- Resource.eval(pool.enqueue1(c))
            } yield ()
          }
      mf = MessageFactory[F](context)
    } yield new JmsProducer[F] {

      override def sendN(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage, DestinationName)]]
      ): F[Unit] =
        for {
          ctx                      <- pool.dequeue1
          messagesWithDestinations <- f(mf)
          _ <- messagesWithDestinations.traverse_ {
                case (message, destinationName) => ctx.send(destinationName, message)
              }
          _ <- pool.enqueue1(ctx)
        } yield ()

      override def sendNWithDelay(
        f: MessageFactory[F] => F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]]
      ): F[Unit] =
        for {
          ctx                                <- pool.dequeue1
          messagesWithDestinationsAndDelayes <- f(mf)
          _ <- messagesWithDestinationsAndDelayes.traverse_ {
                case (message, (destinatioName, duration)) =>
                  duration.fold(ctx.send(destinatioName, message))(delay => ctx.send(destinatioName, message, delay))
              }
          _ <- pool.enqueue1(ctx)
        } yield ()

      override def sendWithDelay(
        f: MessageFactory[F] => F[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
      ): F[Unit] =
        for {
          ctx                                 <- pool.dequeue1
          (message, (destinationName, delay)) <- f(mf)
          _                                   <- delay.fold(ctx.send(destinationName, message))(delay => ctx.send(destinationName, message, delay))
          _                                   <- pool.enqueue1(ctx)
        } yield ()

      override def send(f: MessageFactory[F] => F[(JmsMessage, DestinationName)]): F[Unit] =
        for {
          ctx                    <- pool.dequeue1
          (message, destination) <- f(mf)
          _                      <- ctx.send(destination, message)
          _                      <- pool.enqueue1(ctx)
        } yield ()
    }
}
