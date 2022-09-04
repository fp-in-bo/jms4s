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

package jms4s.jms

import cats.effect.Resource
import cats.effect.kernel.{ Async, Concurrent }
import cats.effect.std.Queue
import cats.syntax.all._
import fs2._
import jms4s.config.DestinationName
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

private[jms4s] class MessageConsumer[F[_]: Concurrent](
  pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])],
  concurrencyLevel: Int
) {

  def consume[A](f: (JmsMessage, JmsContext[F], MessageFactory[F]) => F[A]): Stream[F, A] =
    Stream
      .emit(Stream.resource(poolResources).evalMap {
        case (context, consumer, mf) =>
          consumer.receiveJmsMessage
            .flatMap(received => f(received, context, mf))
      })
      .repeat
      .parJoin(concurrencyLevel)

  private def poolResources: Resource[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])] =
    Resource.make(pool.take)(pool.offer)
}

object MessageConsumer {

  private[jms4s] def pooled[F[_]: Async](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration,
    sessionType: SessionType
  ): Resource[F, MessageConsumer[F]] =
    for {
      pool <- Resource.eval(
               Queue.bounded[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              ctx      <- context.createContext(sessionType)
              consumer <- ctx.createJmsConsumer(inputDestinationName, pollingInterval)
              _        <- Resource.eval(pool.offer((ctx, consumer, MessageFactory[F](ctx))))
            } yield ()
          }
    } yield new MessageConsumer(pool, concurrencyLevel)

}
