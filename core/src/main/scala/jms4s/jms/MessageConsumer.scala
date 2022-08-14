package jms4s.jms

import cats.effect.Resource
import cats.effect.kernel.{ Async, Concurrent }
import cats.effect.std.Queue
import cats.syntax.all._
import fs2._
import jms4s.config.DestinationName
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

private[jms4s] class MessageConsumer[F[_]: Concurrent, A](
  pool: Queue[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])],
  concurrencyLevel: Int
) {

  def consume(f: (JmsMessage, JmsContext[F], MessageFactory[F]) => F[A]): F[Unit] =
    Stream
      .emit(Stream.resource(take).evalMap {
        case (context, consumer, mf) =>
          for {
            received <- consumer.receiveJmsMessage
            _        <- f(received, context, mf)
          } yield ()
      })
      .repeat
      .parJoin(concurrencyLevel)
      .compile
      .drain

  private def take: Resource[F, (JmsContext[F], JmsMessageConsumer[F], MessageFactory[F])] =
    Resource.make(pool.take)(pool.offer)
}

object MessageConsumer {

  private[jms4s] def pooled[F[_]: Async, A](
    context: JmsContext[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration,
    sessionType: SessionType
  ): Resource[F, MessageConsumer[F, Unit]] =
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
