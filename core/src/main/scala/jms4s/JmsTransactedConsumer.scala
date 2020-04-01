package jms4s

import cats.effect.{ Concurrent, ContextShift }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsTransactedConsumer.JmsConsumerPool.{ JmsResource, Received }
import jms4s.config.DestinationName
import jms4s.jms.{ JmsMessage, JmsMessageConsumer, JmsSession }
import jms4s.model.TransactionResult
import jms4s.model.TransactionResult.Destination

trait JmsTransactedConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[TransactionResult]): F[Unit]
}

object JmsTransactedConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    pool: JmsConsumerPool[F],
    concurrencyLevel: Int
  ): JmsTransactedConsumer[F] =
    (f: JmsMessage[F] => F[TransactionResult]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              received <- pool.receive
              tResult  <- f(received.message)
              _ <- tResult match {
                    case TransactionResult.Commit   => pool.commit(received.resource)
                    case TransactionResult.Rollback => pool.rollback(received.resource)
                    case TransactionResult.Send(destinations) =>
                      destinations.traverse_ {
                        case Destination(name, delay) =>
                          delay.fold(
                            received.resource
                              .producers(name)
                              .publish(received.message)
                          )(
                            d =>
                              received.resource
                                .producers(name)
                                .publish(received.message, d)
                          ) *> pool.commit(received.resource)
                      }
                  }
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  private[jms4s] class JmsConsumerPool[F[_]: Concurrent: ContextShift](pool: Queue[F, JmsResource[F]]) {

    val receive: F[Received[F]] =
      for {
        resource <- pool.dequeue1
        msg      <- resource.consumer.receiveJmsMessage
      } yield Received(msg, resource)

    def commit(resource: JmsResource[F]): F[Unit] =
      for {
        _ <- resource.session.commit
        _ <- pool.enqueue1(resource)
      } yield ()

    def rollback(resource: JmsResource[F]): F[Unit] =
      for {
        _ <- resource.session.rollback
        _ <- pool.enqueue1(resource)
      } yield ()
  }

  object JmsConsumerPool {

    private[jms4s] case class JmsResource[F[_]] private[jms4s] (
      session: JmsSession[F],
      consumer: JmsMessageConsumer[F],
      producers: Map[DestinationName, JmsProducer[F]]
    )

    private[jms4s] case class Received[F[_]] private (message: JmsMessage[F], resource: JmsResource[F])

  }

}
