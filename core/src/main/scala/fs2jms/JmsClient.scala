package fs2jms

import cats.effect.concurrent.Ref
import cats.effect.{ Concurrent, ContextShift, Resource }
import cats.implicits._
import fs2.Stream
import fs2jms.JmsMessageConsumer.UnsupportedMessage
import fs2jms.JmsPool.Received.{ ReceivedTextMessage, ReceivedUnsupportedMessage }
import fs2jms.JmsPool.{ JmsResource, Received }
import fs2jms.config.QueueName
import fs2jms.model.{ SessionType, TransactionResult }

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createTransactedQueueConsumer(
    connection: JmsQueueConnection[F],
    queueName: QueueName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedQueueConsumer[F]] =
    for {
      queue <- Resource.liftF(connection.createQueueSession(SessionType.Transacted).use(_.createQueue(queueName)))
      resources <- (0 until concurrencyLevel).toList.traverse[Resource[F, *], JmsResource[F]] { _ =>
                    for {
                      session  <- connection.createQueueSession(SessionType.Transacted)
                      consumer <- session.createConsumer(queue)
                    } yield JmsResource(session, consumer)
                  }
      pool <- Resource.liftF(Ref.of(resources))
    } yield new JmsTransactedQueueConsumer(new JmsPool(pool), concurrencyLevel)
}

class JmsTransactedQueueConsumer[F[_]: Concurrent: ContextShift] private[fs2jms] (
  private val pool: JmsPool[F],
  private val concurrencyLevel: Int
) {

  def handle(f: Received[F] => F[TransactionResult]): F[Unit] =
    Stream
      .emits(0 until concurrencyLevel)
      .as(
        Stream.eval(
          for {
            received <- pool.receive
            tResult  <- f(received)
            _ <- tResult match {
                  case TransactionResult.Commit   => pool.commit(received.resource)
                  case TransactionResult.Rollback => pool.rollback(received.resource)
                }
          } yield ()
        )
      )
      .parJoin(concurrencyLevel)
      .repeat
      .compile
      .drain
}

class JmsPool[F[_]: Concurrent: ContextShift] private[fs2jms] (
  private val pool: Ref[F, List[JmsResource[F]]]
) {

  val receive: F[Received[F]] =
    for {
      resource <- pool.modify(resources => (resources.tail, resources.head))
      received <- resource.consumer.receiveTextMessage.map {
                   case Left(um)  => ReceivedUnsupportedMessage(um, resource)
                   case Right(tm) => ReceivedTextMessage(tm, resource)
                 }
    } yield received

  def commit(resource: JmsResource[F]): F[Unit] =
    for {
      _ <- resource.session.commit
      _ <- pool.modify(ss => (ss :+ resource, ()))
    } yield ()

  def rollback(resource: JmsResource[F]): F[Unit] =
    for {
      _ <- resource.session.rollback
      _ <- pool.modify(ss => (ss :+ resource, ()))
    } yield ()
}

object JmsPool {
  case class JmsResource[F[_]] private[fs2jms] (
    session: JmsQueueSession[F],
    consumer: JmsMessageConsumer[F]
  )

  sealed abstract class Received[F[_]] extends Product with Serializable {
    private[fs2jms] val resource: JmsResource[F]
  }

  object Received {
    case class ReceivedTextMessage[F[_]] private (message: JmsTextMessage[F], resource: JmsResource[F])
        extends Received[F]
    case class ReceivedUnsupportedMessage[F[_]] private (message: UnsupportedMessage, resource: JmsResource[F])
        extends Received[F]
  }

}
