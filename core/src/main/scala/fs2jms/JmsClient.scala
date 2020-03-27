package fs2jms

import cats.data.{ NonEmptyList, NonEmptyMap }
import cats.effect.concurrent.Ref
import cats.effect.{ Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2jms.JmsMessageConsumer.UnsupportedMessage
import fs2jms.JmsConsumerPool.Received.{ ReceivedTextMessage, ReceivedUnsupportedMessage }
import fs2jms.JmsConsumerPool.{ JmsResource, Received }
import fs2jms.config.QueueName
import fs2jms.model.{ SessionType, TransactionResult }

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

class JmsClient[F[_]: ContextShift: Concurrent] {

  def createQueueTransactedConsumer(
    connection: JmsQueueConnection[F],
    queueName: QueueName,
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F, Unit]] =
    for {
      queue <- Resource.liftF(connection.createQueueSession(SessionType.Transacted).use(_.createQueue(queueName)))
      resources <- (0 until concurrencyLevel).toList.traverse[Resource[F, *], JmsResource[F, Unit]] { _ =>
                    for {
                      session  <- connection.createQueueSession(SessionType.Transacted)
                      consumer <- session.createConsumer(queue)
                    } yield JmsResource(session, consumer, ())
                  }
      pool <- Resource.liftF(Ref.of(resources))
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)

  def createQueueTransactedConsumerToProducers(
    connection: JmsQueueConnection[F],
    inputQueueName: QueueName,
    outputQueueNames: NonEmptyList[QueueName],
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F, NonEmptyMap[QueueName, JmsQueueProducer[F]]]] =
    for {
      inputQueue <- Resource.liftF(
                     connection.createQueueSession(SessionType.Transacted).use(_.createQueue(inputQueueName))
                   )
      outputQueues <- Resource.liftF(
                       outputQueueNames.traverse(
                         outputQueueName =>
                           connection
                             .createQueueSession(SessionType.Transacted)
                             .use(_.createQueue(outputQueueName))
                             .map(jmsQueue => (outputQueueName, jmsQueue))
                       )
                     )
      resources <- (0 until concurrencyLevel).toList
                    .traverse[Resource[F, *], JmsResource[F, NonEmptyMap[QueueName, JmsQueueProducer[F]]]] { _ =>
                      for {
                        session  <- connection.createQueueSession(SessionType.Transacted)
                        consumer <- session.createConsumer(inputQueue)
                        producers <- outputQueues.traverse {
                                      case (outputQueueName, outputQueue) =>
                                        session
                                          .createProducer(outputQueue)
                                          .map(jmsProducer => (outputQueueName, new JmsQueueProducer(jmsProducer)))
                                    }.map(_.toNem)
                      } yield JmsResource(session, consumer, producers)
                    }
      pool <- Resource.liftF(Ref.of(resources))
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)

  // TODO evaluate if this can be rewritten in terms of `createQueueTransactedConsumerToProducers`
  // it's pretty much the same, but here it does not make any sense to have a NonEmptyMap[QueueName, JmsQueueProducer[F]]
  // since the producer is only one!
  def createQueueTransactedConsumerToProducer(
    connection: JmsQueueConnection[F],
    inputQueueName: QueueName,
    outputQueueName: QueueName,
    concurrencyLevel: Int
  ): Resource[F, JmsQueueTransactedConsumer[F, JmsQueueProducer[F]]] =
    for {
      inputQueue <- Resource.liftF(
                     connection.createQueueSession(SessionType.Transacted).use(_.createQueue(inputQueueName))
                   )
      outputQueue <- Resource.liftF(
                      connection.createQueueSession(SessionType.Transacted).use(_.createQueue(outputQueueName))
                    )
      resources <- (0 until concurrencyLevel).toList.traverse[Resource[F, *], JmsResource[F, JmsQueueProducer[F]]] {
                    _ =>
                      for {
                        session     <- connection.createQueueSession(SessionType.Transacted)
                        consumer    <- session.createConsumer(inputQueue)
                        jmsProducer <- session.createProducer(outputQueue)
                        producer    = new JmsQueueProducer(jmsProducer)
                      } yield JmsResource(session, consumer, producer)
                  }
      pool <- Resource.liftF(Ref.of(resources))
    } yield new JmsQueueTransactedConsumer(new JmsConsumerPool(pool), concurrencyLevel)
}

class JmsQueueTransactedConsumer[F[_]: Concurrent: ContextShift, R] private[fs2jms] (
  private val pool: JmsConsumerPool[F, R],
  private val concurrencyLevel: Int
) {

  def handle(f: Received[F, R] => F[TransactionResult]): F[Unit] =
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

class JmsQueueProducer[F[_]: Sync: ContextShift] private[fs2jms] (private[fs2jms] val producer: JmsMessageProducer[F]) {

  def publish(message: JmsTextMessage[F]): F[Unit] =
    producer.send(message)

  def publish(message: JmsTextMessage[F], delay: FiniteDuration): F[Unit] =
    producer.setDeliveryDelay(delay) >> producer.send(message) >> producer.setDeliveryDelay(0.millis)

}

class JmsConsumerPool[F[_]: Concurrent: ContextShift, R] private[fs2jms] (
  private val pool: Ref[F, List[JmsResource[F, R]]]
) {

  val receive: F[Received[F, R]] =
    for {
      resource <- pool.modify(resources => (resources.tail, resources.head))
      received <- resource.consumer.receiveTextMessage.map {
                   case Left(um)  => ReceivedUnsupportedMessage(um, resource)
                   case Right(tm) => ReceivedTextMessage(tm, resource)
                 }
    } yield received

  def commit(resource: JmsResource[F, R]): F[Unit] =
    for {
      _ <- resource.session.commit
      _ <- pool.modify(ss => (ss :+ resource, ()))
    } yield ()

  def rollback(resource: JmsResource[F, R]): F[Unit] =
    for {
      _ <- resource.session.rollback
      _ <- pool.modify(ss => (ss :+ resource, ()))
    } yield ()
}

object JmsConsumerPool {
  case class JmsResource[F[_], R] private[fs2jms] (
    session: JmsQueueSession[F],
    consumer: JmsMessageConsumer[F],
    producing: R
  )

  sealed abstract class Received[F[_], R] extends Product with Serializable {
    private[fs2jms] val resource: JmsResource[F, R]
  }

  object Received {
    case class ReceivedTextMessage[F[_], R] private (message: JmsTextMessage[F], resource: JmsResource[F, R])
        extends Received[F, R]
    case class ReceivedUnsupportedMessage[F[_], R] private (message: UnsupportedMessage, resource: JmsResource[F, R])
        extends Received[F, R]
  }

}
