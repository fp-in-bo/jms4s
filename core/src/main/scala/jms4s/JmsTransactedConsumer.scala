package jms4s

import cats.data.NonEmptyList
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsTransactedConsumer.JmsTransactedConsumerPool.{JmsResource, Received}
import jms4s.JmsTransactedConsumer.{TransactionAction, TransactionActionSyntax}
import jms4s.config.DestinationName
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.{JmsConnection, JmsMessage, JmsMessageConsumer, JmsSession}
import jms4s.model.SessionType
import jms4s.model.SessionType.Transacted

import scala.concurrent.duration.FiniteDuration

trait JmsTransactedConsumer[F[_]] extends TransactionActionSyntax[F]{
  def handle(f: JmsMessage[F] => F[TransactionAction]): F[Unit]
}

object JmsTransactedConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      input <- Resource.liftF(connection.createSession(Transacted).use(_.createDestination(inputDestinationName)))
      pool  <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(input)
              _ <- Resource.liftF(
                    pool.enqueue1(JmsResource(session, consumer, Map.empty, new MessageFactory[F](session)))
                  )
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestinations <- Resource.liftF(
                             outputDestinationNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(SessionType.Transacted)
                                     .use(_.createDestination(outputDestinationName))
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(
               Queue.bounded[F, JmsResource[F]](concurrencyLevel)
             )
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(SessionType.Transacted)
              consumer <- session.createConsumer(inputDestination)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(
                    pool.enqueue1(JmsResource(session, consumer, producers.toSortedMap, new MessageFactory[F](session)))
                  )
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(SessionType.Transacted)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(SessionType.Transacted)
                              .use(_.createDestination(outputDestinationName))
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(SessionType.Transacted)
              consumer    <- session.createConsumer(inputDestination)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _ <- Resource.liftF(
                    pool.enqueue1(JmsResource(session, consumer, producer, new MessageFactory[F](session)))
                  )
            } yield ()
          }
    } yield build(new JmsTransactedConsumerPool(pool), concurrencyLevel)

  private def build[F[_]: ContextShift: Concurrent](
    pool: JmsTransactedConsumerPool[F],
    concurrencyLevel: Int
  ): JmsTransactedConsumer[F] =
    new JmsTransactedConsumer[F] {
      override def handle(f: JmsMessage[F] => F[TransactionAction]): F[Unit] =
        Stream
          .emits(0 until concurrencyLevel)
          .as(
            Stream.eval(
              fo = for {
                received                   <- pool.receive
                tResult <- f(received.message)
                _ <- tResult match {
                      case Commit   => pool.commit(received.resource)
                      case Rollback => pool.rollback(received.resource)
                      case Send(createMessages) => {
                        createMessages(received.resource.messageFactory)
                          .flatMap(
                            dest =>
                              dest.traverse_ {
                                case (message, (name, delay)) =>
                                  delay.fold(
                                    received.resource
                                      .producers(name)
                                      .publish(message)
                                  )(
                                    d =>
                                      received.resource
                                        .producers(name)
                                        .publish(message, d)
                                  ) *> pool.commit(received.resource)
                              }
                          )
                      }
                  //    case _ => pool.commit(received.resource)
                    }
              } yield ()
            )
          )
          .parJoin(concurrencyLevel)
          .repeat
          .compile
          .drain
    }

  private[jms4s] class JmsTransactedConsumerPool[F[_]: Concurrent: ContextShift](pool: Queue[F, JmsResource[F]]) {

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

  object JmsTransactedConsumerPool {

    private[jms4s] case class JmsResource[F[_]] private[jms4s] (
      session: JmsSession[F],
      consumer: JmsMessageConsumer[F],
      producers: Map[DestinationName, JmsProducer[F]],
      messageFactory: MessageFactory[F]
    )

    private[jms4s] case class Received[F[_]] private (message: JmsMessage[F], resource: JmsResource[F])

  }

  sealed abstract class TransactionAction extends Product with Serializable {
    def fold[F[_]](ifCommit: => F[Unit], ifRollback: => F[Unit], ifSend: Send => F[Unit]):F[Unit]
  }

  object TransactionActionSyntax {

    private[jms4s] case object Commit extends TransactionAction

    private[jms4s] case object Rollback extends TransactionAction

    private[jms4s] case class Send(
      createMessages: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
    ) extends TransactionAction

    val commit: TransactionAction   = Commit
    val rollback: TransactionAction = Rollback

    def sendToAndCommit(
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
    ): Send =  {


      Send(messageFactory)
    }
  }

  class MessageFactory[F[_]: Sync](session: JmsSession[F]) {
    def makeTextMessage(value: String): F[JmsTextMessage[F]] = session.createTextMessage(value)
  }
}
