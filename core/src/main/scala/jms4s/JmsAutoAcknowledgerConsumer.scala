package jms4s

import cats.Functor
import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsAutoAcknowledgerConsumer.JmsAutoAcknowledgerConsumerPool.JmsResource
import jms4s.config.DestinationName
import jms4s.jms.{ JmsConnection, JmsMessage, JmsMessageConsumer, MessageFactory }
import jms4s.model.SessionType.AutoAcknowledge

import scala.concurrent.duration.FiniteDuration

trait JmsAutoAcknowledgerConsumer[F[_]] {
  def handle(f: JmsMessage[F] => F[AutoAckAction[F]]): F[Unit]
}

object JmsAutoAcknowledgerConsumer {

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      input <- Resource.liftF(
                connection.createSession(AutoAcknowledge).use(_.createDestination(inputDestinationName))
              )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(AutoAcknowledge)
              consumer <- session.createConsumer(input)
              _        <- Resource.liftF(pool.enqueue1(JmsResource(consumer, Map.empty, new MessageFactory[F](session))))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationNames: NonEmptyList[DestinationName],
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(AutoAcknowledge)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestinations <- Resource.liftF(
                             outputDestinationNames
                               .traverse(
                                 outputDestinationName =>
                                   connection
                                     .createSession(AutoAcknowledge)
                                     .use(_.createDestination(outputDestinationName))
                                     .map(jmsDestination => (outputDestinationName, jmsDestination))
                               )
                           )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session  <- connection.createSession(AutoAcknowledge)
              consumer <- session.createConsumer(inputDestination)
              producers <- outputDestinations.traverse {
                            case (outputDestinationName, outputDestination) =>
                              session
                                .createProducer(outputDestination)
                                .map(jmsProducer => (outputDestinationName, new JmsProducer(jmsProducer)))
                          }.map(_.toNem)
              _ <- Resource.liftF(
                    pool.enqueue1(JmsResource(consumer, producers.toSortedMap, new MessageFactory[F](session)))
                  )
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private[jms4s] def make[F[_]: ContextShift: Concurrent](
    connection: JmsConnection[F],
    inputDestinationName: DestinationName,
    outputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    for {
      inputDestination <- Resource.liftF(
                           connection
                             .createSession(AutoAcknowledge)
                             .use(_.createDestination(inputDestinationName))
                         )
      outputDestination <- Resource.liftF(
                            connection
                              .createSession(AutoAcknowledge)
                              .use(_.createDestination(outputDestinationName))
                          )
      pool <- Resource.liftF(Queue.bounded[F, JmsResource[F]](concurrencyLevel))
      _ <- (0 until concurrencyLevel).toList.traverse_ { _ =>
            for {
              session     <- connection.createSession(AutoAcknowledge)
              consumer    <- session.createConsumer(inputDestination)
              jmsProducer <- session.createProducer(outputDestination)
              producer    = Map(outputDestinationName -> new JmsProducer(jmsProducer))
              _           <- Resource.liftF(pool.enqueue1(JmsResource(consumer, producer, new MessageFactory[F](session))))
            } yield ()
          }
    } yield build(pool, concurrencyLevel, connection.blocker)

  private def build[F[_]: ContextShift: Concurrent](
    pool: Queue[F, JmsResource[F]],
    concurrencyLevel: Int,
    blocker: Blocker
  ): JmsAutoAcknowledgerConsumer[F] =
    (f: JmsMessage[F] => F[AutoAckAction[F]]) =>
      Stream
        .emits(0 until concurrencyLevel)
        .as(
          Stream.eval(
            for {
              resource <- pool.dequeue1
              message  <- resource.consumer.receiveJmsMessage
              res      <- f(message)
              _ <- res.fold(
                    ifNoOp = Sync[F].unit,
                    ifSend = send =>
                      blocker.blockOn(
                        send
                          .createMessages(resource.messageFactory)
                          .flatMap(
                            toSend =>
                              toSend.messagesAndDestinations.traverse_ {
                                case (message, (name, delay)) =>
                                  delay.fold(
                                    ifEmpty = resource
                                      .producers(name)
                                      .publish(message)
                                  )(
                                    f = d =>
                                      resource
                                        .producers(name)
                                        .publish(message, d)
                                  )
                              } *> Sync[F].delay(message.wrapped.acknowledge())
                          )
                      )
                  )
              _ <- pool.enqueue1(resource)
            } yield ()
          )
        )
        .parJoin(concurrencyLevel)
        .repeat
        .compile
        .drain

  object JmsAutoAcknowledgerConsumerPool {
    private[jms4s] case class JmsResource[F[_]] private[jms4s] (
      consumer: JmsMessageConsumer[F],
      producers: Map[DestinationName, JmsProducer[F]],
      messageFactory: MessageFactory[F]
    )
  }

  sealed abstract class AutoAckAction[F[_]] extends Product with Serializable {
    def fold(ifNoOp: => F[Unit], ifSend: AutoAckAction.Send[F] => F[Unit]): F[Unit]
  }

  object AutoAckAction {
    private[jms4s] case class NoOp[F[_]]() extends AutoAckAction[F] {
      override def fold(ifNoOp: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] = ifNoOp
    }

    case class Send[F[_]](
      createMessages: MessageFactory[F] => F[ToSend[F]]
    ) extends AutoAckAction[F] {
      override def fold(ifNoOp: => F[Unit], ifSend: Send[F] => F[Unit]): F[Unit] =
        ifSend(this)
    }

    private[jms4s] case class ToSend[F[_]](
      messagesAndDestinations: NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
    )

    def noOp[F[_]]: NoOp[F] = NoOp[F]()

    def sendN[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
    ): Send[F] =
      Send[F](
        mf => messageFactory(mf).map(nel => nel.map { case (message, name) => (message, (name, None)) }).map(ToSend[F])
      )

    def sendNWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(ToSend[F]))

    def sendWithDelay[F[_]: Functor](
      messageFactory: MessageFactory[F] => F[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
    ): Send[F] =
      Send[F](mf => messageFactory(mf).map(x => ToSend[F](NonEmptyList.one(x))))

    def send[F[_]: Functor](messageFactory: MessageFactory[F] => F[(JmsMessage[F], DestinationName)]): Send[F] =
      Send[F](
        mf => messageFactory(mf).map { case (message, name) => ToSend[F](NonEmptyList.one((message, (name, None)))) }
      )
  }
}
