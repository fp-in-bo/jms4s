package jms4s.jms

import cats.effect.{ Blocker, ContextShift, Sync }
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import javax.jms.JMSConsumer

class JmsMessageConsumer[F[_]: ContextShift: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer,
  private[jms4s] val blocker: Blocker
) {

  val receiveJmsMessage: F[JmsMessage] =
    for {
      recOpt <- blocker.delay(Option(wrapped.receiveNoWait()))
      rec <- recOpt match {
              case Some(message) => Sync[F].pure(new JmsMessage(message))
              case None          => ContextShift[F].shift >> receiveJmsMessage
            }
    } yield rec
}
