package jms4s.jms

import cats.effect.{ Concurrent, ContextShift, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSConsumer

class JmsMessageConsumer[F[_]: ContextShift: Concurrent: Logger] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer
) {

  val receiveJmsMessage: F[JmsMessage] =
    for {
      recOpt <- Sync[F].delay(Option(wrapped.receiveNoWait()))
      rec <- recOpt match {
              case Some(message) => Sync[F].pure(new JmsMessage(message))
              case None          => ContextShift[F].shift >> receiveJmsMessage
            }
    } yield rec
}
