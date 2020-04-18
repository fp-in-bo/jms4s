package jms4s.jms

import cats.effect.{ Concurrent, ContextShift }
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSConsumer
import jms4s.IOOps.interruptable

class JmsMessageConsumer[F[_]: ContextShift: Concurrent: Logger] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer
) {

  val receiveJmsMessage: F[JmsMessage] =
    interruptable(force = true) {
      val message = wrapped.receive()
      new JmsMessage(message)
    }
}
