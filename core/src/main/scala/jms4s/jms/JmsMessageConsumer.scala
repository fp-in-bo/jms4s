package jms4s.jms

import cats.effect.{ Concurrent, ContextShift }
import javax.jms.MessageConsumer
import jms4s.IOOps.interruptable

class JmsMessageConsumer[F[_]: Concurrent: ContextShift] private[jms4s] (
  private[jms4s] val wrapped: MessageConsumer
) {

  val receiveJmsMessage: F[JmsMessage[F]] =
    interruptable(force = true) {
      val message = wrapped.receive()
      new JmsMessage(message)
    }
}
