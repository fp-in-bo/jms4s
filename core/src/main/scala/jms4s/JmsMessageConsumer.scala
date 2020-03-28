package jms4s

import cats.effect.{ Concurrent, ContextShift }
import IOOps._
import javax.jms.MessageConsumer

class JmsMessageConsumer[F[_]: Concurrent: ContextShift] private[jms4s] (
  private[jms4s] val wrapped: MessageConsumer
) {

  val receiveJmsMessage: F[JmsMessage[F]] =
    interruptable(force = true) {
      val message = wrapped.receive()
      new JmsMessage(message)
    }
}
