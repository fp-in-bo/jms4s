package fs2jms

import cats.effect.{ Concurrent, ContextShift }
import fs2jms.IOOps._
import javax.jms.MessageConsumer

class JmsMessageConsumer[F[_]: Concurrent: ContextShift] private[fs2jms] (
  private[fs2jms] val wrapped: MessageConsumer
) {

  val receiveJmsMessage: F[JmsMessage[F]] =
    interruptable(force = true) {
      val message = wrapped.receive()
      new JmsMessage(message)
    }
}
