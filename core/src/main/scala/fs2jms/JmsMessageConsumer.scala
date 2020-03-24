package fs2jms

import cats.effect.{ Concurrent, ContextShift }
import fs2jms.IOOps._
import javax.jms.{ Message, MessageConsumer }

class JmsMessageConsumer[F[_]: Concurrent: ContextShift](private[fs2jms] val wrapped: MessageConsumer) {

  val receive: F[Message] =
    interruptable(force = true)(wrapped.receive())
}
