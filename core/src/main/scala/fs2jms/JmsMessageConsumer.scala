package fs2jms

import cats.Show
import cats.effect.{ Concurrent, ContextShift }
import fs2jms.IOOps._
import javax.jms.{ Message, MessageConsumer, TextMessage }
import cats.implicits._
import MessageOps._
import fs2jms.JmsMessageConsumer.UnsupportedMessage

import scala.util.control.NoStackTrace

class JmsMessageConsumer[F[_]: Concurrent: ContextShift] private[fs2jms] (
  private[fs2jms] val wrapped: MessageConsumer
) {

  val receiveTextMessage: F[Either[UnsupportedMessage, JmsTextMessage[F]]] =
    interruptable(force = true) {
      val message = wrapped.receive()
      message match {
        case t: TextMessage => new JmsTextMessage(t).asRight
        case x              => UnsupportedMessage(x).asLeft
      }
    }
}

object JmsMessageConsumer {
  case class UnsupportedMessage(message: Message)
      extends Exception("Unsupported Message: " + Show[Message].show(message))
      with NoStackTrace
}
