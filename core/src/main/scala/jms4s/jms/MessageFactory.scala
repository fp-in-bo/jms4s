package jms4s.jms

import cats.effect.Sync
import jms4s.jms.JmsMessage.JmsTextMessage

class MessageFactory[F[_]: Sync](context: JmsContext[F]) {
  def makeTextMessage(value: String): F[JmsTextMessage[F]] = context.createTextMessage(value)
}

object MessageFactory {
  def apply[F[_]: Sync](context: JmsContext[F]): MessageFactory[F] = new MessageFactory(context)
}
