package jms4s.jms

import cats.effect.Sync
import jms4s.jms.JmsMessage.JmsTextMessage

class MessageFactory[F[_]: Sync](session: JmsSession[F]) {
  def makeTextMessage(value: String): F[JmsTextMessage[F]] = session.createTextMessage(value)
}

class MessageFactory2[F[_]: Sync](context: JmsContext[F]) extends MessageFactory(null) {
  override def makeTextMessage(value: String): F[JmsTextMessage[F]] = context.createTextMessage(value)
}
