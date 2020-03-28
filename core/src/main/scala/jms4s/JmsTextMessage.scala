package jms4s

import cats.effect.Sync
import javax.jms.TextMessage

class JmsTextMessage[F[_]: Sync] private[jms4s] (override private[jms4s] val wrapped: TextMessage)
    extends JmsMessage[F](wrapped) {

  def setText(text: String): F[Unit] =
    Sync[F].delay(wrapped.setText(text))

  val getText: F[String] = Sync[F].delay(wrapped.getText)
}
