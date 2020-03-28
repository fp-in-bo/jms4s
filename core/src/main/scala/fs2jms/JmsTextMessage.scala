package fs2jms

import cats.effect.Sync
import javax.jms.TextMessage

class JmsTextMessage[F[_]: Sync] private[fs2jms] (override private[fs2jms] val wrapped: TextMessage)
    extends JmsMessage[F](wrapped) {

  def setText(text: String): F[Unit] =
    Sync[F].delay(wrapped.setText(text))

  val getText: F[String] = Sync[F].delay(wrapped.getText)
}
