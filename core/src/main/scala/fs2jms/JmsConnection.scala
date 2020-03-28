package fs2jms

import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import fs2jms.model.SessionType
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection

class JmsConnection[F[_]: Sync: Logger] private[fs2jms] (
  private[fs2jms] val wrapped: Connection,
  private val blocker: Blocker
) {

  def createSession(sessionType: SessionType): Resource[F, JmsSession[F]] =
    for {
      session <- Resource.fromAutoCloseable(
                  Logger[F].info(s"Opening QueueSession for $wrapped.") *>
                    Sync[F].delay(wrapped.createSession(sessionType.rawTransacted, sessionType.rawAcknowledgeMode))
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueSession $session for $wrapped."))
    } yield new JmsSession(session, blocker)
}
