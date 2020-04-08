package jms4s.jms

import cats.effect.{ Blocker, ContextShift, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.model.SessionType

class JmsConnection[F[_]: Sync: Logger: ContextShift] private[jms4s] (
  private[jms4s] val wrapped: Connection,
  private[jms4s] val blocker: Blocker
) {

  def createSession(sessionType: SessionType): Resource[F, JmsSession[F]] =
    for {
      session <- Resource.make(
                  Logger[F].info(s"Opening Session for Connection $wrapped.") *>
                    blocker.delay(wrapped.createSession(sessionType.rawTransacted, sessionType.rawAcknowledgeMode))
                )(
                  s =>
                    Logger[F].info(s"Closing Session $s for Connection $wrapped...") *>
                      blocker.delay(s.close()) *>
                      Logger[F].info(s"Closed Session $s for Connection $wrapped.")
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened Session $session for Connection $wrapped."))
    } yield new JmsSession(session, blocker)
}
