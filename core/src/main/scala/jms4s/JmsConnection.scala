package jms4s

import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import jms4s.model.SessionType
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.model.SessionType

class JmsConnection[F[_]: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: Connection,
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
