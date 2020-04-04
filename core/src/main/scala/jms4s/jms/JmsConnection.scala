package jms4s.jms

import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.model.SessionType

class JmsConnection[F[_]: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: Connection,
  private[jms4s] val blocker: Blocker
) {

  def createSession(sessionType: SessionType): Resource[F, JmsSession[F]] =
    for {
      session <- Resource.make(
                  Logger[F].info(s"Opening QueueSession for $wrapped.") *>
                    Sync[F].delay(wrapped.createSession(sessionType.rawTransacted, sessionType.rawAcknowledgeMode))
                )(x => Sync[F].delay(x.close()))
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueSession $session for $wrapped."))
    } yield new JmsSession(session, blocker)
}
