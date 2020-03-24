package fs2jms

import cats.effect.{ Resource, Sync }
import javax.jms.{ MessageConsumer, MessageProducer, QueueConnection, QueueSession }

object model {

  sealed trait SessionType
  // local transaction which may subsequently be committed or rolled back by calling the session's commit or rollback methods.
  case object Transacted                           extends SessionType
  case class Acknowledged(`type`: AcknowledgeType) extends SessionType

  sealed trait AcknowledgeType
  case object ClientAcknowledge extends AcknowledgeType
  case object AutoAcknowledge   extends AcknowledgeType
  case object DupsOkAcknowledge extends AcknowledgeType

  class JmsQueueConnection[F[_]: Sync] private (private val value: QueueConnection) {

    def createQueueSession(`type`: SessionType): Resource[F, JmsQueueSession] =
      Resource.fromAutoCloseable(Sync[F].delay(JmsQueueSession(value.createQueueSession(transacted, acknowledgeMode))))
  }

  case class JmsQueueSession(value: QueueSession) {}

  case class JmsMessageConsumer(value: MessageConsumer)

  case class JmsMessageProducer(value: MessageProducer)

}
