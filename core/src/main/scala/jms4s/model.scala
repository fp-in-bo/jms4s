package jms4s

import cats.data.NonEmptyList
import javax.jms.Session
import jms4s.config.{ DestinationName, QueueName }

import scala.concurrent.duration.FiniteDuration

object model {

  sealed abstract class SessionType(val rawTransacted: Boolean, val rawAcknowledgeMode: Int)
      extends Product
      with Serializable

  object SessionType {

    case object Transacted extends SessionType(true, Session.SESSION_TRANSACTED)

    case object ClientAcknowledge extends SessionType(false, Session.CLIENT_ACKNOWLEDGE)

    case object AutoAcknowledge extends SessionType(false, Session.AUTO_ACKNOWLEDGE)

    case object DupsOkAcknowledge extends SessionType(false, Session.DUPS_OK_ACKNOWLEDGE)

  }

  sealed abstract class TransactionResult extends Product with Serializable

  object TransactionResult {

    case object Commit extends TransactionResult

    case object Rollback extends TransactionResult

    object Send {

      def to(queueNames: QueueName*): Send =
        Send(NonEmptyList.fromListUnsafe(queueNames.toList.map(name => Destination(name, None))))

      def withDelayTo(queueNames: (QueueName, FiniteDuration)*): Send = Send(
        NonEmptyList.fromListUnsafe(
          queueNames.toList.map(x => Destination(x._1, Some(x._2)))
        )
      )
    }

    private[jms4s] case class Send(destinations: NonEmptyList[Destination]) extends TransactionResult

    case class Destination(queueName: DestinationName, delay: Option[FiniteDuration])

  }

}
