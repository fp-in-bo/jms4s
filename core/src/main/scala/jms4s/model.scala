package jms4s

import javax.jms.Session

object model {

  sealed abstract class SessionType(val rawAcknowledgeMode: Int) extends Product with Serializable

  object SessionType {

    case object Transacted extends SessionType(Session.SESSION_TRANSACTED)

    case object ClientAcknowledge extends SessionType(Session.CLIENT_ACKNOWLEDGE)

    case object AutoAcknowledge extends SessionType(Session.AUTO_ACKNOWLEDGE)

    case object DupsOkAcknowledge extends SessionType(Session.DUPS_OK_ACKNOWLEDGE)

  }
}
