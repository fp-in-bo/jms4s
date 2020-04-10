package jms4s

import javax.jms.Session

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

  sealed abstract class SessionType2(val rawAcknowledgeMode: Int) extends Product with Serializable

  object SessionType2 {

    case object Transacted extends SessionType2(Session.SESSION_TRANSACTED)

    case object ClientAcknowledge extends SessionType2(Session.CLIENT_ACKNOWLEDGE)

    case object AutoAcknowledge extends SessionType2(Session.AUTO_ACKNOWLEDGE)

    case object DupsOkAcknowledge extends SessionType2(Session.DUPS_OK_ACKNOWLEDGE)

  }
}
