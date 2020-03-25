package fs2jms

import javax.jms.Session

object model {

  sealed abstract class SessionType(val rawTransacted: Boolean, val rawAcknowledgeMode: Int)
      extends Product
      with Serializable

  object SessionType {
    case object Transacted        extends SessionType(true, Session.SESSION_TRANSACTED)
    case object ClientAcknowledge extends SessionType(false, Session.CLIENT_ACKNOWLEDGE)
    case object AutoAcknowledge   extends SessionType(false, Session.AUTO_ACKNOWLEDGE)
    case object DupsOkAcknowledge extends SessionType(false, Session.DUPS_OK_ACKNOWLEDGE)
  }

}
