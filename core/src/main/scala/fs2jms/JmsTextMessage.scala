package fs2jms

import cats.Show
import cats.effect.Sync
import javax.jms.{ Destination, Message, TextMessage }

import scala.util.{ Failure, Try }

class JmsTextMessage[F[_]: Sync] private[fs2jms] (private[fs2jms] val wrapped: TextMessage) {

  def setText(text: String): F[Unit] =
    Sync[F].delay(wrapped.setText(text))

  def setJMSCorrelationId(correlationId: String): F[Unit] =
    Sync[F].delay(wrapped.setJMSCorrelationID(correlationId))

  def setJMSReplyTo(destination: JmsDestination): F[Unit] =
    Sync[F].delay(wrapped.setJMSReplyTo(destination.wrapped))

  def setJMSType(`type`: String): F[Unit] =
    Sync[F].delay(wrapped.setJMSType(`type`))

  def setJMSCorrelationIDAsBytes(correlationId: Array[Byte]): F[Unit] =
    Sync[F].delay(wrapped.setJMSCorrelationIDAsBytes(correlationId))

  val getText: F[String]                         = Sync[F].delay(wrapped.getText)
  val getJMSMessageId: F[String]                 = Sync[F].delay(wrapped.getJMSMessageID)
  val getJMSTimestamp: F[Long]                   = Sync[F].delay(wrapped.getJMSTimestamp)
  val getJMSCorrelationId: F[String]             = Sync[F].delay(wrapped.getJMSCorrelationID)
  val getJMSCorrelationIdAsBytes: F[Array[Byte]] = Sync[F].delay(wrapped.getJMSCorrelationIDAsBytes)
  val getJMSReplyTo: F[Destination]              = Sync[F].delay(wrapped.getJMSReplyTo)
  val getJMSDestination: F[Destination]          = Sync[F].delay(wrapped.getJMSDestination)
  val getJMSDeliveryMode: F[Int]                 = Sync[F].delay(wrapped.getJMSDeliveryMode)
  val getJMSRedelivered: F[Boolean]              = Sync[F].delay(wrapped.getJMSRedelivered)
  val getJMSType: F[String]                      = Sync[F].delay(wrapped.getJMSType)
  val getJMSExpiration: F[Long]                  = Sync[F].delay(wrapped.getJMSExpiration)
  val getJMSPriority: F[Int]                     = Sync[F].delay(wrapped.getJMSPriority)

  def getBooleanProperty(name: String): F[Boolean] = Sync[F].delay(wrapped.getBooleanProperty(name))
  def getByteProperty(name: String): F[Byte]       = Sync[F].delay(wrapped.getByteProperty(name))
  def getDoubleProperty(name: String): F[Double]   = Sync[F].delay(wrapped.getDoubleProperty(name))
  def getFloatProperty(name: String): F[Float]     = Sync[F].delay(wrapped.getFloatProperty(name))
  def getIntProperty(name: String): F[Int]         = Sync[F].delay(wrapped.getIntProperty(name))
  def getLongProperty(name: String): F[Long]       = Sync[F].delay(wrapped.getLongProperty(name))
  def getShortProperty(name: String): F[Short]     = Sync[F].delay(wrapped.getShortProperty(name))
  def getStringProperty(name: String): F[String]   = Sync[F].delay(wrapped.getStringProperty(name))

}

object MessageOps {
  implicit def showMessage[F[_]]: Show[Message] = Show.show[Message] { message =>
    def getStringContent: Try[String] = message match {
      case message: TextMessage => Try(message.getText)
      case _                    => Failure(new RuntimeException())
    }

    def propertyNames: List[String] = {
      val e   = message.getPropertyNames
      val buf = collection.mutable.Buffer.empty[String]
      while (e.hasMoreElements) {
        val propertyName = e.nextElement.asInstanceOf[String]
        buf += propertyName
      }
      buf.toList
    }

    Try {
      s"""
         |${propertyNames.map(pn => s"$pn       ${message.getObjectProperty(pn)}").mkString("\n")}
         |JMSMessageID        ${message.getJMSMessageID}
         |JMSTimestamp        ${message.getJMSTimestamp}
         |JMSCorrelationID    ${message.getJMSCorrelationID}
         |JMSReplyTo          ${message.getJMSReplyTo}
         |JMSDestination      ${message.getJMSDestination}
         |JMSDeliveryMode     ${message.getJMSDeliveryMode}
         |JMSRedelivered      ${message.getJMSRedelivered}
         |JMSType             ${message.getJMSType}
         |JMSExpiration       ${message.getJMSExpiration}
         |JMSPriority         ${message.getJMSPriority}
         |===============================================================================
         |${getStringContent.getOrElse(s"Unsupported message type: ${message}")}
        """.stripMargin
    }.getOrElse("")
  }
}
