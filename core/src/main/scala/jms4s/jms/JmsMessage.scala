package jms4s.jms

import cats.Show
import cats.effect.Sync
import cats.implicits._
import javax.jms.{ Destination, Message, TextMessage }
import jms4s.jms.JmsMessage.{ JmsTextMessage, UnsupportedMessage }
import jms4s.jms.MessageOps._

import scala.util.control.NoStackTrace
import scala.util.{ Failure, Success, Try }

class JmsMessage private[jms4s] (private[jms4s] val wrapped: Message) {

  def attemptAsJmsTextMessage: Try[JmsTextMessage] = wrapped match {
    case textMessage: TextMessage => Success(new JmsTextMessage(textMessage))
    case _                        => Failure(UnsupportedMessage(wrapped))
  }

  def attemptAsText: Try[String] = attemptAsJmsTextMessage.flatMap(_.getText)

  def asJmsTextMessageF[F[_]: Sync]: F[JmsTextMessage] = Sync[F].fromTry(attemptAsJmsTextMessage)
  def asTextF[F[_]: Sync]: F[String]                   = Sync[F].fromTry(attemptAsText)

  def setJMSCorrelationId(correlationId: String): Try[Unit] = Try(wrapped.setJMSCorrelationID(correlationId))
  def setJMSReplyTo(destination: JmsDestination): Try[Unit] = Try(wrapped.setJMSReplyTo(destination.wrapped))
  def setJMSType(`type`: String): Try[Unit]                 = Try(wrapped.setJMSType(`type`))

  def setJMSCorrelationIDAsBytes(correlationId: Array[Byte]): Try[Unit] =
    Try(wrapped.setJMSCorrelationIDAsBytes(correlationId))

  val getJMSMessageId: Try[String]                 = Try(wrapped.getJMSMessageID)
  val getJMSTimestamp: Try[Long]                   = Try(wrapped.getJMSTimestamp)
  val getJMSCorrelationId: Try[String]             = Try(wrapped.getJMSCorrelationID)
  val getJMSCorrelationIdAsBytes: Try[Array[Byte]] = Try(wrapped.getJMSCorrelationIDAsBytes)
  val getJMSReplyTo: Try[Destination]              = Try(wrapped.getJMSReplyTo)
  val getJMSDestination: Try[Destination]          = Try(wrapped.getJMSDestination)
  val getJMSDeliveryMode: Try[Int]                 = Try(wrapped.getJMSDeliveryMode)
  val getJMSRedelivered: Try[Boolean]              = Try(wrapped.getJMSRedelivered)
  val getJMSType: Try[String]                      = Try(wrapped.getJMSType)
  val getJMSExpiration: Try[Long]                  = Try(wrapped.getJMSExpiration)
  val getJMSPriority: Try[Int]                     = Try(wrapped.getJMSPriority)
  val getJMSDeliveryTime: Try[Long]                = Try(wrapped.getJMSDeliveryTime)

  def getBooleanProperty(name: String): Try[Boolean] =
    Try(wrapped.getBooleanProperty(name))

  def getByteProperty(name: String): Try[Byte] =
    Try(wrapped.getByteProperty(name))

  def getDoubleProperty(name: String): Try[Double] =
    Try(wrapped.getDoubleProperty(name))

  def getFloatProperty(name: String): Try[Float] =
    Try(wrapped.getFloatProperty(name))

  def getIntProperty(name: String): Try[Int] =
    Try(wrapped.getIntProperty(name))

  def getLongProperty(name: String): Try[Long] =
    Try(wrapped.getLongProperty(name))

  def getShortProperty(name: String): Try[Short] =
    Try(wrapped.getShortProperty(name))

  def getStringProperty(name: String): Try[String] =
    Try(wrapped.getStringProperty(name))

  def setBooleanProperty(name: String, value: Boolean): Try[Unit] = Try(wrapped.setBooleanProperty(name, value))
  def setByteProperty(name: String, value: Byte): Try[Unit]       = Try(wrapped.setByteProperty(name, value))
  def setDoubleProperty(name: String, value: Double): Try[Unit]   = Try(wrapped.setDoubleProperty(name, value))
  def setFloatProperty(name: String, value: Float): Try[Unit]     = Try(wrapped.setFloatProperty(name, value))
  def setIntProperty(name: String, value: Int): Try[Unit]         = Try(wrapped.setIntProperty(name, value))
  def setLongProperty(name: String, value: Long): Try[Unit]       = Try(wrapped.setLongProperty(name, value))
  def setShortProperty(name: String, value: Short): Try[Unit]     = Try(wrapped.setShortProperty(name, value))
  def setStringProperty(name: String, value: String): Try[Unit]   = Try(wrapped.setStringProperty(name, value))

}

object MessageOps {

  implicit val showMessage: Show[Message] = Show.show[Message] { message =>
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
         |${getStringContent.getOrElse(s"Unsupported message type: $message")}
        """.stripMargin
    }.getOrElse("")
  }

  implicit val showJmsMessage: Show[JmsMessage] = Show.show[JmsMessage](_.wrapped.show)
}

object JmsMessage {

  case class UnsupportedMessage(message: Message)
      extends Exception("Unsupported Message: " + message.show)
      with NoStackTrace

  class JmsTextMessage private[jms4s] (override private[jms4s] val wrapped: TextMessage) extends JmsMessage(wrapped) {

    def setText(text: String): Try[Unit] =
      Try(wrapped.setText(text))

    val getText: Try[String] = Try(wrapped.getText)
  }
}
