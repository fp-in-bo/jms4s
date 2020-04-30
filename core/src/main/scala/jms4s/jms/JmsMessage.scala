package jms4s.jms

import cats.implicits._
import cats.{ ApplicativeError, Show }
import javax.jms.{ Destination, Message, TextMessage }
import jms4s.jms.JmsMessage.{ JmsTextMessage, UnsupportedMessage }
import jms4s.jms.utils.TryUtils._

import scala.util.control.NoStackTrace
import scala.util.{ Failure, Success, Try }

class JmsMessage private[jms4s] (private[jms4s] val wrapped: Message) {

  def attemptAsJmsTextMessage: Try[JmsTextMessage] = wrapped match {
    case textMessage: TextMessage => Success(new JmsTextMessage(textMessage))
    case _                        => Failure(UnsupportedMessage(wrapped))
  }

  def attemptAsText: Try[String] = attemptAsJmsTextMessage.flatMap(_.getText)

  def asJmsTextMessageF[F[_]](implicit a: ApplicativeError[F, Throwable]): F[JmsTextMessage] =
    ApplicativeError[F, Throwable].fromTry(attemptAsJmsTextMessage)

  def asTextF[F[_]](implicit a: ApplicativeError[F, Throwable]): F[String] =
    ApplicativeError[F, Throwable].fromTry(attemptAsText)

  def setJMSCorrelationId(correlationId: String): Try[Unit] = Try(wrapped.setJMSCorrelationID(correlationId))
  def setJMSReplyTo(destination: JmsDestination): Try[Unit] = Try(wrapped.setJMSReplyTo(destination.wrapped))
  def setJMSType(`type`: String): Try[Unit]                 = Try(wrapped.setJMSType(`type`))

  def setJMSCorrelationIDAsBytes(correlationId: Array[Byte]): Try[Unit] =
    Try(wrapped.setJMSCorrelationIDAsBytes(correlationId))

  val getJMSMessageId: Option[String]     = Try(Option(wrapped.getJMSMessageID)).toOpt
  val getJMSTimestamp: Option[Long]       = Try(Option(wrapped.getJMSTimestamp)).toOpt
  val getJMSCorrelationId: Option[String] = Try(Option(wrapped.getJMSCorrelationID)).toOpt

  val getJMSCorrelationIdAsBytes: Option[Array[Byte]] = Try(Option(wrapped.getJMSCorrelationIDAsBytes)).toOpt
  val getJMSReplyTo: Option[Destination]              = Try(Option(wrapped.getJMSReplyTo)).toOpt
  val getJMSDestination: Option[Destination]          = Try(Option(wrapped.getJMSDestination)).toOpt
  val getJMSDeliveryMode: Option[Int]                 = Try(Option(wrapped.getJMSDeliveryMode)).toOpt
  val getJMSRedelivered: Option[Boolean]              = Try(Option(wrapped.getJMSRedelivered)).toOpt
  val getJMSType: Option[String]                      = Try(Option(wrapped.getJMSType)).toOpt
  val getJMSExpiration: Option[Long]                  = Try(Option(wrapped.getJMSExpiration)).toOpt
  val getJMSPriority: Option[Int]                     = Try(Option(wrapped.getJMSPriority)).toOpt
  val getJMSDeliveryTime: Option[Long]                = Try(Option(wrapped.getJMSDeliveryTime)).toOpt

  def getBooleanProperty(name: String): Option[Boolean] =
    Try(Option(wrapped.getBooleanProperty(name))).toOpt

  def getByteProperty(name: String): Option[Byte] =
    Try(Option(wrapped.getByteProperty(name))).toOpt

  def getDoubleProperty(name: String): Option[Double] =
    Try(Option(wrapped.getDoubleProperty(name))).toOpt

  def getFloatProperty(name: String): Option[Float] =
    Try(Option(wrapped.getFloatProperty(name))).toOpt

  def getIntProperty(name: String): Option[Int] =
    Try(Option(wrapped.getIntProperty(name))).toOpt

  def getLongProperty(name: String): Option[Long] =
    Try(Option(wrapped.getLongProperty(name))).toOpt

  def getShortProperty(name: String): Option[Short] =
    Try(Option(wrapped.getShortProperty(name))).toOpt

  def getStringProperty(name: String): Option[String] =
    Try(Option(wrapped.getStringProperty(name))).toOpt

  def setBooleanProperty(name: String, value: Boolean): Try[Unit] = Try(wrapped.setBooleanProperty(name, value))
  def setByteProperty(name: String, value: Byte): Try[Unit]       = Try(wrapped.setByteProperty(name, value))
  def setDoubleProperty(name: String, value: Double): Try[Unit]   = Try(wrapped.setDoubleProperty(name, value))
  def setFloatProperty(name: String, value: Float): Try[Unit]     = Try(wrapped.setFloatProperty(name, value))
  def setIntProperty(name: String, value: Int): Try[Unit]         = Try(wrapped.setIntProperty(name, value))
  def setLongProperty(name: String, value: Long): Try[Unit]       = Try(wrapped.setLongProperty(name, value))
  def setShortProperty(name: String, value: Short): Try[Unit]     = Try(wrapped.setShortProperty(name, value))
  def setStringProperty(name: String, value: String): Try[Unit]   = Try(wrapped.setStringProperty(name, value))

}

object JmsMessage {

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

  case class UnsupportedMessage(message: Message)
      extends Exception("Unsupported Message: " + message.show)
      with NoStackTrace

  class JmsTextMessage private[jms4s] (override private[jms4s] val wrapped: TextMessage) extends JmsMessage(wrapped) {

    def setText(text: String): Try[Unit] =
      Try(wrapped.setText(text))

    val getText: Try[String] = Try(wrapped.getText)
  }
}
