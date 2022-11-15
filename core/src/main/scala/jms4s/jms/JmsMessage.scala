/*
 * Copyright (c) 2020 Functional Programming in Bologna
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package jms4s.jms

import cats.syntax.all._
import cats.{ ApplicativeError, Show }
import jms4s.jms.JmsMessage.{ JmsTextMessage, UnsupportedMessage }
import jms4s.jms.utils.TryUtils._

import javax.jms.{ Destination, Message, TextMessage }
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

  /**
   * settings properties works only on newly created message, if invoked on an already existing message they will fail.
   * Clone the message first and then modify it.
   */
  def setJMSCorrelationId(correlationId: String): Try[Unit]     = Try(wrapped.setJMSCorrelationID(correlationId))
  def setJMSReplyTo(destination: JmsDestination): Try[Unit]     = Try(wrapped.setJMSReplyTo(destination.wrapped))
  def setJMSType(`type`: String): Try[Unit]                     = Try(wrapped.setJMSType(`type`))
  def setJMSMessageID(messageID: String): Try[Unit]             = Try(wrapped.setJMSMessageID(messageID))
  def setJMSTimestamp(timestamp: Long): Try[Unit]               = Try(wrapped.setJMSTimestamp(timestamp))
  def setJMSDestination(destination: JmsDestination): Try[Unit] = Try(wrapped.setJMSDestination(destination.wrapped))
  def setJMSDeliveryMode(deliveryMode: Int): Try[Unit]          = Try(wrapped.setJMSDeliveryMode(deliveryMode))
  def setJMSRedelivered(redelivered: Boolean): Try[Unit]        = Try(wrapped.setJMSRedelivered(redelivered))
  def setJMSExpiration(expiration: Long): Try[Unit]             = Try(wrapped.setJMSExpiration(expiration))
  def setJMSPriority(priority: Int): Try[Unit]                  = Try(wrapped.setJMSPriority(priority))

  def setJMSCorrelationIDAsBytes(correlationId: Array[Byte]): Try[Unit] =
    Try(wrapped.setJMSCorrelationIDAsBytes(correlationId))

  def getJMSMessageId: Option[String]                 = Try(Option(wrapped.getJMSMessageID)).toOpt
  def getJMSTimestamp: Option[Long]                   = Try(Option(wrapped.getJMSTimestamp)).toOpt
  def getJMSCorrelationId: Option[String]             = Try(Option(wrapped.getJMSCorrelationID)).toOpt
  def getJMSCorrelationIdAsBytes: Option[Array[Byte]] = Try(Option(wrapped.getJMSCorrelationIDAsBytes)).toOpt
  def getJMSReplyTo: Option[Destination]              = Try(Option(wrapped.getJMSReplyTo)).toOpt
  def getJMSDestination: Option[Destination]          = Try(Option(wrapped.getJMSDestination)).toOpt
  def getJMSDeliveryMode: Option[Int]                 = Try(Option(wrapped.getJMSDeliveryMode)).toOpt
  def getJMSRedelivered: Option[Boolean]              = Try(Option(wrapped.getJMSRedelivered)).toOpt
  def getJMSType: Option[String]                      = Try(Option(wrapped.getJMSType)).toOpt
  def getJMSExpiration: Option[Long]                  = Try(Option(wrapped.getJMSExpiration)).toOpt
  def getJMSPriority: Option[Int]                     = Try(Option(wrapped.getJMSPriority)).toOpt
  def getJMSDeliveryTime: Option[Long]                = Try(Option(wrapped.getJMSDeliveryTime)).toOpt

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

  def getObjectProperty(name: String): Option[Any] =
    Try(Option(wrapped.getObjectProperty(name))).toOpt

  def setBooleanProperty(name: String, value: Boolean): Try[Unit] = Try(wrapped.setBooleanProperty(name, value))
  def setByteProperty(name: String, value: Byte): Try[Unit]       = Try(wrapped.setByteProperty(name, value))
  def setDoubleProperty(name: String, value: Double): Try[Unit]   = Try(wrapped.setDoubleProperty(name, value))
  def setFloatProperty(name: String, value: Float): Try[Unit]     = Try(wrapped.setFloatProperty(name, value))
  def setIntProperty(name: String, value: Int): Try[Unit]         = Try(wrapped.setIntProperty(name, value))
  def setLongProperty(name: String, value: Long): Try[Unit]       = Try(wrapped.setLongProperty(name, value))
  def setShortProperty(name: String, value: Short): Try[Unit]     = Try(wrapped.setShortProperty(name, value))
  def setStringProperty(name: String, value: String): Try[Unit]   = Try(wrapped.setStringProperty(name, value))
  def setObjectProperty(name: String, value: Any): Try[Unit]      = Try(wrapped.setObjectProperty(name, value))

  def properties: Try[Map[String, Any]] =
    JmsMessage.properties(wrapped)
}

object JmsMessage {

  def properties(msg: Message): Try[Map[String, Any]] =
    Try {
      val propertyNames = msg.getPropertyNames
      val buf           = collection.mutable.Map.empty[String, Any]
      while (propertyNames.hasMoreElements) {
        val propertyName = propertyNames.nextElement.asInstanceOf[String]
        buf += propertyName -> msg.getObjectProperty(propertyName)
      }
      buf.toMap
    }

  implicit val showMessage: Show[Message] = Show.show[Message] { message =>
    def getStringContent: Try[String] = message match {
      case message: TextMessage => Try(message.getText)
      case _                    => Failure(new RuntimeException())
    }

    Try {
      s"""
         |${properties(message).map(_.mkString("\n")).getOrElse("")}
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

    def getText: Try[String] = Try(wrapped.getText)
  }
}
