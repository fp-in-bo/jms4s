package fs2jms

import cats.effect.{ Resource, Sync }
import cats.implicits._
import fs2jms.config.QueueName
import io.chrisdavenport.log4cats.Logger
import javax.jms._

import scala.util.{ Failure, Try }

object jms {

  def makeSession[F[_]: Sync: Logger](queueConnection: QueueConnection): Resource[F, QueueSession] =
    for {
      session <- Resource.fromAutoCloseable(
                  Logger[F].info(s"Opening QueueSession for $queueConnection.") *>
                    Sync[F].delay(queueConnection.createQueueSession(true, Session.SESSION_TRANSACTED))
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueSession $session for $queueConnection."))
    } yield session

  def makeConsumer[F[_]: Sync: Logger](session: QueueSession, input: QueueName): Resource[F, MessageConsumer] =
    for {
      queue <- Resource.liftF(
                Logger[F].info(s"Opening MessageConsumer for $input, session: $session...") *>
                  Sync[F].delay(session.createQueue(input.value))
              )
      consumer <- Resource.fromAutoCloseable(Sync[F].delay(session.createConsumer(queue)))
      _        <- Resource.liftF(Logger[F].info(s"Opened MessageConsumer for $input, session: $session."))
    } yield consumer

  def makeProducer[F[_]: Sync: Logger](session: QueueSession, queue: QueueName): Resource[F, MessageProducer] =
    for {
      queue <- Resource.liftF(
                Logger[F].info(s"Opening MessageProducer for $queue, session: $session...") *>
                  Sync[F].delay(session.createQueue(queue.value))
              )
      producer <- Resource.fromAutoCloseable(Sync[F].delay(session.createProducer(queue)))
      _        <- Resource.liftF(Logger[F].info(s"Opened MessageProducer for $queue, session: $session."))
    } yield producer

  private def propertyNames(message: Message): List[String] = {
    val e   = message.getPropertyNames
    val buf = collection.mutable.Buffer.empty[String]
    while (e.hasMoreElements) {
      val propertyName = e.nextElement.asInstanceOf[String]
      buf += propertyName
    }
    buf.toList
  }

  private def copyMessageHeader(message: Message, newMessage: Message): Unit = {
    newMessage.setJMSMessageID(message.getJMSMessageID)
    newMessage.setJMSTimestamp(message.getJMSTimestamp)
    newMessage.setJMSCorrelationID(message.getJMSCorrelationID)
    newMessage.setJMSReplyTo(message.getJMSReplyTo)
    newMessage.setJMSDestination(message.getJMSDestination)
    newMessage.setJMSDeliveryMode(message.getJMSDeliveryMode)
    newMessage.setJMSRedelivered(message.getJMSRedelivered)
    newMessage.setJMSType(message.getJMSType)
    newMessage.setJMSExpiration(message.getJMSExpiration)
    newMessage.setJMSPriority(message.getJMSPriority)
    propertyNames(message).foreach(
      propertyName => newMessage.setObjectProperty(propertyName, message.getObjectProperty(propertyName))
    )
  }

  implicit class JMSMessage(private val message: Message) extends AnyVal {

    private def getStringContent: Try[String] = message match {
      case message: TextMessage => Try(message.getText)
      case _                    => Failure(new RuntimeException())
    }

    def show: String =
      Try(
        s"""
           |${propertyNames(message).map(pn => s"$pn       ${message.getObjectProperty(pn)}").mkString("\n")}
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
      ).getOrElse("")

    def copyMessage[F[_]: Sync](session: Session): F[Message] = Sync[F].delay {
      message match {
        case message: TextMessage =>
          val newTextMessage = session.createTextMessage()
          newTextMessage.setText(message.getText)
          copyMessageHeader(message, newTextMessage)
          newTextMessage

        case msg => throw new UnsupportedOperationException(s"unsupported jms message \n$msg")
      }
    }
  }
}
