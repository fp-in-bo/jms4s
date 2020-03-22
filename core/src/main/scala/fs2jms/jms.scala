package fs2jms

import cats.effect.{ IO, Resource }
import cats.implicits._
import fs2jms.config.QueueName
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import javax.jms._

import scala.util.{ Failure, Try }

object jms {
  private val logger = Slf4jLogger.getLogger[IO]

  def makeSession(queueConnection: QueueConnection): Resource[IO, QueueSession] =
    for {
      session <- Resource.fromAutoCloseable(
                  logger.info(s"Opening QueueSession for $queueConnection.") *>
                    IO.delay(queueConnection.createQueueSession(true, Session.SESSION_TRANSACTED))
                )
      _ <- Resource.liftF(logger.info(s"Opened QueueSession $session for $queueConnection."))
    } yield session

  def makeConsumer(session: QueueSession, input: QueueName): Resource[IO, MessageConsumer] =
    for {
      queue <- Resource.liftF(
                logger.info(s"Opening MessageConsumer for $input, session: $session...") *>
                  IO.delay(session.createQueue(input.value))
              )
      consumer <- Resource.fromAutoCloseable(IO.delay(session.createConsumer(queue)))
      _        <- Resource.liftF(logger.info(s"Opened MessageConsumer for $input, session: $session."))
    } yield consumer

  def makeProducer(session: QueueSession, queue: QueueName): Resource[IO, MessageProducer] =
    for {
      queue <- Resource.liftF(
                logger.info(s"Opening MessageProducer for $queue, session: $session...") *>
                  IO.delay(session.createQueue(queue.value))
              )
      producer <- Resource.fromAutoCloseable(IO.delay(session.createProducer(queue)))
      _        <- Resource.liftF(logger.info(s"Opened MessageProducer for $queue, session: $session."))
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

    private def slrCountPropertyName: String = "SLRCount"

    def slrCount: Int = Try(message.getIntProperty(slrCountPropertyName)).toOption.getOrElse(0)

    def increaseSlrCount: IO[Unit] = IO.delay(message.setIntProperty(slrCountPropertyName, slrCount + 1))

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

    def copyMessage(session: Session): IO[Message] = IO {
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
