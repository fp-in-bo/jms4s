package jms4s.basespec

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import jms4s.config.{DestinationName, QueueName, TopicName}
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.{JmsConnection, JmsContext, JmsMessageConsumer, MessageFactory}

import scala.concurrent.duration.{FiniteDuration, _}

trait Jms4sBaseSpec {
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def connectionRes(implicit cs: ContextShift[IO]): Resource[IO, JmsConnection[IO]]

  def contextRes(implicit cs: ContextShift[IO]): Resource[IO, JmsContext[IO]]

  val body                         = "body"
  val nMessages: Int               = 50
  val bodies: List[String]         = (0 until nMessages).map(i => s"$i").toList
  val poolSize: Int                = 4
  val timeout: FiniteDuration      = 4.seconds // CI is slow...
  val delay: FiniteDuration        = 200.millis
  val delayWithTolerance: Duration = delay * 0.8 // it looks like activemq is not fully respecting delivery delay
  val topicName: TopicName         = TopicName("DEV.BASE.TOPIC")
  val topicName2: TopicName        = TopicName("DEV.BASE.TOPIC.1")
  val inputQueueName: QueueName    = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName  = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName  = QueueName("DEV.QUEUE.3")

  def receiveBodyAsTextOrFail(destinationName: DestinationName, context: JmsContext[IO]): IO[String] =
    context.receive(destinationName)
      .flatMap(_.asJmsTextMessage)
      .flatMap(_.getText)

  def receiveMessage(consumer: JmsMessageConsumer[IO]): IO[JmsTextMessage[IO]] =
    consumer.receiveJmsMessage
      .flatMap(_.asJmsTextMessage)

  def receiveUntil(
    destinationName: DestinationName,
    context: JmsContext[IO],
    received: Ref[IO, Set[String]],
    nMessages: Int
  ): IO[Set[String]] =
    receiveBodyAsTextOrFail(destinationName,context)
      .flatMap(body => received.update(_ + body) *> received.get)
      .iterateUntil(_.size == nMessages)

  def messageFactory(
    message: JmsTextMessage[IO],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], DestinationName)] = { mFactory: MessageFactory[IO] =>
    message.getText.flatMap { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, destinationName))
    }
  }

  def messageFactory(
    message: JmsTextMessage[IO]
  ): MessageFactory[IO] => IO[JmsTextMessage[IO]] = { mFactory: MessageFactory[IO] =>
    message.getText.flatMap { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message))
    }
  }

  def messageWithDelayFactory(
    message: (JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))] = {
    mFactory: MessageFactory[IO] =>
      message._1.getText.flatMap { text =>
        mFactory
          .makeTextMessage(text)
          .map(m => (m, (message._2._1, message._2._2)))
      }
  }

  def messageFactory(
    messages: NonEmptyList[JmsTextMessage[IO]]
  ): MessageFactory[IO] => IO[NonEmptyList[JmsTextMessage[IO]]] = { mFactory: MessageFactory[IO] =>
    messages
      .map(
        message =>
          message.getText.flatMap { text =>
            mFactory
              .makeTextMessage(text)
              .map(message => (message))
          }
      )
      .sequence
  }
}
