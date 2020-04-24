package jms4s.basespec

import cats.data.NonEmptyList
import cats.effect.concurrent.{ MVar, Ref }
import cats.effect.{ Concurrent, ContextShift, IO, Resource, Timer }
import cats.implicits._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.{ JmsMessageConsumer, MessageFactory }
import jms4s.{ JmsAutoAcknowledgerConsumer, JmsClient }

import scala.concurrent.duration.{ FiniteDuration, _ }

trait Jms4sBaseSpec {
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def jmsClientRes(implicit cs: ContextShift[IO]): Resource[IO, JmsClient[IO]]

  val body                         = "body"
  val nMessages: Int               = 50
  val bodies: List[String]         = (0 until nMessages).map(i => s"$i").toList
  val poolSize: Int                = 1
  val timeout: FiniteDuration      = 4.seconds // CI is slow...
  val delay: FiniteDuration        = 200.millis
  val delayWithTolerance: Duration = delay * 0.8 // it looks like activemq is not fully respecting delivery delay
  val topicName1: TopicName        = TopicName("DEV.BASE.TOPIC")
  val topicName2: TopicName        = TopicName("DEV.BASE.TOPIC.1")
  val inputQueueName: QueueName    = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName  = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName  = QueueName("DEV.QUEUE.3")

  def receiveBodyAsTextOrFail(consumer: JmsMessageConsumer[IO]): IO[String] =
    consumer.receiveJmsMessage
      .flatMap(_.asTextF[IO])

  def receiveUntil(
    consumer: JmsAutoAcknowledgerConsumer[IO],
    received: Ref[IO, Set[String]],
    nMessages: Int,
    duration: FiniteDuration
  )(implicit F: Concurrent[IO], cs: ContextShift[IO], t: Timer[IO]): IO[Set[String]] = {
    def readMessages =
      consumer.handle((m, _) => m.asTextF[IO].flatMap(text => received.update(_ + text).as(AutoAckAction.noOp)))

    for {
      fiber <- readMessages.start
      set   <- received.get.iterateUntil(_.size == nMessages).timeout(duration).guarantee(fiber.cancel)
    } yield set
  }

  def receive(
    consumer: JmsAutoAcknowledgerConsumer[IO],
    duration: FiniteDuration
  )(implicit F: Concurrent[IO], cs: ContextShift[IO], t: Timer[IO]): IO[String] = {
    val received = MVar[IO].empty[String]

    def readMessage(mVar: MVar[IO, String]) =
      consumer.handle((m, _) => m.asTextF[IO].flatMap(tm => mVar.put(tm).as(AutoAckAction.noOp)))

    for {
      mVar  <- received
      fiber <- readMessage(mVar).start
      tm    <- mVar.read.timeout(duration).guarantee(fiber.cancel)
    } yield tm
  }

  def messageWithDelayFactory(
    message: (String, (DestinationName, Option[FiniteDuration]))
  ): MessageFactory[IO] => IO[(JmsTextMessage, (DestinationName, Option[FiniteDuration]))] = {
    mFactory: MessageFactory[IO] => mFactory.makeTextMessage(message._1).map(m => (m, message._2))
  }

  def messageFactory(
    texts: List[String],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage, DestinationName)]] =
    mf =>
      NonEmptyList
        .fromListUnsafe(texts.map(b => mf.makeTextMessage(b).map(t => (t, destinationName))))
        .sequence
}
