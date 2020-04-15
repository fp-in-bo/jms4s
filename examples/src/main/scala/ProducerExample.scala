import cats.data.NonEmptyList
import cats.effect.{ ExitCode, IO, IOApp, Resource }
import cats.implicits._
import jms4s.JmsClient
import jms4s.config.{ DestinationName, TopicName }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.{ JmsContext, MessageFactory }

import scala.concurrent.duration.{ FiniteDuration, _ }

class ProducerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")
  val delay: FiniteDuration                    = 10.millis
  val concurrencyLevel                         = 10
  val messageStrings: List[String]             = (0 until concurrencyLevel).map(i => s"$i").toList

  override def run(args: List[String]): IO[ExitCode] = {
    val producerRes = for {
      jmsContext <- contextRes
      messages   <- Resource.liftF(messageStrings.traverse(i => jmsContext.createTextMessage(i)))
      producer   <- jmsClient.createProducer(jmsContext, concurrencyLevel)
    } yield (producer, messages)

    producerRes.use {
      case (producer, messages) => {
          for {
            _ <- messages.toNel.fold(IO.unit)(ms => producer.sendN(sendNmessageFactory(ms, outputTopic)))
            _ <- messages.toNel.fold(IO.unit)(ms =>
                  producer.sendNWithDelay(sendNWithDelayMessageFactory(ms, outputTopic, Some(delay)))
                )
            _ <- producer.send(sendMessageFactory(messages.head, outputTopic))
            _ <- producer.sendWithDelay(sendWithDelayMessageFactory(messages.head, outputTopic, Some(delay)))
          } yield ()
        }.as(ExitCode.Success)
    }
  }

  private def sendMessageFactory(
    message: JmsTextMessage[IO],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], DestinationName)] = { (mFactory: MessageFactory[IO]) =>
    message.getText.flatMap { text =>
      mFactory
        .makeTextMessage(text)
    }.map(message => (message, destinationName))
  }

  private def sendNmessageFactory(
    messages: NonEmptyList[JmsTextMessage[IO]],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage[IO], DestinationName)]] = { (mFactory: MessageFactory[IO]) =>
    messages.map { message =>
      message.getText.flatMap { text =>
        mFactory
          .makeTextMessage(text)
      }.map(message => (message, destinationName))
    }.sequence
  }

  private def sendWithDelayMessageFactory(
    message: JmsTextMessage[IO],
    destinationName: DestinationName,
    delay: Option[FiniteDuration]
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))] =
    (mFactory: MessageFactory[IO]) =>
      message.getText
        .flatMap(text => mFactory.makeTextMessage(text))
        .map(message => (message, (destinationName, delay)))

  private def sendNWithDelayMessageFactory(
    messages: NonEmptyList[JmsTextMessage[IO]],
    destinationName: DestinationName,
    delay: Option[FiniteDuration]
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))]] =
    (mFactory: MessageFactory[IO]) =>
      messages.map { message =>
        message.getText
          .flatMap(text => mFactory.makeTextMessage(text))
          .map(message => (message, (destinationName, delay)))
      }.sequence
}
