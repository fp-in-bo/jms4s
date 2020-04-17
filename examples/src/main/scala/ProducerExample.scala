import cats.data.NonEmptyList
import cats.effect.{ ExitCode, IO, IOApp, Resource }
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
  val messageStrings: NonEmptyList[String]     = NonEmptyList.fromListUnsafe((0 until 10).map(i => s"$i").toList)

  override def run(args: List[String]): IO[ExitCode] = {
    val producerRes = for {
      jmsContext <- contextRes
      producer   <- jmsClient.createProducer(jmsContext, 10)
    } yield producer

    producerRes.use { producer =>
      {
        for {
          _ <- producer.sendN(makeN(messageStrings, outputTopic))
          _ <- producer.sendNWithDelay(makeNWithDelay(messageStrings, outputTopic, delay))
          _ <- producer.send(make1(messageStrings.head, outputTopic))
          _ <- producer.sendWithDelay(make1WithDelay(messageStrings.head, outputTopic, delay))
        } yield ()
      }.as(ExitCode.Success)
    }
  }

  private def make1(
    text: String,
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], DestinationName)] = { mFactory =>
    mFactory
      .makeTextMessage(text)
      .map(message => (message, destinationName))
  }

  private def makeN(
    texts: NonEmptyList[String],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage[IO], DestinationName)]] = { mFactory =>
    texts.traverse { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, destinationName))
    }
  }

  private def make1WithDelay(
    text: String,
    destinationName: DestinationName,
    delay: FiniteDuration
  ): MessageFactory[IO] => IO[(JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))] = { mFactory =>
    mFactory
      .makeTextMessage(text)
      .map(message => (message, (destinationName, Some(delay))))
  }

  private def makeNWithDelay(
    texts: NonEmptyList[String],
    destinationName: DestinationName,
    delay: FiniteDuration
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage[IO], (DestinationName, Option[FiniteDuration]))]] = {
    mFactory =>
      texts.traverse { text =>
        mFactory
          .makeTextMessage(text)
          .map(message => (message, (destinationName, Some(delay))))
      }
  }
}
