---
layout: docs
title:  "Producer"
---

# Producer

A `JmsProducer` is a producer that lets the client publish a message in queues/topics.

- sendN: to send N messages to N Destinations.
```scala
def sendN(
  makeN: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], DestinationName)]]
): F[Unit]
```

- sendNWithDelay: to send N messages to N Destinations with an optional delay.
```scala
  def sendNWithDelay(
    makeNWithDelay: MessageFactory[F] => F[NonEmptyList[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]]
  ): F[Unit]
```

- sendWithDelay: to send a message to a Destination.
```scala
  def sendWithDelay(
    make1WithDelay: MessageFactory[F] => F[(JmsMessage[F], (DestinationName, Option[FiniteDuration]))]
  ): F[Unit]
```
- send: to send a message to a Destination.
```scala
  def send(
    make1: MessageFactory[F] => F[(JmsMessage[F], DestinationName)]
  ): F[Unit]
```

For each operation, the client has to provide a function that knows how to build a `JmsMessage` given a `MessageFactory`.
This may appear counter-intuitive at first, but the reason behind this design is that creating a `JmsMessage` is an operation that involves interacting with JMS APIs, and we want to provide a high-level API so that the user can't do things wrong.

## A complete example

```scala mdoc
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
```

### A note on concurrency

A `JmsProducer` can be used concurrently, performing up to `concurrencyLevel` concurrent operation.
