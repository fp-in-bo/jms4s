---
layout: docs
title:  "Auto-Acknowledger Consumer"
---

# Auto Acknowledger Consumer

A `JmsAutoAcknowledgerConsumer` is a consumer that will automatically acknowledge a message after its reception.
Its only operation is:

```scala
def handle(f: JmsMessage => F[AutoAckAction[F]]): F[Unit]
```

This is where the user of the API can specify its business logic, which can be any effectful operation.

What `handle` expects is an `AutoAckAction[F]`, which can be either:
- an `AckAction.noOp`, which will do nothing since the message will be acknowledged regardless
- an `AckAction.send` in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations

The consumer can be configured specifying a `concurrencyLevel`, which is used internally to scale the operations (receive and then process up to `concurrencyLevel`).

## A complete example

```scala mdoc
import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsClient
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.JmsContext

class AutoAckConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String): IO[AutoAckAction[IO]] = IO.delay {
    if (text.toInt % 2 == 0)
      AutoAckAction.send[IO](_.makeTextMessage("a brand new message").map(newMsg => (newMsg, outputTopic)))
    else
      AutoAckAction.noOp
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createAutoAcknowledgerConsumer(jmsContext, inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { jmsMessage =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text)
      } yield res
    }.as(ExitCode.Success))
  }
}

```
