---
layout: docs
title:  "Transacted Consumer"
---

# Transacted Consumer

A `JmsTransactedConsumer` is a consumer that will use a local transaction to receive a message and which lets the client decide whether to commit or rollback it.
Its only operation is:

```scala
def handle(f: (JmsMessage, MessageFactory[F]) => F[TransactionAction[F]]): F[Unit]
```

This is where the user of the API can specify its business logic, which can be any effectful operation.

What `handle` expects is a `TransactionAction[F]`, which can be either:
- a `TransactionAction.commit`, which will commit the local transaction
- a `TransactionAction.rollback`, which will rollback the local transaction (message will be put in the input queue)
- a `TransactionAction.send` in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations and then commit the local transaction

The consumer can be configured specifying a `concurrencyLevel`, which is used internally to scale the operations (receive and then process up to `concurrencyLevel`).

## A complete example

````scala mdoc
import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsClient
import jms4s.JmsTransactedConsumer._
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.{ JmsContext, MessageFactory }

class TransactedConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String, mf: MessageFactory[IO]): IO[TransactionAction[IO]] =
    if (text.toInt % 2 == 0)
      mf.makeTextMessage("a brand new message").map(newMsg => TransactionAction.send(newMsg, outputTopic))
    else if (text.toInt % 3 == 0)
      IO.pure(TransactionAction.rollback)
    else IO.pure(TransactionAction.commit)

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createTransactedConsumer(jmsContext, inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text, mf)
      } yield res
    }.as(ExitCode.Success))
  }
}
````
