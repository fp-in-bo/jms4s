---
layout: docs
title:  "Producer"
---

# Producer

A `JmsProducer` is a producer that lets the client publish a message in queues/topics.

- sendN: to send N messages to N Destinations.
```scala
def sendN(
  makeN: MessageFactory[F] => F[NonEmptyList[(JmsMessage, DestinationName)]]
): F[Unit]
```

- sendNWithDelay: to send N messages to N Destinations with an optional delay.
```scala
  def sendNWithDelay(
    makeNWithDelay: MessageFactory[F] => F[NonEmptyList[(JmsMessage, (DestinationName, Option[FiniteDuration]))]]
  ): F[Unit]
```

- sendWithDelay: to send a message to a Destination.
```scala
  def sendWithDelay(
    make1WithDelay: MessageFactory[F] => F[(JmsMessage, (DestinationName, Option[FiniteDuration]))]
  ): F[Unit]
```
- send: to send a message to a Destination.
```scala
  def send(
    make1: MessageFactory[F] => F[(JmsMessage, DestinationName)]
  ): F[Unit]
```

For each operation, the client has to provide a function that knows how to build a `JmsMessage` given a `MessageFactory`.
This may appear counter-intuitive at first, but the reason behind this design is that creating a `JmsMessage` is an operation that involves interacting with JMS APIs, and we want to provide a high-level API so that the user can't do things wrong.

A complete example is available in the [example project](https://github.com/fp-in-bo/jms4s/blob/main/examples/src/main/scala/ProducerExample.scala).

### A note on concurrency

A `JmsProducer` can be used concurrently, performing up to `concurrencyLevel` concurrent operation.
