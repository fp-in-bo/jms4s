---
layout: docs
title:  "Acknowledger Consumer"
---

# Acknowledger Consumer

A `JmsAcknowledgerConsumer` is a consumer which let the client decide whether confirm (a.k.a. ack) or reject (a.k.a. nack) a message after its reception.
Its only operation is:

```scala
def handle(f: (JmsMessage, MessageFactory[F]) => F[AckAction[F]]): F[Unit]
```

This is where the user of the API can specify its business logic, which can be any effectful operation.

Creating a message is as effectful operation as well, and the `MessageFactory` argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages.

What `handle` expects is an `AckAction[F]`, which can be either:
- an `AckAction.ack`, which will instructs the lib to confirm the message
- an `AckAction.noAck`, which will instructs the lib to do nothing
- an `AckAction.send` in all its forms, which can be used to instruct the lib to send 1 or multiple messages to 1 or multiple destinations

The consumer can be configured specifying a `concurrencyLevel`, which is used internally to scale the operations (receive and then process up to `concurrencyLevel`).

A complete example is available in the [example project](https://github.com/fp-in-bo/jms4s/blob/main/examples/src/main/scala/AckConsumerExample.scala).
