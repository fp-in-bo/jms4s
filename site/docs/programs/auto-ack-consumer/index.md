---
layout: docs
title:  "Auto-Acknowledger Consumer"
---

# Auto Acknowledger Consumer

A `JmsAutoAcknowledgerConsumer` is a consumer that will automatically acknowledge a message after its reception.
Its only operation is:

```scala
def handle(f: (JmsMessage, MessageFactory[F]) => F[AutoAckAction[F]]): F[Unit]
```

This is where the user of the API can specify its business logic, which can be any effectful operation.

Creating a message is as effectful operation as well, and the `MessageFactory` argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages.

What `handle` expects is an `AutoAckAction[F]`, which can be either:
- an `AckAction.noOp`, which will instructs the lib to do nothing since the message will be acknowledged regardless
- an `AckAction.send` in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations

The consumer can be configured specifying a `concurrencyLevel`, which is used internally to scale the operations (receive and then process up to `concurrencyLevel`).

A complete example is available in the [example project](https://github.com/fp-in-bo/jms4s/blob/main/examples/src/main/scala/AutoAckConsumerExample.scala).
