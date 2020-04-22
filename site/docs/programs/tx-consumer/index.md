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

Creating a message is as effectful operation as well, and the `MessageFactory` argument will provide the only way in which a client can create a brand new message. This argument can be ignored if the client is only consuming messages.

What `handle` expects is a `TransactionAction[F]`, which can be either:
- a `TransactionAction.commit`, which will instructs the lib to commit the local transaction
- a `TransactionAction.rollback`, which will instructs the lib to rollback the local transaction
- a `TransactionAction.send` in all its forms, which can be used to send 1 or multiple messages to 1 or multiple destinations and then commit the local transaction

The consumer can be configured specifying a `concurrencyLevel`, which is used internally to scale the operations (receive and then process up to `concurrencyLevel`).

A complete example is available in the [example project](https://github.com/fp-in-bo/jms4s/blob/master/examples/src/main/scala/TransactedConsumerExample.scala).
