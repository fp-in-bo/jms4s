---
layout: docs
title:  "IBM MQ"
---

# IBM MQ

Creating a jms context for an IBM MQ queue manager is as easy as:

```scala mdoc
import cats.data.NonEmptyList
import cats.effect.{ Blocker, ContextShift, IO, Resource }
import jms4s.ibmmq.ibmMQ
import jms4s.ibmmq.ibmMQ._
import io.chrisdavenport.log4cats.Logger
import jms4s.JmsClient

def jmsClientResource(implicit CS: ContextShift[IO], L: Logger[IO]): Resource[IO, JmsClient[IO]] =
  Blocker
    .apply[IO]
    .flatMap(blocker =>
      ibmMQ.makeJmsClient[IO](
        Config(
          qm = QueueManager("YOUR.QM"),
          endpoints = NonEmptyList.one(Endpoint("localhost", 1414)),
          channel = Channel("YOUR.CHANNEL"),
          username = Some(Username("YOU")),
          password = Some(Password("PW")),
          clientId = ClientId("YOUR.APP")
        ),
        blocker
      )
    )
```

### NB:
A `Blocker` is required since it's then used for blocking IO jms operations.
If your application is already creating a `Blocker`, you should inject that!
