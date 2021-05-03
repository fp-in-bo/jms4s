---
layout: docs
title:  "Active MQ Artemis"
---

# Active MQ Artemis

Creating a jms context for an Active MQ Artemis cluster is as easy as:

```scala mdoc
import cats.data.NonEmptyList
import cats.effect.{ Blocker, ContextShift, IO, Resource }
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ._
import io.chrisdavenport.log4cats.Logger
import jms4s.JmsClient

def jmsClientResource(implicit CS: ContextShift[IO], L: Logger[IO]): Resource[IO, JmsClient[IO]] =
  Blocker
    .apply[IO]
    .flatMap(blocker =>
      activeMQ.makeJmsClient[IO](
        Config(
          endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
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


## Why not ActiveMQ 5 "Classic"?
ActiveMQ 5 "Classic" is only supporting JMS 1.1, which is missing a bunch of features we really need to offer.
