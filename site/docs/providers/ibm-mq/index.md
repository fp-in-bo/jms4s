---
layout: docs
title:  "IBM MQ"
---

# IBM MQ

Creating a jms client for an IBM MQ queue manager is as easy as:

```scala mdoc
  import cats.data.NonEmptyList
  import cats.effect.{ IO, Resource }
  import jms4s.ibmmq.ibmMQ
  import jms4s.ibmmq.ibmMQ._
  import jms4s.JmsClient
  import org.typelevel.log4cats.Logger

  def jmsClientResource(implicit L: Logger[IO]): Resource[IO, JmsClient[IO]] =
    ibmMQ.makeJmsClient[IO](
      Config(
        qm = QueueManager("YOUR.QM"),
        endpoints = NonEmptyList.one(Endpoint("localhost", 1414)),
        channel = Channel("YOUR.CHANNEL"),
        username = Some(Username("YOU")),
        password = Some(Password("PW")),
        clientId = ClientId("YOUR.APP")
      )
    )
```