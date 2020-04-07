package jms4s.basespec.providers

import cats.data.NonEmptyList
import cats.effect.{ Blocker, IO, Resource }
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ.{ ClientId, Config, Endpoint, Password, Username }
import jms4s.basespec.Jms4sBaseSpec
import jms4s.jms.JmsConnection

trait ActiveMQArtemisBaseSpec extends Jms4sBaseSpec {
  override def connectionRes: Resource[IO, JmsConnection[IO]] =
    Blocker
      .apply[IO]
      .flatMap(
        blocker =>
          activeMQ.makeConnection[IO](
            Config(
              endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
              username = Some(Username("admin")),
              password = Some(Password("passw0rd")),
              clientId = ClientId("jms-specs")
            ),
            blocker
          )
      )
}
