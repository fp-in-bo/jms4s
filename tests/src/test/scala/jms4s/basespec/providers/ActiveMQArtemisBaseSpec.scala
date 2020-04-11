package jms4s.basespec.providers

import cats.data.NonEmptyList
import cats.effect.{ Blocker, ContextShift, IO, Resource }
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ._
import jms4s.basespec.Jms4sBaseSpec
import jms4s.jms.JmsContext

trait ActiveMQArtemisBaseSpec extends Jms4sBaseSpec {

  override def contextRes(implicit cs: ContextShift[IO]): Resource[IO, JmsContext[IO]] =
    Blocker
      .apply[IO]
      .flatMap(
        blocker =>
          activeMQ.makeContext[IO](
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
