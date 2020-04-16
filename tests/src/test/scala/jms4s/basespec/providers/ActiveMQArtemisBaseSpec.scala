package jms4s.basespec.providers

import cats.data.NonEmptyList
import cats.effect.{ Blocker, ContextShift, IO, Resource }
import eu.timepit.refined.auto._
import jms4s.activemq.activeMQ
import jms4s.activemq.configuration.{
  ClientId,
  Configuration,
  Credential,
  Endpoint,
  Hostname,
  Password,
  Port,
  Username
}
import jms4s.basespec.Jms4sBaseSpec
import jms4s.jms.JmsContext

trait ActiveMQArtemisBaseSpec extends Jms4sBaseSpec {

  override def contextRes(implicit cs: ContextShift[IO]): Resource[IO, JmsContext[IO]] =
    Blocker
      .apply[IO]
      .flatMap { blocker =>
        activeMQ.makeContext[IO](
          Configuration(
            endpoints = NonEmptyList.one(Endpoint(Hostname("localhost"), Port(61616))),
            credentials = Some(Credential(Username("admin"), Password("passw0rd"))),
            clientId = ClientId("jms-specs")
          ),
          blocker
        )
      }

}
