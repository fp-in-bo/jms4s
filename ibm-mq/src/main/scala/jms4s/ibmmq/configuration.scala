package jms4s.ibmmq

import cats.data.NonEmptyList
import eu.timepit.refined.types.numeric.NonNegInt
import eu.timepit.refined.types.string.NonEmptyString
import io.estatico.newtype.macros.newtype

object configuration {

  final case class Configuration(
    qm: QueueManager,
    endpoints: NonEmptyList[Endpoint],
    channel: Channel,
    credential: Option[Credential],
    clientId: ClientId
  )

  @newtype case class Username(value: NonEmptyString)
  @newtype case class Password(value: String)
  final case class Credential(username: Username, password: Password)

  @newtype case class Hostname(value: NonEmptyString)
  @newtype case class Port(value: NonNegInt)
  final case class Endpoint(host: Hostname, port: Port)

  @newtype case class QueueManager(value: NonEmptyString)
  @newtype case class Channel(value: NonEmptyString)
  @newtype case class ClientId(value: NonEmptyString)
}
