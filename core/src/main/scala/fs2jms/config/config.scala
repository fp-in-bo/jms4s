package fs2jms.config

import cats.data.NonEmptyList

case class Config(
  qm: QueueManager,
  endpoints: NonEmptyList[Endpoint],
  channel: Channel,
  username: Option[Username] = None,
  password: Option[Password] = None
)

case class Username(value: String) extends AnyVal

case class Password(value: String) extends AnyVal

case class Endpoint(host: String, port: Int)

case class QueueName(value: String) extends AnyVal

case class TopicName(value: String) extends AnyVal

case class QueueManager(value: String) extends AnyVal

case class Channel(value: String) extends AnyVal
