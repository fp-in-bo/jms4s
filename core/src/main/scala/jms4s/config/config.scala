package jms4s.config

import cats.Order
import cats.data.NonEmptyList
import cats.implicits._

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

object QueueName {
  implicit val orderingQueueName: Order[QueueName] = Order.from[QueueName] {
    case (x, y) => Order[String].compare(x.value, y.value)
  }
}

case class TopicName(value: String) extends AnyVal

case class QueueManager(value: String) extends AnyVal

case class Channel(value: String) extends AnyVal
