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

sealed trait DestinationName        extends Product with Serializable
case class QueueName(value: String) extends DestinationName
case class TopicName(value: String) extends DestinationName

object DestinationName {
  implicit val orderingDestinationName: Order[DestinationName] = Order.from[DestinationName] {
    case (x, y) => Order[String].compare(x.toString, y.toString)
  }
}

case class QueueManager(value: String) extends AnyVal

case class Channel(value: String) extends AnyVal
