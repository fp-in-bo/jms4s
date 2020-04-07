package jms4s.config

import cats.Order
import cats.implicits._

sealed trait DestinationName        extends Product with Serializable
case class QueueName(value: String) extends DestinationName
case class TopicName(value: String) extends DestinationName

object DestinationName {
  implicit val orderingDestinationName: Order[DestinationName] = Order.from[DestinationName] {
    case (x, y) => Order[String].compare(x.toString, y.toString)
  }
}
