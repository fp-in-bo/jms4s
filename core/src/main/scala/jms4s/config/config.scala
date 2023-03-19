/*
 * Copyright (c) 2020 Functional Programming in Bologna
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package jms4s.config

import cats.Order
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }

sealed trait DestinationName extends Product with Serializable

case class QueueName(value: String) extends DestinationName
case class TopicName(value: String) extends DestinationName

case class TemporaryQueueName(destination: JmsQueue) extends DestinationName {
  def value: String = destination.name
}

case class TemporaryTopicName(destination: JmsTopic) extends DestinationName {
  def value: String = destination.name
}

object DestinationName {

  implicit val orderingDestinationName: Order[DestinationName] = Order.from[DestinationName] {
    case (x, y) => Order[String].compare(x.toString, y.toString)
  }
}
