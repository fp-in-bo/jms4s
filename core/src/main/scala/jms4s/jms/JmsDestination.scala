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

package jms4s.jms

import cats.Show

import javax.jms.{ Destination, Queue, Topic }

sealed abstract class JmsDestination {
  private[jms4s] val wrapped: Destination
  def name: String

  override def toString: String = s"${getClass.getSimpleName}($name)"
}

object JmsDestination {

  class JmsQueue private[jms4s] (private[jms4s] val wrapped: Queue) extends JmsDestination {
    override def name: String = wrapped.getQueueName
  }

  class JmsTopic private[jms4s] (private[jms4s] val wrapped: Topic) extends JmsDestination {
    override def name: String = wrapped.getTopicName
  }

  class Other private[jms4s] (private[jms4s] val wrapped: Destination) extends JmsDestination {
    override def name: String = wrapped.toString
  }

  def fromDestination(destination: Destination): JmsDestination =
    destination match {
      case queue: Queue => new JmsQueue(queue)
      case topic: Topic => new JmsTopic(topic)
      case x            => new Other(x)
    }

  implicit val showDestination: Show[JmsDestination] = Show.fromToString[JmsDestination]
}
