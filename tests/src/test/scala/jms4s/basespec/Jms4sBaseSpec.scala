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

package jms4s.basespec

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import jms4s.JmsClient
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.{ JmsMessageConsumer, MessageFactory }
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{ FiniteDuration, _ }

trait Jms4sBaseSpec {
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def jmsClientRes(implicit cs: Async[IO]): Resource[IO, JmsClient[IO]]

  val body                         = "body"
  val nMessages: Int               = 50
  val bodies: List[String]         = (0 until nMessages).map(i => s"$i").toList
  val poolSize: Int                = 2
  val timeout: FiniteDuration      = 4.seconds // CI is slow...
  val delay: FiniteDuration        = 200.millis
  val delayWithTolerance: Duration = delay * 0.8 // it looks like activemq is not fully respecting delivery delay
  val topicName1: TopicName        = TopicName("DEV.BASE.TOPIC")
  val topicName2: TopicName        = TopicName("DEV.BASE.TOPIC.1")
  val inputQueueName: QueueName    = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName  = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName  = QueueName("DEV.QUEUE.3")

  def receiveBodyAsTextOrFail(consumer: JmsMessageConsumer[IO]): IO[String] =
    consumer.receiveJmsMessage
      .flatMap(_.asTextF[IO])

  def receiveMessage(consumer: JmsMessageConsumer[IO]): IO[JmsTextMessage] =
    consumer.receiveJmsMessage
      .flatMap(_.asJmsTextMessageF[IO])

  def receiveUntil(
    consumer: JmsMessageConsumer[IO],
    received: Ref[IO, Set[String]],
    nMessages: Int
  ): IO[Set[String]] =
    receiveBodyAsTextOrFail(consumer)
      .flatMap(body => received.update(_ + body) *> received.get)
      .iterateUntil(_.size == nMessages)

  def messageFactory(
    message: JmsTextMessage,
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[(JmsTextMessage, DestinationName)] = { mFactory: MessageFactory[IO] =>
    message.asTextF[IO].flatMap { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, destinationName))
    }
  }

  def messageFactory(
    message: JmsTextMessage
  ): MessageFactory[IO] => IO[JmsTextMessage] = { mFactory: MessageFactory[IO] =>
    message.asTextF[IO].flatMap(text => mFactory.makeTextMessage(text))
  }

  def messageWithDelayFactory(
    message: (JmsTextMessage, (DestinationName, Option[FiniteDuration]))
  ): MessageFactory[IO] => IO[(JmsTextMessage, (DestinationName, Option[FiniteDuration]))] = {
    mFactory: MessageFactory[IO] =>
      message._1.asTextF[IO].flatMap { text =>
        mFactory
          .makeTextMessage(text)
          .map(m => (m, (message._2._1, message._2._2)))
      }
  }

  def messageFactory(
    messages: NonEmptyList[JmsTextMessage]
  ): MessageFactory[IO] => IO[NonEmptyList[JmsTextMessage]] = { mFactory: MessageFactory[IO] =>
    messages
      .map(message => message.asTextF[IO].flatMap(text => mFactory.makeTextMessage(text)))
      .sequence
  }

  def messageFactory(
    messages: NonEmptyList[JmsTextMessage],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage, DestinationName)]] =
    (mFactory: MessageFactory[IO]) =>
      messages.map { message =>
        IO.fromTry(message.getText)
          .flatMap(text => mFactory.makeTextMessage(text))
          .map(message => (message, destinationName))
      }.sequence
}
