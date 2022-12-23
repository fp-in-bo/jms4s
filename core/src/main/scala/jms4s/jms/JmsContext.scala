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

import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import jms4s.config.{ Destination, QueueName, TemporaryQueue, TemporaryTopic, TopicName }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.model.SessionType
import org.typelevel.log4cats.Logger

import javax.jms.JMSContext
import scala.concurrent.duration.FiniteDuration

class JmsContext[F[_]: Async: Logger](private val context: JMSContext) {

  def createContext(sessionType: SessionType): Resource[F, JmsContext[F]] =
    Resource
      .make(
        for {
          _   <- Logger[F].info("Creating context")
          ctx <- Sync[F].blocking(context.createContext(sessionType.rawAcknowledgeMode))
          _   <- Logger[F].info(s"Context $ctx successfully created")
        } yield ctx
      )(context =>
        Logger[F].info(s"Releasing context $context") *>
          Sync[F].blocking(context.close())
      )
      .map(context => new JmsContext(context))

  def send(destination: Destination, message: JmsMessage): F[Unit] =
    for {
      jmsDestination <- createDestination(destination)
      p              <- Sync[F].blocking(context.createProducer())
      _              <- Sync[F].blocking(p.send(jmsDestination.wrapped, message.wrapped))
    } yield ()

  def send(destination: Destination, message: JmsMessage, delay: FiniteDuration): F[Unit] =
    for {
      jmsDestination <- createDestination(destination)
      p              <- Sync[F].blocking(context.createProducer())
      _              <- Sync[F].delay(p.setDeliveryDelay(delay.toMillis))
      _              <- Sync[F].blocking(p.send(jmsDestination.wrapped, message.wrapped))
    } yield ()

  def createJmsConsumer(
    destination: Destination,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsMessageConsumer[F]] =
    for {
      jmsDestination <- Resource.eval(createDestination(destination))
      consumer <- Resource.make(
                   Logger[F].info(s"Creating consumer for destination $destination(${jmsDestination.name})") *>
                     Sync[F].blocking(context.createConsumer(jmsDestination.wrapped))
                 )(consumer =>
                   Logger[F].info(s"Closing consumer for destination $destination(${jmsDestination.name})") *>
                     Sync[F].blocking(consumer.close())
                 )
    } yield new JmsMessageConsumer[F](consumer, pollingInterval)

  def createTextMessage(value: String): F[JmsTextMessage] =
    Sync[F].blocking(context.createTextMessage(value)).map(new JmsTextMessage(_))

  def commit: F[Unit] = Sync[F].blocking(context.commit())

  def rollback: F[Unit] = Sync[F].blocking(context.rollback())

  private def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].blocking(context.createQueue(queue.value)).map(new JmsQueue(_))

  private def createTemporaryQueue(): F[JmsQueue] =
    Sync[F].blocking(context.createTemporaryQueue()).map(new JmsQueue(_))

  private def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].blocking(context.createTopic(topicName.value)).map(new JmsTopic(_))

  private def createTemporaryTopic(): F[JmsTopic] =
    Sync[F].blocking(context.createTemporaryTopic()).map(new JmsTopic(_))

  def createDestination(destination: Destination): F[JmsDestination] = destination match {
    case TemporaryQueue(_) => createTemporaryQueue().widen[JmsDestination]
    case TemporaryTopic(_) => createTemporaryTopic().widen[JmsDestination]
    case qn: QueueName     => createQueue(qn).widen[JmsDestination]
    case tn: TopicName     => createTopic(tn).widen[JmsDestination]
  }

}
