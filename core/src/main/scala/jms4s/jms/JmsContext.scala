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

import cats.effect.{ Concurrent, Resource, Sync }
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSContext
import jms4s.config.{ DestinationName, QueueName, TopicName }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

class JmsContext[F[_]: Sync: Logger: ContextShift: Concurrent](
  private val context: JMSContext,
  private[jms4s] val blocker: Blocker
) {

  def createContext(sessionType: SessionType): Resource[F, JmsContext[F]] =
    Resource
      .make(
        Logger[F].info("Creating context") *> {
          for {
            ctx <- Sync[F].blocking(context.createContext(sessionType.rawAcknowledgeMode))
            _   <- Logger[F].info(s"Context $ctx successfully created")
          } yield ctx
        }
      )(context =>
        Logger[F].info(s"Releasing context $context") *>
          Sync[F].blocking(context.close())
      )
      .map(context => new JmsContext(context, blocker))

  def send(destinationName: DestinationName, message: JmsMessage): F[Unit] =
    createDestination(destinationName)
      .flatMap(destination => Sync[F].blocking(context.createProducer().send(destination.wrapped, message.wrapped)))
      .map(_ => ())

  def send(destinationName: DestinationName, message: JmsMessage, delay: FiniteDuration): F[Unit] =
    for {
      destination <- createDestination(destinationName)
      p           <- Sync[F].delay(context.createProducer())
      _ <- Sync[F].delay(p.setDeliveryDelay(delay.toMillis)) *> Sync[F].blocking(
            p.send(destination.wrapped, message.wrapped)
          )
    } yield ()

  def createJmsConsumer(destinationName: DestinationName): Resource[F, JmsMessageConsumer[F]] =
    for {
      destination <- Resource.eval(createDestination(destinationName))
      consumer <- Resource.make(
                   Logger[F].info(s"Creating consumer for destination $destinationName") *>
                     Sync[F].blocking(context.createConsumer(destination.wrapped))
                 )(consumer =>
                   Logger[F].info(s"Closing consumer for destination $destinationName") *>
                     Sync[F].blocking(consumer.close())
                 )
    } yield new JmsMessageConsumer[F](consumer, blocker)

  def createTextMessage(value: String): F[JmsTextMessage] =
    Sync[F].delay(new JmsTextMessage(context.createTextMessage(value)))

  def commit: F[Unit] = Sync[F].blocking(context.commit())

  def rollback: F[Unit] = Sync[F].blocking(context.rollback())

  private def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(context.createQueue(queue.value)))

  private def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(context.createTopic(topicName.value)))

  def createDestination(destination: DestinationName): F[JmsDestination] = destination match {
    case q: QueueName => createQueue(q).widen[JmsDestination]
    case t: TopicName => createTopic(t).widen[JmsDestination]
  }

}
