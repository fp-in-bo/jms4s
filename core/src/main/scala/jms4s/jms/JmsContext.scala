/*
 * Copyright 2021 Alessandro Zoffoli
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jms4s.jms

import cats.effect.{ Blocker, Concurrent, ContextShift, Resource, Sync }
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
            ctx <- blocker.delay(context.createContext(sessionType.rawAcknowledgeMode))
            _   <- Logger[F].info(s"Context $ctx successfully created")
          } yield ctx
        }
      )(context =>
        Logger[F].info(s"Releasing context $context") *>
          blocker.delay(context.close())
      )
      .map(context => new JmsContext(context, blocker))

  def send(destinationName: DestinationName, message: JmsMessage): F[Unit] =
    createDestination(destinationName)
      .flatMap(destination => blocker.delay(context.createProducer().send(destination.wrapped, message.wrapped)))
      .map(_ => ())

  def send(destinationName: DestinationName, message: JmsMessage, delay: FiniteDuration): F[Unit] =
    for {
      destination <- createDestination(destinationName)
      p           <- Sync[F].delay(context.createProducer())
      _ <- Sync[F].delay(p.setDeliveryDelay(delay.toMillis)) *> blocker.delay(
            p.send(destination.wrapped, message.wrapped)
          )
    } yield ()

  def createJmsConsumer(destinationName: DestinationName): Resource[F, JmsMessageConsumer[F]] =
    for {
      destination <- Resource.eval(createDestination(destinationName))
      consumer <- Resource.make(
                   Logger[F].info(s"Creating consumer for destination $destinationName") *>
                     blocker.delay(context.createConsumer(destination.wrapped))
                 )(consumer =>
                   Logger[F].info(s"Closing consumer for destination $destinationName") *>
                     blocker.delay(consumer.close())
                 )
    } yield new JmsMessageConsumer[F](consumer, blocker)

  def createTextMessage(value: String): F[JmsTextMessage] =
    Sync[F].delay(new JmsTextMessage(context.createTextMessage(value)))

  def commit: F[Unit] = blocker.delay(context.commit())

  def rollback: F[Unit] = blocker.delay(context.rollback())

  private def createQueue(queue: QueueName): F[JmsQueue] =
    Sync[F].delay(new JmsQueue(context.createQueue(queue.value)))

  private def createTopic(topicName: TopicName): F[JmsTopic] =
    Sync[F].delay(new JmsTopic(context.createTopic(topicName.value)))

  def createDestination(destination: DestinationName): F[JmsDestination] = destination match {
    case q: QueueName => createQueue(q).widen[JmsDestination]
    case t: TopicName => createTopic(t).widen[JmsDestination]
  }

}
