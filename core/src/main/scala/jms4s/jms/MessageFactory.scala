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

import cats.syntax.all._
import cats.{ Applicative, MonadThrow }
import jms4s.jms.JmsDestination.{ JmsQueue, JmsTopic }
import jms4s.jms.JmsMessage.JmsTextMessage

import javax.jms.{ Queue, Topic }
import scala.util.{ Failure, Try }

class MessageFactory[F[_]](private val context: JmsContext[F]) extends AnyVal {
  def makeTextMessage(value: String): F[JmsTextMessage] = context.createTextMessage(value)

  def cloneMessageF(original: JmsTextMessage)(implicit mt: MonadThrow[F]): F[JmsTextMessage] =
    attemptCloneMessage(original).flatMap(_.liftTo[F])

  def attemptCloneMessage(original: JmsTextMessage)(implicit a: Applicative[F]): F[Either[Throwable, JmsTextMessage]] =
    original.getText
      .traverse(makeTextMessage)
      .map { x =>
        for {
          copied <- x
          _      <- copyMessageHeaders(original, copied)
        } yield copied
      }
      .map(_.toEither)

  private def copyMessageHeaders(message: JmsMessage, newMessage: JmsMessage): Try[Unit] =
    for {
      _ <- message.getJMSMessageId.traverse_(newMessage.setJMSMessageID)
      _ <- message.getJMSTimestamp.traverse_(newMessage.setJMSTimestamp)
      _ <- message.getJMSCorrelationId.traverse_(newMessage.setJMSCorrelationId)
      _ <- message.getJMSReplyTo.traverse_ {
            case queue: Queue => newMessage.setJMSReplyTo(new JmsQueue(queue))
            case topic: Topic => newMessage.setJMSReplyTo(new JmsTopic(topic))
            case _            => Failure(new RuntimeException("Unsupported destination"))

          }
      _ <- message.getJMSDestination.traverse_ {
            case queue: Queue => newMessage.setJMSDestination(new JmsQueue(queue))
            case topic: Topic => newMessage.setJMSDestination(new JmsTopic(topic))
            case _            => Failure(new RuntimeException("Unsupported destination"))

          }
      _ <- message.getJMSDeliveryMode.traverse_(newMessage.setJMSDeliveryMode)
      _ <- message.getJMSRedelivered.traverse_(newMessage.setJMSRedelivered)
      _ <- message.getJMSType.traverse_(newMessage.setJMSType)
      _ <- message.getJMSExpiration.traverse_(newMessage.setJMSExpiration)
      _ <- message.getJMSPriority.traverse_(newMessage.setJMSPriority)
      _ <- message.properties.traverse_ { props =>
            props.toList.traverse_ { case (k, v) => newMessage.setObjectProperty(k, v) }
          }
    } yield ()

}

object MessageFactory {
  def apply[F[_]](context: JmsContext[F]): MessageFactory[F] = new MessageFactory(context)
}
