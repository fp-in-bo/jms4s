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
import jms4s.jms.JmsMessage.JmsTextMessage

import scala.util.Try

class MessageFactory[F[_]](private val context: JmsContext[F]) extends AnyVal {
  def makeTextMessage(value: String): F[JmsTextMessage] = context.createTextMessage(value)

  def cloneMessageF(original: JmsTextMessage)(implicit mt: MonadThrow[F]): F[JmsTextMessage] =
    attemptCloneMessage(original).flatMap(_.liftTo[F])

  def attemptCloneMessage(original: JmsTextMessage)(implicit a: Applicative[F]): F[Either[Throwable, JmsTextMessage]] =
    original.getText
      .traverse(makeTextMessage)
      .map {
        _.flatMap(copied => copyMessageHeaders(original, copied).as(copied))
      }
      .map(_.toEither)

  private def copyMessageHeaders(from: JmsMessage, to: JmsMessage): Try[Unit] =
    (
      from.getJMSMessageId.traverse_(to.setJMSMessageID),
      from.getJMSTimestamp.traverse_(to.setJMSTimestamp),
      from.getJMSCorrelationId.traverse_(to.setJMSCorrelationId),
      from.getJMSReplyTo.traverse_(d => to.setJMSReplyTo(JmsDestination.fromDestination(d))),
      from.getJMSDestination.traverse_(d => to.setJMSDestination(JmsDestination.fromDestination(d))),
      from.getJMSDeliveryMode.traverse_(to.setJMSDeliveryMode),
      from.getJMSRedelivered.traverse_(to.setJMSRedelivered),
      from.getJMSType.traverse_(to.setJMSType),
      from.getJMSExpiration.traverse_(to.setJMSExpiration),
      from.getJMSPriority.traverse_(to.setJMSPriority),
      from.properties.traverse_(props => props.toList.traverse_ { case (k, v) => to.setObjectProperty(k, v) })
    ).combineAll

}

object MessageFactory {
  def apply[F[_]](context: JmsContext[F]): MessageFactory[F] = new MessageFactory(context)
}
