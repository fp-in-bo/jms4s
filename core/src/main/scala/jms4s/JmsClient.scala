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

package jms4s

import cats.effect.{ Async, Resource }
import jms4s.config.{ Destination, DestinationName }
import jms4s.jms._
import jms4s.model.SessionType

import scala.concurrent.duration.FiniteDuration

class JmsClient[F[_]: Async] private[jms4s] (private[jms4s] val context: JmsContext[F])
    extends JmsDestinationFactory[F] {

  def createTransactedConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsTransactedConsumer[F]] =
    Resource
      .eval(context.createDestination(inputDestinationName))
      .flatMap(createTransactedConsumer(_, concurrencyLevel, pollingInterval))

  def createAutoAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    Resource
      .eval(context.createDestination(inputDestinationName))
      .flatMap(createAutoAcknowledgerConsumer(_, concurrencyLevel, pollingInterval))

  def createAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    Resource
      .eval(context.createDestination(inputDestinationName))
      .flatMap(createAcknowledgerConsumer(_, concurrencyLevel, pollingInterval))

  def createProducer(
    concurrencyLevel: Int
  ): Resource[F, JmsProducer[F]] =
    JmsProducer.make[F](context, concurrencyLevel)

  def createTransactedConsumer(
    inputJmsDestination: JmsDestination,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsTransactedConsumer[F]] =
    PooledConsumer
      .make[F](context, inputJmsDestination, concurrencyLevel, pollingInterval, SessionType.Transacted)
      .map(JmsTransactedConsumer.make(_))

  def createAutoAcknowledgerConsumer(
    inputJmsDestination: JmsDestination,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    PooledConsumer
      .make[F](context, inputJmsDestination, concurrencyLevel, pollingInterval, SessionType.AutoAcknowledge)
      .map(JmsAutoAcknowledgerConsumer.make(_))

  def createAcknowledgerConsumer(
    inputJmsDestination: JmsDestination,
    concurrencyLevel: Int,
    pollingInterval: FiniteDuration
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    PooledConsumer
      .make[F](context, inputJmsDestination, concurrencyLevel, pollingInterval, SessionType.ClientAcknowledge)
      .map(JmsAcknowledgerConsumer.make(_))

  override def createDestination(destination: Destination): F[JmsDestination] = context.createDestination(destination)
}
