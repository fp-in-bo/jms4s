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

package jms4s

import cats.effect.{ Concurrent, Resource }
import jms4s.config.DestinationName
import jms4s.jms._

class JmsClient[F[_]: ContextShift: Concurrent] private[jms4s] (private[jms4s] val context: JmsContext[F]) {

  def createTransactedConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsTransactedConsumer[F]] =
    JmsTransactedConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createAutoAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAutoAcknowledgerConsumer[F]] =
    JmsAutoAcknowledgerConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createAcknowledgerConsumer(
    inputDestinationName: DestinationName,
    concurrencyLevel: Int
  ): Resource[F, JmsAcknowledgerConsumer[F]] =
    JmsAcknowledgerConsumer.make(context, inputDestinationName, concurrencyLevel)

  def createProducer(
    concurrencyLevel: Int
  ): Resource[F, JmsProducer[F]] =
    JmsProducer.make(context, concurrencyLevel)

}
