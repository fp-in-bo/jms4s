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

import cats.effect.{ Blocker, ContextShift, Sync }
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import javax.jms.JMSConsumer

class JmsMessageConsumer[F[_]: ContextShift: Sync: Logger] private[jms4s] (
  private[jms4s] val wrapped: JMSConsumer,
  private[jms4s] val blocker: Blocker
) {

  val receiveJmsMessage: F[JmsMessage] =
    for {
      recOpt <- blocker.delay(Option(wrapped.receiveNoWait()))
      rec <- recOpt match {
              case Some(message) => Sync[F].pure(new JmsMessage(message))
              case None          => ContextShift[F].shift >> receiveJmsMessage
            }
    } yield rec
}
