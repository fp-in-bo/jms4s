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

package jms4s.basespec.providers

import cats.data.NonEmptyList
import cats.effect.{ Blocker, ContextShift, IO, Resource }
import jms4s.JmsClient
import jms4s.activemq.activeMQ
import jms4s.activemq.activeMQ._
import jms4s.basespec.Jms4sBaseSpec

import scala.util.Random

trait ActiveMQArtemisBaseSpec extends Jms4sBaseSpec {

  override def jmsClientRes(implicit cs: ContextShift[IO]): Resource[IO, JmsClient[IO]] =
    for {
      blocker <- Blocker.apply[IO]
      rnd     <- Resource.eval(IO(Random.nextInt))
      client <- activeMQ.makeJmsClient[IO](
                 Config(
                   endpoints = NonEmptyList.one(Endpoint("localhost", 61616)),
                   username = Some(Username("admin")),
                   password = Some(Password("passw0rd")),
                   clientId = ClientId("jms-specs" + rnd)
                 ),
                 blocker
               )
    } yield client
}
