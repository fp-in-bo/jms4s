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

import cats.effect.{ Async, IO, Resource }
import jms4s.JmsClient
import jms4s.basespec.Jms4sBaseSpec
import jms4s.config.QueueName
import jms4s.sqs.simpleQueueService
import jms4s.sqs.simpleQueueService._

trait ElasticMQBaseSpec extends Jms4sBaseSpec {

  override val inputQueueName: QueueName   = QueueName("DEV_QUEUE_1")
  override val outputQueueName1: QueueName = QueueName("DEV_QUEUE_2")
  override val outputQueueName2: QueueName = QueueName("DEV_QUEUE_3")

  override def jmsClientRes(implicit A: Async[IO]): Resource[IO, JmsClient[IO]] =
    simpleQueueService.makeJmsClient[IO](
      Config(
        endpoint = Endpoint(Some(DirectAddress(HTTP, "localhost", Some(9324))), "elasticmq"),
        credentials = Some(Credentials("x", "x")),
        clientId = ClientId("jms-specs"),
        None
      )
    )

}
