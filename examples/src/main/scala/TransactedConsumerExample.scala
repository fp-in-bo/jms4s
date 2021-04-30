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

import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsClient
import jms4s.JmsTransactedConsumer._
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.MessageFactory

class TransactedConsumerExample extends IOApp {

  val jmsClient: Resource[IO, JmsClient[IO]] = null // see providers section!
  val inputQueue: QueueName                  = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                 = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String, mf: MessageFactory[IO]): IO[TransactionAction[IO]] =
    if (text.toInt % 2 == 0)
      mf.makeTextMessage("a brand new message").map(newMsg => TransactionAction.send(newMsg, outputTopic))
    else if (text.toInt % 3 == 0)
      IO.pure(TransactionAction.rollback)
    else IO.pure(TransactionAction.commit)

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      client   <- jmsClient
      consumer <- client.createTransactedConsumer(inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text, mf)
      } yield res
    }.as(ExitCode.Success))
  }
}
