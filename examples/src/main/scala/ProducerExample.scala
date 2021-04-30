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

import cats.data.NonEmptyList
import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsClient
import jms4s.config.{ DestinationName, TopicName }
import jms4s.jms.JmsMessage.JmsTextMessage
import jms4s.jms.MessageFactory

import scala.concurrent.duration.{ FiniteDuration, _ }

class ProducerExample extends IOApp {

  val jmsClient: Resource[IO, JmsClient[IO]] = null // see providers section!
  val outputTopic: TopicName                 = TopicName("YUOR.OUTPUT.TOPIC")
  val delay: FiniteDuration                  = 10.millis
  val messageStrings: NonEmptyList[String]   = NonEmptyList.fromListUnsafe((0 until 10).map(i => s"$i").toList)

  override def run(args: List[String]): IO[ExitCode] = {
    val producerRes = for {
      client   <- jmsClient
      producer <- client.createProducer(10)
    } yield producer

    producerRes.use { producer =>
      {
        for {
          _ <- producer.sendN(makeN(messageStrings, outputTopic))
          _ <- producer.sendNWithDelay(makeNWithDelay(messageStrings, outputTopic, delay))
          _ <- producer.send(make1(messageStrings.head, outputTopic))
          _ <- producer.sendWithDelay(make1WithDelay(messageStrings.head, outputTopic, delay))
        } yield ()
      }.as(ExitCode.Success)
    }
  }

  private def make1(
    text: String,
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[(JmsTextMessage, DestinationName)] = { mFactory =>
    mFactory
      .makeTextMessage(text)
      .map(message => (message, destinationName))
  }

  private def makeN(
    texts: NonEmptyList[String],
    destinationName: DestinationName
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage, DestinationName)]] = { mFactory =>
    texts.traverse { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, destinationName))
    }
  }

  private def make1WithDelay(
    text: String,
    destinationName: DestinationName,
    delay: FiniteDuration
  ): MessageFactory[IO] => IO[(JmsTextMessage, (DestinationName, Option[FiniteDuration]))] = { mFactory =>
    mFactory
      .makeTextMessage(text)
      .map(message => (message, (destinationName, Some(delay))))
  }

  private def makeNWithDelay(
    texts: NonEmptyList[String],
    destinationName: DestinationName,
    delay: FiniteDuration
  ): MessageFactory[IO] => IO[NonEmptyList[(JmsTextMessage, (DestinationName, Option[FiniteDuration]))]] = { mFactory =>
    texts.traverse { text =>
      mFactory
        .makeTextMessage(text)
        .map(message => (message, (destinationName, Some(delay))))
    }
  }
}
