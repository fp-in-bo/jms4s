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

package jms4s.activemq

import cats.data.NonEmptyList
import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import jms4s.JmsClient
import jms4s.jms.JmsContext
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.typelevel.log4cats.Logger

object activeMQ {

  case class Config(
    endpoints: NonEmptyList[Endpoint],
    username: Option[Username] = None,
    password: Option[Password] = None,
    clientId: ClientId
  )
  case class Username(value: String) extends AnyVal
  case class Password(value: String) extends AnyVal
  case class Endpoint(host: String, port: Int)
  case class ClientId(value: String) extends AnyVal

  def makeJmsClient[F[_]: Async: Logger](
    config: Config
  ): Resource[F, JmsClient[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening context to MQ at ${hosts(config.endpoints)}...") *>
                    Sync[F].blocking {
                      val factory = new ActiveMQConnectionFactory(hosts(config.endpoints))
                      factory.setClientID(config.clientId.value)

                      config.username.fold(factory.createContext())(username =>
                        factory.createContext(username.value, config.password.map(_.value).getOrElse(""))
                      )
                    }
                )(c =>
                  Logger[F].info(s"Closing context $c to MQ at ${hosts(config.endpoints)}...") *>
                    Sync[F].blocking(c.close()) *>
                    Logger[F].info(s"Closed context $c to MQ at ${hosts(config.endpoints)}.")
                )
      _ <- Resource.eval(Logger[F].info(s"Opened context $context."))
    } yield new JmsClient[F](new JmsContext[F](context))

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"tcp://${e.host}:${e.port}").toList.mkString(",")

}
