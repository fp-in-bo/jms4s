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

package jms4s.activemq

import cats.data.NonEmptyList
import cats.effect.{ Concurrent, Resource }
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import jms4s.JmsClient
import jms4s.jms.JmsContext
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import cats.effect.Sync

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

  def makeJmsClient[F[_]: ContextShift: Logger: Concurrent](
    config: Config): Resource[F, JmsClient[F]] =
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
    } yield new JmsClient[F](new JmsContext[F](context, blocker))

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"tcp://${e.host}:${e.port}").toList.mkString(",")

}
