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

package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource }
import cats.syntax.all._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import io.chrisdavenport.log4cats.Logger
import jms4s.JmsClient
import jms4s.jms.JmsContext

object ibmMQ {

  case class Config(
    qm: QueueManager,
    endpoints: NonEmptyList[Endpoint],
    channel: Channel,
    username: Option[Username] = None,
    password: Option[Password] = None,
    clientId: ClientId
  )
  case class Username(value: String) extends AnyVal
  case class Password(value: String) extends AnyVal
  case class Endpoint(host: String, port: Int)
  case class QueueManager(value: String) extends AnyVal
  case class Channel(value: String)      extends AnyVal
  case class ClientId(value: String)     extends AnyVal

  def makeJmsClient[F[_]: Logger: ContextShift: Concurrent](
    config: Config,
    blocker: Blocker
  ): Resource[F, JmsClient[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening Context to MQ at ${hosts(config.endpoints)}...") >>
                    blocker.delay {
                      val connectionFactory: MQConnectionFactory = new MQConnectionFactory()
                      connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                      connectionFactory.setQueueManager(config.qm.value)
                      connectionFactory.setConnectionNameList(hosts(config.endpoints))
                      connectionFactory.setChannel(config.channel.value)
                      connectionFactory.setClientID(config.clientId.value)

                      config.username.map { username =>
                        connectionFactory.createContext(
                          username.value,
                          config.password.map(_.value).getOrElse("")
                        )
                      }.getOrElse(connectionFactory.createContext())
                    }
                )(c =>
                  Logger[F].info(s"Closing Context $c at ${hosts(config.endpoints)}...") *>
                    blocker.delay(c.close()) *>
                    Logger[F].info(s"Closed Context $c.")
                )
      _ <- Resource.eval(Logger[F].info(s"Opened Context $context at ${hosts(config.endpoints)}."))
    } yield new JmsClient[F](new JmsContext[F](context, blocker))

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
