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

package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import jms4s.JmsClient
import jms4s.jms.JmsContext
import org.typelevel.log4cats.Logger

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

  def makeJmsClient[F[_]: Logger: Async](
    config: Config
  ): Resource[F, JmsClient[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening Context to MQ at ${hosts(config.endpoints)}...") >>
                    Sync[F].blocking {
                      val connectionFactory: MQConnectionFactory = new MQConnectionFactory()
                      connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                      connectionFactory.setQueueManager(config.qm.value)
                      connectionFactory.setConnectionNameList(hosts(config.endpoints))
                      connectionFactory.setChannel(config.channel.value)
                      connectionFactory.setClientID(config.clientId.value)

                      config.username.map { username =>
                        connectionFactory.createContext(
                          username.value,
                          config.password.map(_.value).orNull
                        )
                      }.getOrElse(connectionFactory.createContext())
                    }
                )(c =>
                  Logger[F].info(s"Closing Context $c at ${hosts(config.endpoints)}...") *>
                    Sync[F].blocking(c.close()) *>
                    Logger[F].info(s"Closed Context $c.")
                )
      _ <- Resource.eval(Logger[F].info(s"Opened Context $context at ${hosts(config.endpoints)}."))
    } yield new JmsClient[F](new JmsContext[F](context))

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
