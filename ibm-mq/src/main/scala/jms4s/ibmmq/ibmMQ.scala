package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource }
import cats.implicits._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import io.chrisdavenport.log4cats.Logger
import jms4s.ibmmq.configuration.{ Configuration, Endpoint }
import jms4s.jms.JmsContext

object ibmMQ {

  def makeContext[F[_]: Logger: ContextShift: Concurrent](
    config: Configuration,
    blocker: Blocker
  ): Resource[F, JmsContext[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening Context to MQ at ${hosts(config.endpoints)}...") >>
                    blocker.delay {
                      val connectionFactory: MQConnectionFactory = new MQConnectionFactory()
                      connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                      connectionFactory.setQueueManager(config.qm.value.value)
                      connectionFactory.setConnectionNameList(hosts(config.endpoints))
                      connectionFactory.setChannel(config.channel.value.value)
                      connectionFactory.setClientID(config.clientId.value.value)

                      config.credential.map { cr =>
                        connectionFactory.createContext(
                          cr.username.value.value,
                          cr.password.value
                        )
                      }.getOrElse(connectionFactory.createContext())
                    }
                )(c =>
                  Logger[F].info(s"Closing Context $c at ${hosts(config.endpoints)}...") *>
                    blocker.delay(c.close()) *>
                    Logger[F].info(s"Closed Context $c.")
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened Context $context at ${hosts(config.endpoints)}."))
    } yield new JmsContext[F](context, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
