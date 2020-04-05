package jms4s.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import com.ibm.mq.jms.MQConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import io.chrisdavenport.log4cats.Logger
import jms4s.config.{ Config, Endpoint }
import jms4s.jms.JmsConnection

object ibmMQ {

  def makeConnection[F[_]: Sync: Logger](config: Config, blocker: Blocker): Resource[F, JmsConnection[F]] =
    for {
      connection <- Resource.make(
                     Logger[F].info(s"Opening Connection to MQ at ${hosts(config.endpoints)}...") >>
                       Sync[F].delay {
                         val connectionFactory: MQConnectionFactory = new MQConnectionFactory()
                         connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                         connectionFactory.setQueueManager(config.qm.value)
                         connectionFactory.setConnectionNameList(hosts(config.endpoints))
                         connectionFactory.setChannel(config.channel.value)
                         connectionFactory.setClientID(config.clientId)

                         val connection = config.username.map { (username) =>
                           connectionFactory.createConnection(
                             username.value,
                             config.password.map(_.value).getOrElse("")
                           )
                         }.getOrElse(connectionFactory.createConnection)

                         connection.start()
                         connection
                       }
                   )(
                     c =>
                       Logger[F].info(s"Closing Connection $c at ${hosts(config.endpoints)}...") *>
                         Sync[F].delay(c.close()) *>
                         Logger[F].info(s"Closed Connection $c.")
                   )
      _ <- Resource.liftF(Logger[F].info(s"Opened Connection $connection at ${hosts(config.endpoints)}."))
    } yield new JmsConnection[F](connection, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
