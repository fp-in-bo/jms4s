package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Resource, Sync }
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.config.{ Config, Endpoint }
import jms4s.jms.JmsConnection
import org.apache.activemq.ActiveMQConnectionFactory
import cats.implicits._

object activeMQ {

  def makeConnection[F[_]: Sync: Logger](config: Config, blocker: Blocker): Resource[F, JmsConnection[F]] =
    for {
      connection <- Resource.make(
                     Logger[F].info(s"Opening Connection to MQ at ${hosts(config.endpoints)}...") *>
                       Sync[F].delay {
                         val connectionFactory: ActiveMQConnectionFactory =
                           new ActiveMQConnectionFactory("tcp://localhost:61616")
//            connectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
//            connectionFactory.set(config.qm.value)
//            connectionFactory.setConnectionNameList(hosts(config.endpoints))
//            connectionFactory.setChannel(config.channel.value)
                         connectionFactory.setClientID(config.clientId)

                         val connection: Connection = config.username.map { (username) =>
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
                       Logger[F].info(s"Closing Connection $c to MQ at ${hosts(config.endpoints)}...") *>
                         Sync[F].delay(c.close()) *>
                         Logger[F].info(s"Closed Connection $c to MQ at ${hosts(config.endpoints)}.")
                   )
      _ <- Resource.liftF(Logger[F].info(s"Opened connection $connection."))
    } yield new JmsConnection[F](connection, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
