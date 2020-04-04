package jms4s

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.config.{ Config, Endpoint }
import jms4s.jms.JmsConnection
import org.apache.activemq.ActiveMQConnectionFactory

object activeMQ {

  def makeConnection[F[_]: Sync: Logger](config: Config, blocker: Blocker): Resource[F, JmsConnection[F]] =
    for {
      connection <- Resource.make(
                     Logger[F].info(s"Opening QueueConnection to MQ at ${hosts(config.endpoints)}...") >>
                       Sync[F].delay {
                         val queueConnectionFactory: ActiveMQConnectionFactory =
                           new ActiveMQConnectionFactory("tcp://localhost:61616")
//            queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
//            queueConnectionFactory.set(config.qm.value)
//            queueConnectionFactory.setConnectionNameList(hosts(config.endpoints))
//            queueConnectionFactory.setChannel(config.channel.value)
                         queueConnectionFactory.setClientID(config.clientId)

                         val connection: Connection = config.username.map { (username) =>
                           queueConnectionFactory.createConnection(
                             username.value,
                             config.password.map(_.value).getOrElse("")
                           )
                         }.getOrElse(queueConnectionFactory.createConnection)

                         connection.start()
                         connection
                       }
                   )(c => Sync[F].delay(c.close()))
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueConnection $connection."))
    } yield new JmsConnection[F](connection, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
