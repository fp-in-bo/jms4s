package jms4s.activemq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import javax.jms.Connection
import jms4s.jms.JmsConnection
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory

object activeMQ {

  case class Config(
    endpoints: NonEmptyList[Endpoint],
    username: Option[Username] = None,
    password: Option[Password] = None,
    clientId: String
  )
  case class Username(value: String) extends AnyVal
  case class Password(value: String) extends AnyVal
  case class Endpoint(host: String, port: Int)

  def makeConnection[F[_]: Sync: Logger](config: Config, blocker: Blocker): Resource[F, JmsConnection[F]] =
    for {
      connection <- Resource.make(
                     Logger[F].info(s"Opening Connection to MQ at ${hosts(config.endpoints)}...") *>
                       Sync[F].delay {
                         val factory = new ActiveMQConnectionFactory(hosts(config.endpoints))
                         factory.setClientID(config.clientId)

                         val connection = config.username.fold(factory.createConnection)(
                           username =>
                             factory.createConnection(username.value, config.password.map(_.value).getOrElse(""))
                         )

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
    endpoints.map(e => s"tcp://${e.host}:${e.port}").toList.mkString(",")

}
