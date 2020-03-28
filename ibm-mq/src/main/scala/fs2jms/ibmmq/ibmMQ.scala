package fs2jms.ibmmq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Resource, Sync }
import cats.implicits._
import com.ibm.mq.jms.{ MQConnectionFactory, MQQueueConnectionFactory }
import com.ibm.msg.client.wmq.common.CommonConstants
import fs2jms.{ JmsConnection, JmsQueueConnection }
import fs2jms.config.{ Config, Endpoint }
import io.chrisdavenport.log4cats.Logger

object ibmMQ {

  def makeConnectionOld[F[_]: Sync: Logger](jms: Config, blocker: Blocker): Resource[F, JmsQueueConnection[F]] =
    for {
      connection <- Resource.fromAutoCloseable(
                     Logger[F].info(s"Opening QueueConnection to MQ at ${hosts(jms.endpoints)}...") >>
                       Sync[F].delay {
                         val queueConnectionFactory: MQQueueConnectionFactory = new MQQueueConnectionFactory()
                         queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                         queueConnectionFactory.setQueueManager(jms.qm.value)
                         queueConnectionFactory.setConnectionNameList(hosts(jms.endpoints))
                         queueConnectionFactory.setChannel(jms.channel.value)

                         val connection = jms.username.map { (username) =>
                           queueConnectionFactory.createQueueConnection(
                             username.value,
                             jms.password.map(_.value).getOrElse("")
                           )
                         }.getOrElse(queueConnectionFactory.createQueueConnection)

                         connection.start()
                         connection
                       }
                   )
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueConnection $connection."))
    } yield new JmsQueueConnection[F](connection, blocker)

  def makeConnection[F[_]: Sync: Logger](jms: Config, blocker: Blocker): Resource[F, JmsConnection[F]] =
    for {
      connection <- Resource.fromAutoCloseable(
                     Logger[F].info(s"Opening QueueConnection to MQ at ${hosts(jms.endpoints)}...") >>
                       Sync[F].delay {
                         val queueConnectionFactory: MQConnectionFactory = new MQConnectionFactory()
                         queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                         queueConnectionFactory.setQueueManager(jms.qm.value)
                         queueConnectionFactory.setConnectionNameList(hosts(jms.endpoints))
                         queueConnectionFactory.setChannel(jms.channel.value)

                         val connection = jms.username.map { (username) =>
                           queueConnectionFactory.createConnection(
                             username.value,
                             jms.password.map(_.value).getOrElse("")
                           )
                         }.getOrElse(queueConnectionFactory.createConnection)

                         connection.start()
                         connection
                       }
                   )
      _ <- Resource.liftF(Logger[F].info(s"Opened QueueConnection $connection."))
    } yield new JmsConnection[F](connection, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"${e.host}(${e.port})").toList.mkString(",")

}
