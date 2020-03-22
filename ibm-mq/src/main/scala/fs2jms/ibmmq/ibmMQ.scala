package fs2jms.ibmmq

import cats.effect.{ IO, Resource }
import com.ibm.mq.jms.MQQueueConnectionFactory
import com.ibm.msg.client.wmq.common.CommonConstants
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import javax.jms.QueueConnection
import fs2jms.config.{ Config, Endpoint }
import cats.implicits._

object ibmMQ {
  private val logger = Slf4jLogger.getLogger[IO]

  def makeConnection(jms: Config): Resource[IO, QueueConnection] =
    for {
      connection <- Resource.fromAutoCloseable[IO, QueueConnection](
                     logger.info(s"Opening QueueConnection to MQ at ${hosts(jms.endpoints)}...") *>
                       IO.delay {
                         val queueConnectionFactory: MQQueueConnectionFactory = new MQQueueConnectionFactory()
                         queueConnectionFactory.setTransportType(CommonConstants.WMQ_CM_CLIENT)
                         queueConnectionFactory.setQueueManager(jms.qm.value)
                         queueConnectionFactory.setConnectionNameList(hosts(jms.endpoints))
                         queueConnectionFactory.setChannel(jms.channel.value)

                         val connection = (jms.username, jms.password).mapN { (username, password) =>
                           queueConnectionFactory.createQueueConnection(username.value, password.value)
                         }.getOrElse(queueConnectionFactory.createQueueConnection)

                         connection.start()
                         connection
                       }
                   )
      _ <- Resource.liftF(logger.info(s"Opened QueueConnection $connection."))
    } yield connection

  private def hosts(endpoints: List[Endpoint]): String = endpoints.map(e => s"${e.host}(${e.port})").mkString(",")

}
