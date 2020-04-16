package jms4s.activemq

import cats.data.NonEmptyList
import cats.effect.{ Blocker, Concurrent, ContextShift, Resource }
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import jms4s.activemq.configuration.{ Configuration, Endpoint }
import jms4s.jms.JmsContext
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import eu.timepit.refined.auto._

object activeMQ {

  def makeContext[F[_]: ContextShift: Logger: Concurrent](
    config: Configuration,
    blocker: Blocker
  ): Resource[F, JmsContext[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening context to MQ at ${hosts(config.endpoints)}...") *>
                    blocker.delay {
                      val factory = new ActiveMQConnectionFactory(hosts(config.endpoints))
                      factory.setClientID(config.clientId.value)

                      config.credentials.fold(factory.createContext())(cr =>
                        factory.createContext(cr.username.value.value, cr.password.value)
                      )
                    }
                )(c =>
                  Logger[F].info(s"Closing context $c to MQ at ${hosts(config.endpoints)}...") *>
                    blocker.delay(c.close()) *>
                    Logger[F].info(s"Closed context $c to MQ at ${hosts(config.endpoints)}.")
                )
      _ <- Resource.liftF(Logger[F].info(s"Opened context $context."))
    } yield new JmsContext[F](context, blocker)

  private def hosts(endpoints: NonEmptyList[Endpoint]): String =
    endpoints.map(e => s"tcp://${e.host}:${e.port}").toList.mkString(",")

}
