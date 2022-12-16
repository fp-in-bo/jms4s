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

package jms4s.sqs

import cats.effect.{ Async, Resource, Sync }
import cats.syntax.all._
import com.amazon.sqs.javamessaging.{ ProviderConfiguration, SQSConnectionFactory }
import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  AwsBasicCredentials,
  AwsCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.services.sqs.endpoints.{ SqsEndpointParams, SqsEndpointProvider }
import software.amazon.awssdk.services.sqs.SqsClient
import jms4s.JmsClient
import jms4s.jms.JmsContext
import jms4s.jms.utils.SharedConnection
import org.typelevel.log4cats.Logger
import software.amazon.awssdk.endpoints

import java.net.URI
import java.util.concurrent.CompletableFuture
import javax.jms.Session

object simpleQueueService {

  case class Config(
    endpoint: Endpoint,
    credentials: Option[Credentials] = None,
    clientId: ClientId,
    numberOfMessagesToPrefetch: Option[Int]
  )
  case class Credentials(accessKey: String, secretKey: String)
  sealed trait Protocol
  case object HTTP  extends Protocol
  case object HTTPS extends Protocol
  case class DirectAddress(protocol: Protocol, host: String, port: Option[Int])

  case class Endpoint(directAddress: Option[DirectAddress], signingRegion: String) {
    override def toString(): String = s"Region: $signingRegion${directAddress.map(" " + _.toString).getOrElse("")}"
  }
  case class ClientId(value: String) extends AnyVal

  def makeJmsClient[F[_]: Async: Logger](config: Config): Resource[F, JmsClient[F]] =
    for {
      context <- Resource.make(
                  Logger[F].info(s"Opening context to MQ at ${config.endpoint}...") *>
                    Sync[F].blocking {
                      val providerConfiguration = new ProviderConfiguration()
                      config.numberOfMessagesToPrefetch.map(providerConfiguration.setNumberOfMessagesToPrefetch(_))

                      // TODO(AR) set the region from config.endpoint.signingRegion
                      val endpointProvider = config.endpoint.directAddress.fold[SqsEndpointProvider](
                        SqsEndpointProvider.defaultProvider()
                      )(DirectAddressEndpointProvider.directAddress(_))

                      val credentialsProvider =
                        config.credentials.fold[AwsCredentialsProvider](AnonymousCredentialsProvider.create())(creds =>
                          StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.accessKey, creds.secretKey))
                        )

                      val sqsClientBuilder = SqsClient
                        .builder()
                        .endpointProvider(endpointProvider)
                        .credentialsProvider(credentialsProvider)

                      val factory = new SQSConnectionFactory(
                        providerConfiguration,
                        sqsClientBuilder
                      )

                      val connection = factory.createConnection()
                      connection.setClientID(config.clientId.value)

                      // NOTE: start the connection before sharing it, it will be stopped in SharedConnection.close()
                      connection.start()

                      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

                      Tuple2(new SharedConnection(connection), session)
                    }
                ) {
                  case (sharedConnection, session) =>
                    Logger[F].info(s"Closing context $session to MQ at ${config.endpoint}...") *>
                      Sync[F].blocking { session.close(); sharedConnection.close() } *>
                      Logger[F].info(s"Closed context $session to MQ at ${config.endpoint}.")
                }
      _ <- Resource.eval(Logger[F].info(s"Opened context ${context._2}"))
    } yield new JmsClient[F](new JmsContext[F](context._1, context._2))

  private class DirectAddressEndpointProvider(directAddress: DirectAddress) extends SqsEndpointProvider {

    override def resolveEndpoint(
      endpointParams: SqsEndpointParams
    ): CompletableFuture[endpoints.Endpoint] = {
      val endpoint = endpoints.Endpoint
        .builder()
        .url(new URI(asUrl()))
        .build()
      CompletableFuture.completedFuture(endpoint)
    }

    private def asUrl(): String =
      s"${directAddress.protocol.toString.toLowerCase}://${directAddress.host}${directAddress.port.fold("")(port => s":$port")}"
  }

  private object DirectAddressEndpointProvider {
    def directAddress(directAddress: DirectAddress) = new DirectAddressEndpointProvider(directAddress)
  }

}
