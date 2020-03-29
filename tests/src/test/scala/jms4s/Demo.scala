package jms4s

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{ Blocker, ExitCode, IO, IOApp, Resource }
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import jms4s.config.{ Channel, Config, Endpoint, Password, QueueManager, QueueName, TopicName, Username }
import jms4s.ibmmq.ibmMQ.makeConnection
import jms4s.model.SessionType

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

object Demo extends IOApp {
  val nMessages: Int              = 100
  val poolSize: Int               = 10
  val timeout: FiniteDuration     = 2.seconds
  val delay: FiniteDuration       = 10.seconds
  val topicName: TopicName        = TopicName("DEV.BASE.TOPIC/dev/pippo")
  val inputQueueName: QueueName   = QueueName("DEV.QUEUE.1")
  val outputQueueName1: QueueName = QueueName("DEV.QUEUE.2")
  val outputQueueName2: QueueName = QueueName("DEV.QUEUE.3")

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val blocker: Resource[IO, Blocker]                 = Blocker.apply

  val connectionRes: Resource[IO, JmsConnection[IO]] = blocker.flatMap(
    blocker =>
      makeConnection[IO](
        Config(
          qm = QueueManager("QM1"),
          endpoints = NonEmptyList.one(Endpoint("localhost", 1414)),
          // the current docker image seems to be misconfigured, so I need to use admin channel/auth in order to test topic
          // but maybe it's just me not understanding something properly.. as usual
          //          channel = Channel("DEV.APP.SVRCONN"),
          //          username = Some(Username("app")),
          //          password = None,
          channel = Channel("DEV.ADMIN.SVRCONN"),
          username = Some(Username("admin")),
          password = Some(Password("passw0rd")),
          clientId = "jms-specs"
        ),
        blocker
      )
  )

  override def run(args: List[String]): IO[ExitCode] = {

    val res = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      queue         <- Resource.liftF(session.createQueue(inputQueueName))
      topic         <- Resource.liftF(session.createTopic(topicName))
      queueConsumer <- session.createConsumer(queue)
      topicConsumer <- session.createConsumer(topic)
      queueProducer <- session.createProducer(queue)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage("body"))
    } yield (queueConsumer, topicConsumer, queueProducer, topicProducer, msg)

    res.use {
      case (_, topicConsumer, _, _, _) =>
        for {
          received <- Ref.of[IO, Set[String]](Set())
          consumerFiber <- topicConsumer.receiveJmsMessage
                            .flatMap(
                              _.asJmsTextMessage.flatMap(
                                _.getText.flatMap(body => received.update(_ + body))
                              )
                            )
                            .start
          _ <- consumerFiber.join
          x <- received.get
          _ <- logger.info("Rec: " + x)
//          _ <- topicProducer.send(msg)
//          _ <- logger.info("Message sent.")
//          receivedMessages <- (received.get.iterateUntil(_.size == 1).timeout(timeout) >> received.get)
//            .guarantee(consumerFiber.cancel)
        } yield ExitCode.Success
    }

  }
}
