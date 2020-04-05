package jms4s.jms

import cats.data.NonEmptyList
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ Blocker, IO, Resource }
import cats.implicits._
import jms4s.Jms4sBaseSpec
import jms4s.config._
import jms4s.ibmmq.ibmMQ
import jms4s.model.SessionType
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.duration._

class JmsSpec extends AsyncFreeSpec with AsyncIOSpec with Jms4sBaseSpec {

  val expectedBody = "body"
  "Basic jms ops" - {
    val queueRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      queue         <- Resource.liftF(session.createQueue(inputQueueName))
      queueConsumer <- session.createConsumer(queue)
      queueProducer <- session.createProducer(queue)
      msg           <- Resource.liftF(session.createTextMessage(expectedBody))
    } yield (queueConsumer, queueProducer, msg)

    val topicRes = for {
      connection    <- connectionRes
      session       <- connection.createSession(SessionType.AutoAcknowledge)
      topic         <- Resource.liftF(session.createTopic(topicName))
      topicConsumer <- session.createConsumer(topic)
      topicProducer <- session.createProducer(topic)
      msg           <- Resource.liftF(session.createTextMessage(expectedBody))
    } yield (topicConsumer, topicProducer, msg)

    "publish to a queue and then receive" in {
      queueRes.use {
        case (queueConsumer, queueProducer, msg) =>
          for {
            _    <- queueProducer.send(msg)
            text <- receiveBodyAsTextOrFail(queueConsumer)
          } yield assert(text == expectedBody)
      }
    }
    "publish and then receive with a delay" in {
      queueRes.use {
        case (consumer, producer, msg) =>
          for {
            _                 <- producer.setDeliveryDelay(delay)
            producerTimestamp <- IO(System.currentTimeMillis())
            _                 <- producer.send(msg)
            msg               <- consumer.receiveJmsMessage
            tm                <- msg.asJmsTextMessage
            body              <- tm.getText
            jmsDeliveryTime   <- tm.getJMSDeliveryTime
            producerDelay     <- IO(jmsDeliveryTime - producerTimestamp)
          } yield assert(producerDelay >= delay.toMillis && body == expectedBody)
      }
    }
    "publish to a topic and then receive" in {
      topicRes.use {
        case (topicConsumer, topicProducer, msg) =>
          for {
            _   <- (IO.delay(10.millis) >> topicProducer.send(msg)).start
            rec <- receiveBodyAsTextOrFail(topicConsumer)
          } yield assert(rec == expectedBody)
      }
    }
  }

  override def connectionRes: Resource[IO, JmsConnection[IO]] =
    Blocker
      .apply[IO]
      .flatMap(
        blocker =>
          ibmMQ.makeConnection[IO](
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
              //            password = Some(Password("admin")),
              clientId = "jms-specs"
            ),
            blocker
          )
      )
}
