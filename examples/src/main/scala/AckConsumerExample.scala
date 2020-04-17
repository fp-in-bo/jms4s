import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsClient
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.JmsContext

class AckConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String): IO[AckAction[IO]] = IO.delay {
    if (text.toInt % 2 == 0)
      AckAction.send[IO](_.makeTextMessage("a brand new message").map(newMsg => (newMsg, outputTopic)))
    else if (text.toInt % 3 == 0)
      AckAction.noAck
    else
      AckAction.ack
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createAcknowledgerConsumer(jmsContext, inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { jmsMessage =>
      for {
        tm   <- jmsMessage.asJmsTextMessage
        text <- tm.getText
        res  <- yourBusinessLogic(text)
      } yield res
    }.as(ExitCode.Success))
  }
}
