import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsAutoAcknowledgerConsumer.AutoAckAction
import jms4s.JmsClient
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.JmsContext

class AutoAckConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String): IO[AutoAckAction[IO]] = IO.delay {
    if (text.toInt % 2 == 0)
      AutoAckAction.send[IO](_.makeTextMessage("a brand new message").map(newMsg => (newMsg, outputTopic)))
    else
      AutoAckAction.noOp
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createAutoAcknowledgerConsumer(jmsContext, inputQueue, 10)
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
