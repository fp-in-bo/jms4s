import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsAcknowledgerConsumer.AckAction
import jms4s.JmsClient
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.{ JmsContext, MessageFactory }

class AckConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String, mf: MessageFactory[IO]): IO[AckAction[IO]] =
    if (text.toInt % 2 == 0)
      mf.makeTextMessage("a brand new message").map(newMsg => AckAction.send(newMsg, outputTopic))
    else if (text.toInt % 3 == 0)
      IO.pure(AckAction.noAck)
    else
      IO.pure(AckAction.ack)

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createAcknowledgerConsumer(jmsContext, inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text, mf)
      } yield res
    }.as(ExitCode.Success))
  }
}
