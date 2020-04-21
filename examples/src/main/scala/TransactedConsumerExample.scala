import cats.effect.{ ExitCode, IO, IOApp, Resource }
import jms4s.JmsClient
import jms4s.JmsTransactedConsumer._
import jms4s.config.{ QueueName, TopicName }
import jms4s.jms.{ JmsContext, MessageFactory }

class TransactedConsumerExample extends IOApp {

  val contextRes: Resource[IO, JmsContext[IO]] = null // see providers section!
  val jmsClient: JmsClient[IO]                 = new JmsClient[IO]
  val inputQueue: QueueName                    = QueueName("YUOR.INPUT.QUEUE")
  val outputTopic: TopicName                   = TopicName("YUOR.OUTPUT.TOPIC")

  def yourBusinessLogic(text: String, mf: MessageFactory[IO]): IO[TransactionAction[IO]] =
    if (text.toInt % 2 == 0)
      mf.makeTextMessage("a brand new message").map(newMsg => TransactionAction.send(newMsg, outputTopic))
    else if (text.toInt % 3 == 0)
      IO.pure(TransactionAction.rollback)
    else IO.pure(TransactionAction.commit)

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerRes = for {
      jmsContext <- contextRes
      consumer   <- jmsClient.createTransactedConsumer(jmsContext, inputQueue, 10)
    } yield consumer

    consumerRes.use(_.handle { (jmsMessage, mf) =>
      for {
        text <- jmsMessage.asTextF[IO]
        res  <- yourBusinessLogic(text, mf)
      } yield res
    }.as(ExitCode.Success))
  }
}
