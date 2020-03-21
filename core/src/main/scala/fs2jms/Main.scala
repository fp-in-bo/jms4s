package fs2jms

import cats.effect.{ ExitCode, IO, IOApp }
import cats.implicits._

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    IO(println("I am a new project!")).as(ExitCode.Success)

}
