package jms4s

import cats.effect.implicits._
import cats.effect.{ Concurrent, ContextShift, Sync }
import cats.implicits._
import javax.jms.JMSException

import scala.util.control.NonFatal

object IOOps {

  // adapted from https://gist.github.com/djspiewak/d587d309930e65549430898a16f82749
  def interruptable[A, F[_]: Concurrent: ContextShift](force: Boolean)(thunk: => A): F[A] = {
    val fa: F[A] = Concurrent[F].cancelable { cb =>
      val t = new Thread(() => cb(Right(thunk)))
      t.setDaemon(true)
      t.setName("interruptable-effect")

      t setUncaughtExceptionHandler {
        case (_, _: JMSException) =>
          ()

        case (_, NonFatal(e)) =>
          cb(Left(e))

        case (_, _: InterruptedException) =>
          ()

        case (_, e) =>
          e.printStackTrace()
          sys.exit(-1)
      }

      t.start()

      val interruptF = Sync[F].delay(t.interrupt())
      val aliveF     = Sync[F].delay(t.isAlive)

      // we need to busy-wait, but ensure thread yields between attempts
      lazy val loopF: F[Unit] =
        aliveF.ifM((if (force) interruptF else ().pure[F]) >> ContextShift[F].shift >> loopF, ().pure[F])

      aliveF.ifM(interruptF >> ContextShift[F].shift >> loopF, ().pure[F])
    }

    fa.guarantee(ContextShift[F].shift)
  }
}
