package jms4s.jms.utils

import scala.util.{ Success, Try }

object TryUtils {

  implicit class TryUtils[T](val underlying: Try[Option[T]]) {

    def toOpt: Option[T] =
      underlying match {
        case Success(t) => {
          t match {
            case Some(null) => None
            case None       => None
            case x          => x
          }
        }
        case _ => None
      }
  }
}
