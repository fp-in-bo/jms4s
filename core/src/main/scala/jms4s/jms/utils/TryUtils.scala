/*
 * Copyright 2021 Alessandro Zoffoli
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
