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

package jms4s

import javax.jms.Session

object model {

  sealed abstract class SessionType(val rawAcknowledgeMode: Int) extends Product with Serializable

  object SessionType {

    case object Transacted extends SessionType(Session.SESSION_TRANSACTED)

    case object ClientAcknowledge extends SessionType(Session.CLIENT_ACKNOWLEDGE)

    case object AutoAcknowledge extends SessionType(Session.AUTO_ACKNOWLEDGE)

    case object DupsOkAcknowledge extends SessionType(Session.DUPS_OK_ACKNOWLEDGE)

  }
}
