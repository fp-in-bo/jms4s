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

package jms4s.jms

import javax.jms.{ Destination, Queue, Topic }

sealed abstract class JmsDestination {
  private[jms4s] val wrapped: Destination
}

object JmsDestination {
  class JmsQueue private[jms4s] (private[jms4s] val wrapped: Queue) extends JmsDestination
  class JmsTopic private[jms4s] (private[jms4s] val wrapped: Topic) extends JmsDestination
}
