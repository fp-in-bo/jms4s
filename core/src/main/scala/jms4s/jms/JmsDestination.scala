package jms4s.jms

import javax.jms.{ Destination, Queue, Topic }

sealed abstract class JmsDestination {
  private[jms4s] val wrapped: Destination
}

object JmsDestination {
  class JmsQueue private[jms4s] (private[jms4s] val wrapped: Queue) extends JmsDestination
  class JmsTopic private[jms4s] (private[jms4s] val wrapped: Topic) extends JmsDestination
}
