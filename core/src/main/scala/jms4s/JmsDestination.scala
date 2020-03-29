package jms4s

import javax.jms.{ Destination, Queue, Topic }

sealed abstract class JmsDestination {
  private[jms4s] val wrapped: Destination
}

class JmsQueue private[jms4s] (private[jms4s] val wrapped: Queue) extends JmsDestination

class JmsTopic private[jms4s] (private[jms4s] val wrapped: Topic) extends JmsDestination
