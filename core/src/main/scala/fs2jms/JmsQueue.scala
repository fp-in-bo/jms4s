package fs2jms

import javax.jms.{ Destination, Queue, Topic }

sealed abstract class JmsDestination {
  private[fs2jms] val wrapped: Destination
}

class JmsQueue private[fs2jms] (private[fs2jms] val wrapped: Queue) extends JmsDestination

class JmsTopic private[fs2jms] (private[fs2jms] val wrapped: Topic) extends JmsDestination
