package fs2jms

import javax.jms.MessageProducer

class JmsMessageProducer(private[fs2jms] val value: MessageProducer) {}
