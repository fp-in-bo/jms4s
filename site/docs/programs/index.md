---
layout: docs
title:  "Program"
position: 1
---

# Program

A program is a high level interface that will hide all the interaction with low level JMS apis.
This library is targetting JMS 2.0.

There are different types of programs for consuming and/or producing, each of them is parameterized on the effect type (eg. `IO`).

The data they consume/produce is kept as-is, thus the de/serialization is left to the user of the api (e.g. json, xml, text).

Currently only `javax.jms.TextMessage` is supported, but we designed the api in order to easily support new message types as soon as we see demand or contributions.

- **[JmsTransactedConsumer](./tx-consumer/)**: A consumer that supports local transactions, also producing messages to destinations.
- **[JmsAcknowledgerConsumer](./ack-consumer/)**: A consumer that leaves the choice to acknowledge (or not to) message consumption to the user, also producing messages to destinations.
- **[JmsAutoAcknowledgerConsumer](./auto-ack-consumer/)**: A consumer that acknowledges message consumption automatically, also producing messages to destinations.
- **[JmsProducer](./producer/)**: Producing messages to a destination.

## Concurrency with JMS?

The core of all JMS is the entity named [`JMSContext`](https://docs.oracle.com/javaee/7/api/javax/jms/JMSContext.html), introduced with JMS 2.0.

The only safe way to scale things a bit is to create a "root" context (which will open a physical connection) and from that creating multiple other contexts.

This library will keep a pool of contexts so that the user can scale up to pre-defined concurrency level.
