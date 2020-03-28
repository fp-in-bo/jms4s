# jms4s - fs2/cats-effects wrapper for jms [![Build Status](https://travis-ci.com/al333z/jms4s.svg?branch=master)](https://travis-ci.com/al333z/jms4s) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.al333z/jms4s_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.al333z/jms4s_2.12) ![Code of Consuct](https://img.shields.io/badge/Code%20of%20Conduct-Scala-blue.svg)

This is a WIP.

## Roadmap

Support at the very least the following features:

- Support `TextMessage`

- Consuming, returning a never-ending cancellable program that concurrently consumes from a queue (an `IO`)
  - [x] createQueueTransactedConsumer
  - [ ] createQueueAckConsumer
  - [ ] createQueueAutoAckConsumer

- Publishing, returning a component which can publish messages
  - [ ] createQueuePublisher
  - [ ] createTopicPublisher

- [x] Consuming and Publishing within the same transaction

## [Head on over to the microsite](https://al333z.github.io/jms4s)

## Quick Start

To use jms4s in an existing SBT project with Scala 2.11 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
libraryDependencies ++= Seq(
  "com.al333z" %% "jms4s" % "<version>"
)
```
