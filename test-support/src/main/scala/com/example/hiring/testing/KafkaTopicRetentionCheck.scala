package com.example.hiring.testing

import cats.effect.{IO, IOApp}
import cats.syntax.all.*

/** Real physical retention on the exact command and result topics of one isolated Kafka namespace. */
object KafkaTopicRetentionCheck extends IOApp.Simple {
  def run: IO[Unit] = KafkaTestNamespace.resource.use { optional =>
    IO.fromOption(optional)(new IllegalArgumentException("An isolated test service manifest is required")).flatMap {
      namespace =>
        KafkaShortRetention.probe(namespace.manifest).use { probe =>
          Vector(namespace.commands, namespace.results).traverse_ { topic =>
            probe.expireCurrentRecords(topic).flatMap { case (barrier, beginning) =>
              IO.println(
                s"Physical Kafka retention PASS topic=$topic partition=0 barrier=$barrier beginning=$beginning"
              )
            }
          }
        }
    }
  }
}
