package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.repository.mongo.KafkaIntegrationSuite
import com.example.hiring.testing.KafkaShortRetention
import scala.concurrent.duration.*

/** DDC-02.9: the readable-age bound `retention.ms + segment.ms` holds on a topic that receives no further appends. The
  * broker deletes the active segment once its newest record is older than `retention.ms`, so a quiet partition does not
  * keep its last records beyond the bound the receipt window checks.
  */
class KafkaQuietTopicRetentionIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout = 2.minutes

  test("DDC-02.9 a quiet topic deletes its active segment by retention without a later append") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    val topic = kafkaNamespace.commands
    KafkaShortRetention.probe(kafkaNamespace.manifest).use { probe =>
      for {
        _ <- probe.shortenRetentionOnly(topic)
        _ <- probe.send(topic, "quiet-1") *> probe.send(topic, "quiet-2")
        barrier <- probe.latest(topic)
        beginning <- probe.awaitEarliestWithoutAppends(topic, barrier)
        latestAfter <- probe.latest(topic)
      } yield {
        assertEquals(latestAfter, barrier, "the probe appended nothing after the barrier")
        assert(beginning.exists(_ >= barrier), s"records stayed readable on a quiet topic: earliest=$beginning")
      }
    }
  }
}
