package com.example.hiring.analytics
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import munit.FunSuite

import java.nio.file.Path

final class HmacKeyRetirementSecuritySpec extends FunSuite {
  test("running bind mounts of the protected source or its ancestors and descendants are detected") {
    val source = Path.of("/proof/data/rotation")
    assert(LocalHmacKeyWriterExclusion.mountIntersectsProtectedSource(source, source))
    assert(LocalHmacKeyWriterExclusion.mountIntersectsProtectedSource(Path.of("/proof/data"), source))
    assert(
      LocalHmacKeyWriterExclusion.mountIntersectsProtectedSource(Path.of("/proof/data/rotation/lakehouse"), source)
    )
    assert(!LocalHmacKeyWriterExclusion.mountIntersectsProtectedSource(Path.of("/proof/data/other"), source))
  }

  test("Kafka lineage rejects a recreated broker volume, cluster, or topic") {
    val original = HmacKeyRetirementKafkaLineage(
      "cluster-a",
      "topic-a",
      "rotation_hmac-rotation-kafka",
      "/var/lib/docker/volumes/rotation_hmac-rotation-kafka/_data",
      "2026-09-27T12:00:00Z",
      "127.0.0.1:19093"
    )
    assert(HmacKeyRetirementKafkaLineage.matches(original, original))
    assert(!HmacKeyRetirementKafkaLineage.matches(original, original.copy(clusterId = "cluster-b")))
    assert(!HmacKeyRetirementKafkaLineage.matches(original, original.copy(topicId = "topic-b")))
    assert(!HmacKeyRetirementKafkaLineage.matches(original, original.copy(volumeCreatedAt = "2026-09-28T12:00:00Z")))
    assert(!HmacKeyRetirementKafkaLineage.matches(original, original.copy(volumeName = "other_hmac-rotation-kafka")))
    assert(!HmacKeyRetirementKafkaLineage.matches(original, original.copy(bootstrapEndpoint = "127.0.0.1:19094")))
  }

  test("Kafka endpoint must be published by the container mounting the exact isolated volume") {
    val volume = "rotation_hmac-rotation-kafka"
    val valid = Vector(("volume", volume, "/var/lib/kafka/data", true))
    assert(HmacKeyRetirementKafkaLineage.bindingMatches("127.0.0.1:19093", "127.0.0.1:19093", valid, volume))
    assert(!HmacKeyRetirementKafkaLineage.bindingMatches("127.0.0.1:19093", "127.0.0.1:19094", valid, volume))
    assert(
      !HmacKeyRetirementKafkaLineage.bindingMatches(
        "127.0.0.1:19093",
        "127.0.0.1:19093",
        Vector(("volume", "other", "/var/lib/kafka/data", true)),
        volume
      )
    )
    assert(
      !HmacKeyRetirementKafkaLineage.bindingMatches(
        "127.0.0.1:19093",
        "127.0.0.1:19093",
        Vector(("bind", volume, "/var/lib/kafka/data", true)),
        volume
      )
    )
  }
}
