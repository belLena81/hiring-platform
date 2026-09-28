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

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import munit.FunSuite

class KafkaRetentionBarrierSpec extends FunSuite {
  private val barrier = KafkaRetentionBarrier(
    "hiring.operational-events",
    Vector(KafkaRetentionBarrier.Partition(1, 23L), KafkaRetentionBarrier.Partition(0, 41L))
  )

  test("retention is passed only after earliest offsets reach each captured exclusive end") {
    assertEquals(
      KafkaRetentionBarrier.hasExpired(barrier, Map(0 -> 41L, 1 -> 23L)),
      Right(true)
    )
    assertEquals(
      KafkaRetentionBarrier.hasExpired(barrier, Map(0 -> 42L, 1 -> 22L)),
      Right(false)
    )
  }

  test("a missing captured Kafka partition fails closed") {
    assert(KafkaRetentionBarrier.hasExpired(barrier, Map(0 -> 41L)).isLeft)
  }

  test("malformed and duplicate partition barriers are rejected") {
    val malformed = KafkaRetentionBarrier(" ", Vector(KafkaRetentionBarrier.Partition(0, 1L)))
    val duplicate = KafkaRetentionBarrier(
      "hiring.operational-events",
      Vector(KafkaRetentionBarrier.Partition(0, 1L), KafkaRetentionBarrier.Partition(0, 2L))
    )
    assert(KafkaRetentionBarrier.validate(malformed).isLeft)
    assert(KafkaRetentionBarrier.validate(duplicate).isLeft)
  }
}
