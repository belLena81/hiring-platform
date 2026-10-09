package com.example.hiring.analytics
import com.example.hiring.analytics.service.erasure.*

import munit.FunSuite

class KafkaRetentionBarrierSpec extends FunSuite {
  private val barrier = KafkaRetentionBarrier
    .from("hiring.operational-events", Vector(1 -> 23L, 0 -> 41L))
    .toOption
    .get

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
    assert(KafkaRetentionBarrier.from(" ", Vector(0 -> 1L)).isLeft)
    assert(KafkaRetentionBarrier.from("hiring.operational-events", Vector(-1 -> 1L)).isLeft)
    assert(KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> -1L)).isLeft)
    assert(KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 1L, 0 -> 2L)).isLeft)
  }
}
