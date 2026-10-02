package com.example.hiring.analytics.cli

import munit.FunSuite
import scala.concurrent.duration.*

final class HmacRetirementProofCalendarSpec extends FunSuite {
  private val nonce = "0123456789abcdef"
  private val root = s"file:///workspace/.local/data/hmac-key-retirement/isolated-$nonce/lakehouse"
  private val database = s"hiring_hmac_rotation_test_$nonce"
  private val topic = s"hiring.hmac.rotation.test.$nonce"
  private val key = s"rotation-old-$nonce"

  test("calendar substitution binds root, database, topic and key to one owned nonce") {
    assertEquals(HmacRetirementProofCalendar.shift(root, database, topic, key, Some(nonce)), Right(32.days.toMillis))
    assert(HmacRetirementProofCalendar.shift(root, "hiring", topic, key, Some(nonce)).isLeft)
    assert(HmacRetirementProofCalendar.shift("file:///production/lakehouse", database, topic, key, Some(nonce)).isLeft)
    assert(HmacRetirementProofCalendar.shift(root, database, "hiring.operational-events", key, Some(nonce)).isLeft)
    assert(HmacRetirementProofCalendar.shift(root, database, topic, "production-key", Some(nonce)).isLeft)
    assert(HmacRetirementProofCalendar.shift(root, database, topic, key, Some("invalid")).isLeft)
    assert(
      HmacRetirementProofCalendar
        .shift(
          s"file:///workspace/.local/data/hmac-key-retirement/$nonce/lakehouse",
          s"hiring_hmac_rotation_$nonce",
          s"hiring.hmac.rotation.$nonce",
          key,
          Some(nonce)
        )
        .isLeft
    )
  }

  test("existing real-horizon fixture has no calendar substitution") {
    assertEquals(
      HmacRetirementProofCalendar.shift("file:///real/lakehouse", "hiring", "real.topic", "real-key", None),
      Right(0L)
    )
  }
}
