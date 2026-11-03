package com.example.graphQL.cats.domain.policy

import com.example.graphQL.cats.domain.policy.ConsumerReceiptWindowRefusal.*
import munit.FunSuite
import scala.concurrent.duration.*

final class ConsumerReceiptWindowSpec extends FunSuite {
  private def check(ttl: FiniteDuration, retention: FiniteDuration, segment: FiniteDuration) =
    ConsumerReceiptWindow.check(ttl, retention.toMillis, segment.toMillis)

  test("DDC-02.1 retention 7 days, segment 1 day and receipt TTL 9 days is accepted") {
    assertEquals(check(9.days, 7.days, 1.day), Right(()))
  }

  test("DDC-02.2 receipt TTL 8 days with retention 7 days and segment 1 day is refused for lack of the margin") {
    assertEquals(check(8.days, 7.days, 1.day), Left(WindowExceedsReceiptTtl))
  }

  test("DDC-02.3 unbounded retention (-1) never passes, however long the receipt TTL") {
    assertEquals(ConsumerReceiptWindow.check(3650.days, -1L, 1.day.toMillis), Left(RetentionUnbounded))
    assertEquals(ConsumerReceiptWindow.check(3650.days, 7.days.toMillis, -1L), Left(RetentionUnbounded))
  }

  test("DDC-02.7 the one hour margin decides the boundary: segment 23h30m is refused, 23h is accepted") {
    assertEquals(check(8.days, 7.days, 23.hours + 30.minutes), Left(WindowExceedsReceiptTtl))
    assertEquals(check(8.days, 7.days, 23.hours), Right(()))
  }

  test("DDC-02.7 the largest representable retention is refused without overflow") {
    assertEquals(
      ConsumerReceiptWindow.check(9.days, Long.MaxValue, Long.MaxValue),
      Left(WindowExceedsReceiptTtl)
    )
  }
}
