package com.example.hiring.analytics

import com.example.hiring.analytics.service.erasure.{ErasureBacklog, ErasureBacklogLogPolicy, ErasurePhase}
import munit.FunSuite

import java.time.Instant
import scala.concurrent.duration.*

final class ErasureBacklogLogPolicySpec extends FunSuite {
  private val start = Instant.parse("2026-10-22T10:00:00Z")
  private val interval = 1.hour
  private val backlog = ErasureBacklog(1L, 1L, Some(2.hours), Some(ErasurePhase.Requested))

  test("AEC-06.1 the first non-empty backlog is logged and an empty one is not") {
    assert(ErasureBacklogLogPolicy.decide(None, backlog, start, interval).log)
    val idle = ErasureBacklogLogPolicy.decide(None, ErasureBacklog(0L, 0L, None, None), start, interval)
    assertEquals(idle, ErasureBacklogLogPolicy.Decision(None, log = false))
  }

  test("AEC-06.1 an unchanged summary with a growing age is not logged before the interval") {
    val first = ErasureBacklogLogPolicy.decide(None, backlog, start, interval)
    val older = backlog.copy(oldestAge = Some(3.hours))
    val quiet = ErasureBacklogLogPolicy.decide(first.next, older, start.plusSeconds(3599L), interval)
    assertEquals(quiet, ErasureBacklogLogPolicy.Decision(first.next, log = false))
    val due = ErasureBacklogLogPolicy.decide(first.next, older, start.plusSeconds(3600L), interval)
    assert(due.log)
    assertEquals(due.next.map(_.at), Some(start.plusSeconds(3600L)))
  }

  test("AEC-06.1 a changed count or oldest phase is logged at once, and a drained queue forgets the last line") {
    val first = ErasureBacklogLogPolicy.decide(None, backlog, start, interval)
    val at = start.plusSeconds(5L)
    assert(ErasureBacklogLogPolicy.decide(first.next, backlog.copy(pending = 2L), at, interval).log)
    val otherPhase = backlog.copy(oldestPhase = Some(ErasurePhase.DeltaPurged))
    assert(ErasureBacklogLogPolicy.decide(first.next, otherPhase, at, interval).log)
    val drained = ErasureBacklogLogPolicy.decide(first.next, ErasureBacklog(0L, 0L, None, None), at, interval)
    assertEquals(drained.next, None)
    assert(ErasureBacklogLogPolicy.decide(drained.next, backlog, at.plusSeconds(1L), interval).log)
  }
}
