package com.example.graphQL.cats.domain.policy

import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.State.*
import java.time.Instant
import munit.FunSuite
import scala.concurrent.duration.*

final class CircuitBreakerPolicySpec extends FunSuite {
  private val settings = Settings(3, 10.seconds, 2.seconds, 5.seconds, 1)
  private val t0 = Instant.parse("2026-10-14T10:00:00Z")
  private val cycle = t0.plusSeconds(2)

  private def failed(state: State, at: Instant): State =
    complete(settings, state, None, Completion.CountedFailure, at)

  test("CBR-01.1 three counted failures within the window open the breaker") {
    val opened = List(0L, 1L, 2L).foldLeft(initial)((state, s) => failed(state, t0.plusSeconds(s)))
    assertEquals(opened, Open(t0.plusSeconds(2).plusSeconds(2), 1))
  }

  test("CBR-01.2 failures older than the window no longer count") {
    val afterTwo = List(0L, 1L).foldLeft(initial)((state, s) => failed(state, t0.plusSeconds(s)))
    val later = failed(afterTwo, t0.plusSeconds(30))
    assertEquals(later, Closed(Vector(t0.plusSeconds(30))))
  }

  test("CBR-01.3 ignored failure while closed leaves the count unchanged") {
    val one = failed(initial, t0)
    assertEquals(complete(settings, one, None, Completion.IgnoredFailure, t0.plusSeconds(1)), one)
    assertEquals(complete(settings, one, None, Completion.Success, t0.plusSeconds(1)), one)
  }

  test("CBR-01.4 open before the reopening time rejects with retryAt") {
    val open = Open(cycle, 1)
    assertEquals(attempt(settings, open, t0.plusSeconds(1)), (open, Admission.Reject(Some(cycle))))
  }

  test("CBR-01.5 open at the reopening time admits the attempt as a half-open probe") {
    val (state, admission) = attempt(settings, Open(cycle, 1), cycle)
    assertEquals(state, HalfOpen(1, 1, cycle))
    assertEquals(admission, Admission.Admit(Some(cycle)))
  }

  test("CBR-01.6 half-open success closes and forgets earlier openings") {
    val closed = complete(settings, HalfOpen(1, 3, cycle), Some(cycle), Completion.Success, t0)
    assertEquals(closed, initial)
    // A failure after recovery starts the backoff again from the first opening.
    val again = List(0L, 1L, 2L).foldLeft(closed)((state, s) => failed(state, t0.plusSeconds(s)))
    assertEquals(again, Open(t0.plusSeconds(4), 1))
  }

  test("CBR-01.7 half-open failure reopens with a doubled duration capped at the maximum") {
    val first = complete(settings, HalfOpen(1, 1, cycle), Some(cycle), Completion.CountedFailure, t0)
    assertEquals(first, Open(t0.plusSeconds(4), 2))
    val capped = complete(settings, HalfOpen(1, 2, cycle), Some(cycle), Completion.CountedFailure, t0)
    assertEquals(capped, Open(t0.plusSeconds(5), 3))
  }

  test("CBR-01.8 half-open with all probes in flight rejects the attempt") {
    val full = HalfOpen(1, 1, cycle)
    assertEquals(attempt(settings, full, t0), (full, Admission.Reject(None)))
  }

  test("CBR-01.8 an ignored probe frees its slot without closing or reopening") {
    assertEquals(
      complete(settings, HalfOpen(1, 2, cycle), Some(cycle), Completion.IgnoredFailure, t0),
      HalfOpen(0, 2, cycle)
    )
  }

  test("CBR-01.2 a call admitted while closed does not change a half-open breaker when it finishes late") {
    val half = HalfOpen(1, 1, cycle)
    assertEquals(complete(settings, half, None, Completion.CountedFailure, t0), half)
    assertEquals(complete(settings, half, None, Completion.Success, t0), half)
  }

  test("CBR-01.9 a probe from an earlier cycle cannot close, reopen or free a slot of the current cycle") {
    val earlier = t0.plusSeconds(1)
    val current = HalfOpen(1, 2, cycle)
    List(Completion.Success, Completion.CountedFailure, Completion.IgnoredFailure).foreach { completion =>
      assertEquals(complete(settings, current, Some(earlier), completion, t0), current)
    }
  }

  test("CBR-01.1 failures finishing while open do not extend the open period") {
    val open = Open(cycle, 1)
    assertEquals(complete(settings, open, None, Completion.CountedFailure, t0.plusSeconds(1)), open)
  }

  test("CBR-01.1 transitions are reported once per state change") {
    val open = Open(t0, 1)
    assertEquals(transition(initial, open), Some(Transition.Opened))
    assertEquals(transition(open, HalfOpen(1, 1, t0)), Some(Transition.HalfOpened))
    assertEquals(transition(HalfOpen(1, 1, t0), initial), Some(Transition.Closed))
    assertEquals(transition(HalfOpen(1, 1, t0), open), Some(Transition.Reopened))
    assertEquals(transition(HalfOpen(1, 1, t0), HalfOpen(0, 1, t0)), None)
    assertEquals(transition(initial, Closed(Vector(t0))), None)
  }
}
