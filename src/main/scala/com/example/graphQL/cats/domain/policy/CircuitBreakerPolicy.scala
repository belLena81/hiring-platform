package com.example.graphQL.cats.domain.policy

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Pure circuit-breaker rules. Time is a value; the effectful wrapper owns state and clocks. */
object CircuitBreakerPolicy {

  /** `failureThreshold` and `halfOpenProbes` are at least 1; `openDuration <= maxOpenDuration` (checked at startup). */
  final case class Settings(
      failureThreshold: Int,
      window: FiniteDuration,
      openDuration: FiniteDuration,
      maxOpenDuration: FiniteDuration,
      halfOpenProbes: Int
  )

  /** `failures` holds only counted failures still inside the window, never more than the threshold minus one. */
  enum State {
    case Closed(failures: Vector[Instant])
    case Open(until: Instant, consecutiveOpens: Int)

    /** `cycle` is the end of the open period this half-open phase began from; it tells probes of different cycles
      * apart.
      */
    case HalfOpen(probesInFlight: Int, consecutiveOpens: Int, cycle: Instant)
  }

  /** Whether the dependency answered, failed as unavailable, or failed for a reason that says nothing about it. */
  enum Completion {
    case Success, CountedFailure, IgnoredFailure
  }

  enum Admission {

    /** `probe` holds the cycle of a half-open probe, and is empty for a call admitted while closed. */
    case Admit(probe: Option[Instant])
    case Reject(retryAt: Option[Instant])
  }

  enum Transition(val wire: String) {
    case Opened extends Transition("closed_to_open")
    case HalfOpened extends Transition("open_to_half_open")
    case Closed extends Transition("half_open_to_closed")
    case Reopened extends Transition("half_open_to_open")
  }

  val initial: State = State.Closed(Vector.empty)

  /** Decides one call. An elapsed open period admits the call as the first half-open probe. */
  def attempt(settings: Settings, state: State, now: Instant): (State, Admission) =
    state match {
      case State.Closed(_)                             => (state, Admission.Admit(None))
      case State.Open(until, _) if now.isBefore(until) => (state, Admission.Reject(Some(until)))
      case State.Open(until, opens) => (State.HalfOpen(1, opens, until), Admission.Admit(Some(until)))
      case State.HalfOpen(inFlight, opens, cycle) if inFlight < settings.halfOpenProbes =>
        (State.HalfOpen(inFlight + 1, opens, cycle), Admission.Admit(Some(cycle)))
      case State.HalfOpen(_, _, _) => (state, Admission.Reject(None))
    }

  /** Whether `attempt` would admit, without taking a probe slot. */
  def admits(settings: Settings, state: State, now: Instant): Boolean =
    attempt(settings, state, now)._2 match {
      case Admission.Admit(_)  => true
      case Admission.Reject(_) => false
    }

  /** Records a finished call. `probe` is what `attempt` returned: a call admitted while closed, or a probe of an
    * earlier cycle, never closes, reopens or frees a slot of the current half-open phase.
    */
  def complete(
      settings: Settings,
      state: State,
      probe: Option[Instant],
      completion: Completion,
      now: Instant
  ): State =
    state match {
      case State.HalfOpen(inFlight, opens, cycle) if probe.contains(cycle) =>
        completion match {
          case Completion.Success        => initial
          case Completion.CountedFailure => reopen(settings, opens + 1, now)
          case Completion.IgnoredFailure => State.HalfOpen(math.max(inFlight - 1, 0), opens, cycle)
        }
      case State.Closed(failures) if completion == Completion.CountedFailure =>
        val live = failures.filter(_.isAfter(now.minusMillis(settings.window.toMillis))) :+ now
        if (live.size >= settings.failureThreshold) reopen(settings, 1, now) else State.Closed(live)
      case other => other
    }

  /** The observable change between two states, if any. */
  def transition(before: State, after: State): Option[Transition] =
    (before, after) match {
      case (State.Closed(_), State.Open(_, _))         => Some(Transition.Opened)
      case (State.Open(_, _), State.HalfOpen(_, _, _)) => Some(Transition.HalfOpened)
      case (State.HalfOpen(_, _, _), State.Closed(_))  => Some(Transition.Closed)
      case (State.HalfOpen(_, _, _), State.Open(_, _)) => Some(Transition.Reopened)
      case _                                           => None
    }

  private def reopen(settings: Settings, opens: Int, now: Instant): State.Open = {
    val millis = RetryBackoff.exponentialMillis(
      opens.toLong,
      settings.openDuration.toMillis,
      settings.maxOpenDuration.toMillis
    )
    State.Open(now.plusMillis(millis), opens)
  }
}
