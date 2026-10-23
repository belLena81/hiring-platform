package com.example.hiring.analytics.service.erasure

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Decides when the aggregate backlog line is worth logging. The oldest age grows every poll, so it is not compared. */
object ErasureBacklogLogPolicy {

  /** What the last logged line said, without the ever-growing age. */
  final case class Summary(pending: Long, processing: Long, oldestPhase: Option[ErasurePhase])

  final case class LoggedLine(summary: Summary, at: Instant)

  final case class Decision(next: Option[LoggedLine], log: Boolean)

  def summaryOf(backlog: ErasureBacklog): Summary = Summary(backlog.pending, backlog.processing, backlog.oldestPhase)

  /** Logs when the summary changed or `interval` has elapsed since the last line; an empty backlog forgets the line. */
  def decide(last: Option[LoggedLine], backlog: ErasureBacklog, now: Instant, interval: FiniteDuration): Decision =
    if (backlog.isEmpty) Decision(None, log = false)
    else {
      val summary = summaryOf(backlog)
      val due = last.forall(line => line.summary != summary || !now.isBefore(line.at.plusMillis(interval.toMillis)))
      if (due) Decision(Some(LoggedLine(summary, now)), log = true) else Decision(last, log = false)
    }

  def render(backlog: ErasureBacklog): String =
    "erasure backlog: pending=" + backlog.pending + " processing=" + backlog.processing +
      " oldestRequestedAgeHours=" + backlog.oldestAge.fold("unknown")(_.toHours.toString) +
      " phase=" + backlog.oldestPhase.fold("unknown")(_.persistedName)
}
