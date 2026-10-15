package com.example.graphQL.cats.service.port

import scala.concurrent.duration.*

/** Largest number of a deleted recruiter's open jobs closed by one transaction. The account-deletion transaction and
  * every follow-up transaction use the same bound, so no transaction grows with the recruiter's job count.
  */
final case class JobClosurePageSize private (value: Int)

object JobClosurePageSize {
  val Max = 1000
  val Default: JobClosurePageSize = JobClosurePageSize(Max)

  def from(value: Int): Option[JobClosurePageSize] = Option.when(value >= 1 && value <= Max)(JobClosurePageSize(value))
}

/** Delay before a pending closure whose page failed is tried again: `initial`, doubled per failed attempt, capped at
  * `max`. A failing row is rotated behind the others, never dropped, so one poison row cannot stall later closures.
  */
final case class JobClosureRetry private (initial: FiniteDuration, max: FiniteDuration) {
  def delay(attempts: Int): FiniteDuration = {
    val doublings = math.min(math.max(attempts - 1, 0), 30)
    FiniteDuration(math.min(initial.toMillis * (1L << doublings), max.toMillis), MILLISECONDS)
  }
}

object JobClosureRetry {
  val Default: JobClosureRetry = JobClosureRetry(5.seconds, 15.minutes)

  def from(initial: FiniteDuration, max: FiniteDuration): Option[JobClosureRetry] =
    Option.when(initial > Duration.Zero && max >= initial)(JobClosureRetry(initial, max))
}

/** What one follow-up transaction did for the oldest pending closure. */
enum RecruiterJobClosureProgress {

  /** No pending closure is eligible now (none exist, or every one is waiting out its retry delay). */
  case Idle

  /** A page was closed and more open jobs of the same recruiter remain. */
  case PageClosed(jobs: Int)

  /** The last open jobs were closed and the pending closure was removed in the same transaction. */
  case Completed(jobs: Int)
}

/** Durable follow-up for open jobs of a deleted recruiter beyond the first page closed by the deletion transaction.
  * Each call is one idempotent transaction: job closures, their one `JOB_CLOSED` event each, and the removal of the
  * pending work commit together or not at all, so a crash or retry never duplicates an event or loses a job.
  */
trait RecruiterJobClosureRepository[F[_]] {
  def closeNextPage: RepositoryF[F, RecruiterJobClosureProgress]
}
