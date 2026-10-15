package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import _root_.pureconfig.*

/** Follow-up closure of a deleted recruiter's open jobs. `pageSize` bounds every closure transaction, including the one
  * inside account deletion. Durations are milliseconds: `idlePollMillis` is the wait when nothing is eligible,
  * `failureBackoff*` the worker wait after an infrastructure failure, and `rowRetry*` the delay before a failing
  * pending closure is tried again (doubled per failed attempt up to the maximum). The defaults mirror the packaged
  * `application.conf` for an absent section.
  */
final case class RecruiterJobClosureConfig(
    pageSize: Int = 1000,
    idlePollMillis: Int = 1000,
    failureBackoffInitialMillis: Int = 1000,
    failureBackoffMaxMillis: Int = 30000,
    rowRetryInitialMillis: Int = 5000,
    rowRetryMaxMillis: Int = 900000
)

private[config] final case class RawRecruiterJobClosureConfig(
    pageSize: Int,
    idlePollMillis: Int,
    failureBackoffInitialMillis: Int,
    failureBackoffMaxMillis: Int,
    rowRetryInitialMillis: Int,
    rowRetryMaxMillis: Int
) derives ConfigReader

private[config] object RecruiterJobClosureConfigValidation {
  private val PageSizes = 1 to 1000
  private val Durations = 10 to 86400000

  def read(raw: RawRecruiterJobClosureConfig): ValidatedNel[ConfigError, RecruiterJobClosureConfig] =
    (
      within(raw.pageSize, PageSizes, ConfigError.InvalidRecruiterJobClosurePageSize),
      within(raw.idlePollMillis, Durations, ConfigError.InvalidRecruiterJobClosureIdlePoll),
      within(raw.failureBackoffInitialMillis, Durations, ConfigError.InvalidRecruiterJobClosureFailureBackoff),
      within(raw.failureBackoffMaxMillis, Durations, ConfigError.InvalidRecruiterJobClosureFailureBackoff),
      within(raw.rowRetryInitialMillis, Durations, ConfigError.InvalidRecruiterJobClosureRowRetry),
      within(raw.rowRetryMaxMillis, Durations, ConfigError.InvalidRecruiterJobClosureRowRetry),
      (
        orderedWhenValid(raw.failureBackoffInitialMillis, raw.failureBackoffMaxMillis),
        orderedWhenValid(raw.rowRetryInitialMillis, raw.rowRetryMaxMillis)
      ).tupled.void
    ).mapN((page, idle, failInitial, failMax, rowInitial, rowMax, _) =>
      RecruiterJobClosureConfig(page, idle, failInitial, failMax, rowInitial, rowMax)
    )

  /** The order is only judged once both values are themselves valid, so one mistake reports one error. */
  private def orderedWhenValid(initial: Int, max: Int): ValidatedNel[ConfigError, Unit] =
    Either
      .cond(
        !(Durations.contains(initial) && Durations.contains(max)) || initial <= max,
        (),
        ConfigError.InvalidRecruiterJobClosureBackoffOrder
      )
      .toValidatedNel

  private def within(value: Int, range: Range, error: ConfigError): ValidatedNel[ConfigError, Int] =
    Either.cond(range.contains(value), value, error).toValidatedNel
}
