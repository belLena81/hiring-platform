package com.example.hiring.analytics.domain

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*

/** Delta retention horizons that bound how long purged data stays physically recoverable. */
final case class DeltaErasureHorizons(vacuumSafety: FiniteDuration, logRetention: FiniteDuration)

/** Pure rule for when purged Delta data is physically gone and how long that can take at worst. */
object ErasureCompletionRule {
  private val OneDay: FiniteDuration = 1.day

  /** Delta truncates the log-cleanup cutoff to the start of a UTC day, so a log written on the purge day cannot be
    * reclaimed before the next day plus the log retention. Data files are reclaimable after the vacuum safety.
    */
  def deltaDeadline(purgedAt: Instant, vacuumSafety: FiniteDuration, logRetention: FiniteDuration): Instant = {
    val dataDeadline = purgedAt.plusMillis(vacuumSafety.toMillis)
    val logDeadline = purgedAt
      .truncatedTo(ChronoUnit.DAYS)
      .plus(1L, ChronoUnit.DAYS)
      .plusMillis(logRetention.toMillis)
    if (dataDeadline.isAfter(logDeadline)) dataDeadline else logDeadline
  }

  def deltaDeadline(purgedAt: Instant, horizons: DeltaErasureHorizons): Instant =
    deltaDeadline(purgedAt, horizons.vacuumSafety, horizons.logRetention)

  /** `OPTIMIZE` replaces data files without deleting them, so a file that once held the subject's rows stays
    * recoverable until VACUUM after the replacement. The horizon therefore runs from the later of the purge and the
    * last compaction.
    */
  def deltaDeadline(purgedAt: Instant, lastCompactionAt: Option[Instant], horizons: DeltaErasureHorizons): Instant =
    deltaDeadline(
      lastCompactionAt.filter(_.isAfter(purgedAt)).getOrElse(purgedAt),
      horizons.vacuumSafety,
      horizons.logRetention
    )

  /** Upper bound from the purge to verified physical erasure: the slowest of the Kafka expiry, the Delta log rollover
    * plus retention, and the vacuum safety.
    */
  def worstCaseCompletion(horizons: DeltaErasureHorizons, kafkaWorstCase: FiniteDuration): FiniteDuration =
    List(kafkaWorstCase, OneDay + horizons.logRetention, horizons.vacuumSafety).max
}
