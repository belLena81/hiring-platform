package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.EmbeddingMeta
import com.example.graphQL.cats.service.port.{EmbeddingWorkFailure, EmbeddingWorkKind, EmbeddingWorkState}

import java.time.{Duration, Instant}
import scala.concurrent.duration.*

/** Does the stored embedding match the entity now? Independent of the repair dimension. */
enum EmbeddingFreshness {
  case Current, ContentChanged, ModelMismatch, NotEmbedded
}

/** Is a repair scheduled for the entity? Independent of the freshness dimension. */
enum EmbeddingRepairState {
  case NoQueuedWork, Waiting, Retrying, InProgress, LeaseExpired, Failed
}

/** The queue row facts needed for classification; carries no payload, token or worker identity. */
final case class EmbeddingQueuedWork(
    state: EmbeddingWorkState,
    failure: Option[EmbeddingWorkFailure],
    availableAt: Instant,
    leaseUntil: Option[Instant]
)

/** One searchable entity as observed by the scan. `changedAt` is present only where the store records it. */
final case class EmbeddingCoverageEntity(
    kind: EmbeddingWorkKind,
    stored: Option[EmbeddingMeta],
    currentSourceHash: String,
    changedAt: Option[Instant],
    work: Option[EmbeddingQueuedWork]
)

object EmbeddingCoverageClassifier {
  def freshness(
      stored: Option[EmbeddingMeta],
      currentSourceHash: String,
      expectedModel: Option[String]
  ): EmbeddingFreshness =
    stored match {
      case None                                                => EmbeddingFreshness.NotEmbedded
      case Some(meta) if meta.sourceHash != currentSourceHash  => EmbeddingFreshness.ContentChanged
      case Some(meta) if expectedModel.exists(_ != meta.model) => EmbeddingFreshness.ModelMismatch
      case Some(_)                                             => EmbeddingFreshness.Current
    }

  /** A processing row is claimable again once its lease has strictly passed, matching the claim predicate. */
  def repairState(work: Option[EmbeddingQueuedWork], now: Instant): EmbeddingRepairState =
    work.fold(EmbeddingRepairState.NoQueuedWork) { row =>
      row.state match {
        case EmbeddingWorkState.Ready      => EmbeddingRepairState.Waiting
        case EmbeddingWorkState.Retry      => EmbeddingRepairState.Retrying
        case EmbeddingWorkState.Failed     => EmbeddingRepairState.Failed
        case EmbeddingWorkState.Processing =>
          if (row.leaseUntil.exists(!_.isBefore(now))) EmbeddingRepairState.InProgress
          else EmbeddingRepairState.LeaseExpired
      }
    }

  def failure(work: Option[EmbeddingQueuedWork]): Option[EmbeddingWorkFailure] =
    work.filter(_.state == EmbeddingWorkState.Failed).flatMap(_.failure)

  /** The single classification of one entity; every consumer (tally, checks) uses this result. */
  def classify(
      entity: EmbeddingCoverageEntity,
      expectedModel: Option[String],
      now: Instant
  ): EmbeddingClassification = {
    val freshness = this.freshness(entity.stored, entity.currentSourceHash, expectedModel)
    val repair = repairState(entity.work, now)
    EmbeddingClassification(freshness, repair, failure(entity.work))
  }

  /** A non-current entity with no queued work will never be repaired by the existing worker. */
  def isOrphanedGap(freshness: EmbeddingFreshness, repair: EmbeddingRepairState): Boolean =
    freshness != EmbeddingFreshness.Current && repair == EmbeddingRepairState.NoQueuedWork
}

final case class EmbeddingClassification(
    freshness: EmbeddingFreshness,
    repairState: EmbeddingRepairState,
    failure: Option[EmbeddingWorkFailure]
)

final case class EmbeddingCoverageCellKey(
    kind: EmbeddingWorkKind,
    freshness: EmbeddingFreshness,
    repairState: EmbeddingRepairState,
    failure: Option[EmbeddingWorkFailure]
)

/** Immutable cross-tab accumulator: each added entity increments exactly one cell. */
final case class EmbeddingCoverageTally(
    cells: Map[EmbeddingCoverageCellKey, Long],
    models: Map[(EmbeddingWorkKind, String), Long],
    lagSeconds: Vector[Long],
    oldestNotCurrentChangedAt: Option[Instant]
) {

  /** Classifies the entity once and counts it in exactly one cell. */
  def add(entity: EmbeddingCoverageEntity, expectedModel: Option[String], now: Instant): EmbeddingCoverageTally = {
    val classification = EmbeddingCoverageClassifier.classify(entity, expectedModel, now)
    val freshness = classification.freshness
    val key = EmbeddingCoverageCellKey(entity.kind, freshness, classification.repairState, classification.failure)
    val lag = for {
      meta <- entity.stored
      changed <- entity.changedAt
      if freshness == EmbeddingFreshness.Current
    } yield math.max(0L, Duration.between(changed, meta.updatedAt).getSeconds)
    val older =
      if (freshness == EmbeddingFreshness.Current) oldestNotCurrentChangedAt
      else
        (oldestNotCurrentChangedAt.toList ++ entity.changedAt.toList).minOption
    copy(
      cells = cells.updated(key, cells.getOrElse(key, 0L) + 1L),
      models = entity.stored.fold(models) { meta =>
        val modelKey = entity.kind -> meta.model
        models.updated(modelKey, models.getOrElse(modelKey, 0L) + 1L)
      },
      lagSeconds = lagSeconds ++ lag,
      oldestNotCurrentChangedAt = older
    )
  }

  def scanned(kind: EmbeddingWorkKind): Long =
    cells.iterator.collect { case (key, count) if key.kind == kind => count }.sum

  def current(kind: EmbeddingWorkKind): Long =
    cells.iterator.collect {
      case (key, count) if key.kind == kind && key.freshness == EmbeddingFreshness.Current => count
    }.sum

  def orphanedGaps: Long =
    cells.iterator.collect {
      case (key, count) if EmbeddingCoverageClassifier.isOrphanedGap(key.freshness, key.repairState) => count
    }.sum
}

object EmbeddingCoverageTally {
  val empty: EmbeddingCoverageTally = EmbeddingCoverageTally(Map.empty, Map.empty, Vector.empty, None)
}

/** Named, narrowly scoped bounds for one report scan. */
final case class EmbeddingCoverageLimits(maxEntitiesPerKind: Int, pageSize: Int, maxTime: FiniteDuration)

object EmbeddingCoverageLimits {
  val default: EmbeddingCoverageLimits =
    EmbeddingCoverageLimits(maxEntitiesPerKind = 50000, pageSize = 500, maxTime = 10.seconds)
}

final case class EmbeddingCoverageScanRequest(
    expectedModel: Option[String],
    now: Instant,
    stuckBefore: Instant,
    limits: EmbeddingCoverageLimits
)

final case class EmbeddingCoverageKindObservation(kind: EmbeddingWorkKind, searchableCount: Long, truncated: Boolean)

/** Queue-wide facts for one entity kind; failed rows are not waiting work. */
final case class EmbeddingQueueObservation(
    truncated: Boolean,
    stuckCount: Long,
    oldestWaitingAvailableAt: Option[Instant]
)

final case class EmbeddingCoverageObservation(
    tally: EmbeddingCoverageTally,
    kinds: List[EmbeddingCoverageKindObservation],
    queue: EmbeddingQueueObservation
)

enum EmbeddingCoverageCheckName {
  case NoOrphanedGap, NoStuckWork
}

enum EmbeddingCoverageCheckStatus {
  case Passed, Failed, Inconclusive
}

final case class EmbeddingCoverageCheck(
    name: EmbeddingCoverageCheckName,
    status: EmbeddingCoverageCheckStatus,
    offendingCount: Long
)

final case class EmbeddingCoverageCell(
    kind: EmbeddingWorkKind,
    freshness: EmbeddingFreshness,
    repairState: EmbeddingRepairState,
    failure: Option[EmbeddingWorkFailure],
    count: Long
)

final case class EmbeddingObservedModel(kind: EmbeddingWorkKind, model: String, count: Long)

/** `searchableCount` is the number of eligible entities of the kind (an exact server-side count, never smaller than
  * `scannedCount`). `scannedCount` is the number of entities that were classified and counted in the cross-tab (the sum
  * of this kind's cells), not a raw cursor counter; it can differ from `searchableCount` when the scan was truncated,
  * or when eligibility changed between the count and the scan (entities becoming or ceasing to be searchable).
  */
final case class EmbeddingCoverageKindSummary(
    kind: EmbeddingWorkKind,
    searchableCount: Long,
    scannedCount: Long,
    truncated: Boolean,
    coverageShare: Option[Double]
)

final case class EmbeddingCoverageLag(p50Seconds: Long, p95Seconds: Long, p99Seconds: Long, sampleCount: Long)

/** Aggregate-only report; it never carries identifiers, names, text, vectors or provider payloads. */
final case class EmbeddingCoverageReport(
    asOf: Instant,
    expectedModel: Option[String],
    kinds: List[EmbeddingCoverageKindSummary],
    cells: List[EmbeddingCoverageCell],
    observedModels: List[EmbeddingObservedModel],
    queueTruncated: Boolean,
    oldestQueuedWorkAgeSeconds: Option[Long],
    oldestNotCurrentAgeSeconds: Option[Long],
    lagEntityKinds: List[EmbeddingWorkKind],
    lagSeconds: Option[EmbeddingCoverageLag],
    checks: List[EmbeddingCoverageCheck]
)

object EmbeddingCoverageReport {

  /** Entity kinds whose store records a change time. Users carry `updatedAt` only after a profile edit, so candidate
    * lag and age would be selection-biased and are excluded.
    */
  val timestampedKinds: List[EmbeddingWorkKind] = List(EmbeddingWorkKind.Job)

  def assemble(
      observation: EmbeddingCoverageObservation,
      expectedModel: Option[String],
      now: Instant
  ): EmbeddingCoverageReport = {
    val tally = observation.tally
    val kinds = observation.kinds.map { kind =>
      val searchable = math.max(kind.searchableCount, tally.scanned(kind.kind))
      EmbeddingCoverageKindSummary(
        kind.kind,
        searchable,
        tally.scanned(kind.kind),
        kind.truncated,
        Option.when(searchable > 0L)(tally.current(kind.kind).toDouble / searchable.toDouble)
      )
    }
    val cells = tally.cells.toList
      .sortBy { case (key, _) =>
        (key.kind.ordinal, key.freshness.ordinal, key.repairState.ordinal, key.failure.fold(-1)(_.ordinal))
      }
      .map { case (key, count) => EmbeddingCoverageCell(key.kind, key.freshness, key.repairState, key.failure, count) }
    val models = tally.models.toList
      .sortBy { case ((kind, model), _) => (kind.ordinal, model) }
      .map { case ((kind, model), count) => EmbeddingObservedModel(kind, model, count) }
    val scanTruncated = kinds.exists(_.truncated)
    EmbeddingCoverageReport(
      asOf = now,
      expectedModel = expectedModel,
      kinds = kinds,
      cells = cells,
      observedModels = models,
      queueTruncated = observation.queue.truncated,
      oldestQueuedWorkAgeSeconds = observation.queue.oldestWaitingAvailableAt.map(ageSeconds(_, now)),
      oldestNotCurrentAgeSeconds = tally.oldestNotCurrentChangedAt.map(ageSeconds(_, now)),
      lagEntityKinds = timestampedKinds,
      lagSeconds = lag(tally.lagSeconds),
      checks = List(
        check(EmbeddingCoverageCheckName.NoOrphanedGap, tally.orphanedGaps, scanTruncated),
        check(EmbeddingCoverageCheckName.NoStuckWork, observation.queue.stuckCount, observation.queue.truncated)
      )
    )
  }

  /** A truncated read can never pass; the offending count is then a lower bound. */
  private def check(name: EmbeddingCoverageCheckName, offending: Long, truncated: Boolean): EmbeddingCoverageCheck =
    EmbeddingCoverageCheck(
      name,
      if (truncated) EmbeddingCoverageCheckStatus.Inconclusive
      else if (offending == 0L) EmbeddingCoverageCheckStatus.Passed
      else EmbeddingCoverageCheckStatus.Failed,
      offending
    )

  private def ageSeconds(since: Instant, now: Instant): Long =
    math.max(0L, Duration.between(since, now).getSeconds)

  private def lag(samples: Vector[Long]): Option[EmbeddingCoverageLag] =
    Option.when(samples.nonEmpty) {
      val sorted = samples.sorted
      def nearestRank(percentile: Int): Long =
        sorted(math.max(0, math.ceil(percentile / 100.0 * sorted.size).toInt - 1))
      EmbeddingCoverageLag(nearestRank(50), nearestRank(95), nearestRank(99), sorted.size.toLong)
    }
}
