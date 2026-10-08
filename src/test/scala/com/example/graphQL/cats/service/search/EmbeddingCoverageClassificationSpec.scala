package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.EmbeddingMeta
import com.example.graphQL.cats.service.port.{EmbeddingWorkFailure, EmbeddingWorkKind, EmbeddingWorkState}
import munit.FunSuite

import java.time.Instant

final class EmbeddingCoverageClassificationSpec extends FunSuite {
  private val now = Instant.parse("2026-10-08T12:00:00Z")
  private val hash = "hash-now"
  private def meta(model: String = "model-a", sourceHash: String = hash, at: Instant = now.minusSeconds(60)) =
    EmbeddingMeta(model, sourceHash, at)
  private def row(
      state: EmbeddingWorkState,
      leaseUntil: Option[Instant] = None,
      failure: Option[EmbeddingWorkFailure] = None,
      availableAt: Instant = now
  ) = EmbeddingQueuedWork(state, failure, availableAt, leaseUntil)

  test("ECR-01 freshness maps every stored shape to exactly one value") {
    import EmbeddingFreshness.*
    val classify = EmbeddingCoverageClassifier.freshness
    assertEquals(classify(None, hash, None), NotEmbedded)
    assertEquals(classify(None, hash, Some("model-a")), NotEmbedded)
    assertEquals(classify(Some(meta()), hash, None), Current)
    assertEquals(classify(Some(meta()), hash, Some("model-a")), Current)
    assertEquals(classify(Some(meta(model = "model-b")), hash, None), Current)
    assertEquals(classify(Some(meta(model = "model-b")), hash, Some("model-a")), ModelMismatch)
    assertEquals(classify(Some(meta(sourceHash = "old")), hash, None), ContentChanged)
    assertEquals(classify(Some(meta(sourceHash = "old", model = "model-b")), hash, Some("model-a")), ContentChanged)
  }

  test("ECR-01 repair state maps every queue row to exactly one value, including an expired lease") {
    import EmbeddingRepairState.*
    val classify = EmbeddingCoverageClassifier.repairState
    assertEquals(classify(None, now), NoQueuedWork)
    assertEquals(classify(Some(row(EmbeddingWorkState.Ready)), now), Waiting)
    assertEquals(classify(Some(row(EmbeddingWorkState.Retry)), now), Retrying)
    assertEquals(classify(Some(row(EmbeddingWorkState.Processing, Some(now.plusSeconds(1)))), now), InProgress)
    assertEquals(classify(Some(row(EmbeddingWorkState.Processing, Some(now))), now), InProgress)
    assertEquals(classify(Some(row(EmbeddingWorkState.Processing, Some(now.minusMillis(1)))), now), LeaseExpired)
    assertEquals(
      classify(Some(row(EmbeddingWorkState.Failed, failure = Some(EmbeddingWorkFailure.RetryExhausted))), now),
      Failed
    )
  }

  private def entity(
      stored: Option[EmbeddingMeta],
      work: Option[EmbeddingQueuedWork],
      kind: EmbeddingWorkKind = EmbeddingWorkKind.Job,
      changedAt: Option[Instant] = None,
      sourceHash: String = hash
  ) = EmbeddingCoverageEntity(kind, stored, sourceHash, changedAt, work)

  test("a single classification drives the cell and the orphaned-gap rule") {
    val orphan = entity(None, None)
    val repaired = entity(Some(meta(sourceHash = "old")), Some(row(EmbeddingWorkState.Ready)))
    val classification = EmbeddingCoverageClassifier.classify(orphan, None, now)
    assertEquals(
      classification,
      EmbeddingClassification(EmbeddingFreshness.NotEmbedded, EmbeddingRepairState.NoQueuedWork, None)
    )
    assertEquals(EmbeddingCoverageTally.empty.add(orphan, None, now).orphanedGaps, 1L)
    assertEquals(EmbeddingCoverageTally.empty.add(repaired, None, now).orphanedGaps, 0L)
  }

  test("ECR-02 the cross-tab counts each entity once and keeps freshness and repair state separate") {
    val entities = List(
      entity(Some(meta()), None),
      entity(Some(meta()), Some(row(EmbeddingWorkState.Ready))),
      entity(Some(meta(sourceHash = "old")), Some(row(EmbeddingWorkState.Retry))),
      entity(None, Some(row(EmbeddingWorkState.Processing, Some(now.minusSeconds(1))))),
      entity(Some(meta(model = "model-b")), None),
      entity(
        None,
        Some(row(EmbeddingWorkState.Failed, failure = Some(EmbeddingWorkFailure.DocumentTooLarge))),
        kind = EmbeddingWorkKind.CandidateProfile
      )
    )
    val tally = entities.foldLeft(EmbeddingCoverageTally.empty)(_.add(_, Some("model-a"), now))
    assertEquals(tally.cells.values.sum, entities.size.toLong)
    assertEquals(tally.scanned(EmbeddingWorkKind.Job), 5L)
    assertEquals(tally.scanned(EmbeddingWorkKind.CandidateProfile), 1L)
    def cell(kind: EmbeddingWorkKind, f: EmbeddingFreshness, r: EmbeddingRepairState, x: Option[EmbeddingWorkFailure]) =
      tally.cells.getOrElse(EmbeddingCoverageCellKey(kind, f, r, x), 0L)
    val job = EmbeddingWorkKind.Job
    assertEquals(cell(job, EmbeddingFreshness.Current, EmbeddingRepairState.NoQueuedWork, None), 1L)
    assertEquals(cell(job, EmbeddingFreshness.Current, EmbeddingRepairState.Waiting, None), 1L)
    assertEquals(cell(job, EmbeddingFreshness.ContentChanged, EmbeddingRepairState.Retrying, None), 1L)
    assertEquals(cell(job, EmbeddingFreshness.NotEmbedded, EmbeddingRepairState.LeaseExpired, None), 1L)
    assertEquals(cell(job, EmbeddingFreshness.ModelMismatch, EmbeddingRepairState.NoQueuedWork, None), 1L)
    assertEquals(
      cell(
        EmbeddingWorkKind.CandidateProfile,
        EmbeddingFreshness.NotEmbedded,
        EmbeddingRepairState.Failed,
        Some(EmbeddingWorkFailure.DocumentTooLarge)
      ),
      1L
    )
    assertEquals(tally.models.get(job -> "model-a"), Some(3L))
    assertEquals(tally.models.get(job -> "model-b"), Some(1L))
  }

  private def observation(tally: EmbeddingCoverageTally, truncated: Boolean = false, stuck: Long = 0L) =
    EmbeddingCoverageObservation(
      tally,
      List(
        EmbeddingCoverageKindObservation(EmbeddingWorkKind.Job, tally.scanned(EmbeddingWorkKind.Job), truncated),
        EmbeddingCoverageKindObservation(EmbeddingWorkKind.CandidateProfile, 0L, false)
      ),
      EmbeddingQueueObservation(truncated = false, stuck, Some(now.minusSeconds(90)))
    )
  private def status(report: EmbeddingCoverageReport, name: EmbeddingCoverageCheckName) =
    report.checks.find(_.name == name).map(check => check.status -> check.offendingCount)

  test("ECR-03 NoOrphanedGap fails for a non-current entity without queued work and passes otherwise") {
    val healthy = EmbeddingCoverageTally.empty
      .add(entity(Some(meta()), None), None, now)
      .add(entity(Some(meta(sourceHash = "old")), Some(row(EmbeddingWorkState.Ready))), None, now)
    val orphan = healthy.add(entity(None, None), None, now)
    assertEquals(
      status(
        EmbeddingCoverageReport.assemble(observation(healthy), None, now),
        EmbeddingCoverageCheckName.NoOrphanedGap
      ),
      Some(EmbeddingCoverageCheckStatus.Passed -> 0L)
    )
    assertEquals(
      status(
        EmbeddingCoverageReport.assemble(observation(orphan), None, now),
        EmbeddingCoverageCheckName.NoOrphanedGap
      ),
      Some(EmbeddingCoverageCheckStatus.Failed -> 1L)
    )
  }

  test("ECR-04 NoStuckWork reflects the stuck count and the report age uses the fixed clock") {
    val tally = EmbeddingCoverageTally.empty.add(entity(Some(meta()), None), None, now)
    val passed = EmbeddingCoverageReport.assemble(observation(tally), None, now)
    val failed = EmbeddingCoverageReport.assemble(observation(tally, stuck = 2L), None, now)
    assertEquals(
      status(passed, EmbeddingCoverageCheckName.NoStuckWork),
      Some(EmbeddingCoverageCheckStatus.Passed -> 0L)
    )
    assertEquals(
      status(failed, EmbeddingCoverageCheckName.NoStuckWork),
      Some(EmbeddingCoverageCheckStatus.Failed -> 2L)
    )
    assertEquals(passed.oldestQueuedWorkAgeSeconds, Some(90L))
  }

  test("ECR-06 a truncated scan makes checks inconclusive and never passed") {
    val orphan = EmbeddingCoverageTally.empty.add(entity(None, None), None, now)
    val clean = EmbeddingCoverageTally.empty.add(entity(Some(meta()), None), None, now)
    val partial = EmbeddingCoverageReport.assemble(observation(clean, truncated = true), None, now)
    assertEquals(
      status(partial, EmbeddingCoverageCheckName.NoOrphanedGap),
      Some(EmbeddingCoverageCheckStatus.Inconclusive -> 0L)
    )
    assertEquals(
      status(partial, EmbeddingCoverageCheckName.NoStuckWork),
      Some(EmbeddingCoverageCheckStatus.Passed -> 0L)
    )
    assertEquals(
      status(
        EmbeddingCoverageReport.assemble(observation(orphan, truncated = true), None, now),
        EmbeddingCoverageCheckName.NoOrphanedGap
      ),
      Some(EmbeddingCoverageCheckStatus.Inconclusive -> 1L)
    )
    val queueTruncated =
      observation(clean).copy(queue = EmbeddingQueueObservation(truncated = true, 0L, None))
    assertEquals(
      status(EmbeddingCoverageReport.assemble(queueTruncated, None, now), EmbeddingCoverageCheckName.NoStuckWork),
      Some(EmbeddingCoverageCheckStatus.Inconclusive -> 0L)
    )
  }

  test("informational lag, ages and coverage share are derived from timestamped entities only") {
    val changed = now.minusSeconds(1000)
    val tally = List(10L, 20L, 30L, 40L)
      .foldLeft(EmbeddingCoverageTally.empty) { (acc, lag) =>
        acc.add(entity(Some(meta(at = changed.plusSeconds(lag))), None, changedAt = Some(changed)), None, now)
      }
      .add(
        entity(Some(meta(sourceHash = "old")), Some(row(EmbeddingWorkState.Ready)), changedAt = Some(changed)),
        None,
        now
      )
    val report = EmbeddingCoverageReport.assemble(observation(tally), None, now)
    assertEquals(report.lagSeconds, Some(EmbeddingCoverageLag(20L, 40L, 40L, 4L)))
    assertEquals(report.oldestNotCurrentAgeSeconds, Some(1000L))
    assertEquals(report.kinds.head.coverageShare, Some(0.8))
    assertEquals(report.kinds(1).coverageShare, None)
    assertEquals(report.lagEntityKinds, List(EmbeddingWorkKind.Job))
  }
}
