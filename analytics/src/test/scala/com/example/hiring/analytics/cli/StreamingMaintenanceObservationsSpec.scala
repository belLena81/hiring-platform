package com.example.hiring.analytics.cli

import munit.FunSuite
import java.time.Instant

final class StreamingMaintenanceObservationsSpec extends FunSuite {
  import StreamingMaintenanceObservations.*
  private val at = Instant.parse("2026-10-03T12:00:00Z")
  private def progress(id: Long, outcome: String = "Published"): Progress =
    Progress(
      "fixture",
      id,
      outcome,
      if (outcome == "Prepared") None else Some(at.minusSeconds(90)),
      if (outcome == "Published") Some(at.minusSeconds(100)) else None,
      Some(at.minusSeconds(100))
    )
  private val permanent = Map("identity" -> "unchanged")
  private val old = Snapshot(
    Vector(progress(0), progress(1)),
    Set("fixture" -> 0L, "fixture" -> 1L),
    Set(0L, 1L),
    Set(0L, 1L),
    permanent
  )
  private val before = Snapshot(
    Vector(progress(0), progress(1), progress(3), progress(4), progress(5, "Prepared")),
    Set("fixture" -> 0L, "fixture" -> 1L, "fixture" -> 3L, "fixture" -> 4L),
    Set(3L, 4L),
    Set(3L, 4L),
    permanent
  )
  private val after =
    before.copy(progress = before.progress.filter(_.batchId >= 3L), decisions = before.decisions.filter(_._2 >= 3L))

  test("natural pruning preserves native anchors latest watermark and unfinished rows") {
    val result = verifyPruning(old, before, after, at)
    assert(result.oldCompletedRemoved)
    assert(result.latestAnchorsRetained)
    assertEquals(result.unfinishedStatus, "OBSERVED_UNCHANGED_RETAINED")
  }

  test("pruning assertions reject premature age, forgotten anchors and changed permanent identity") {
    val unsafe = Vector(
      after.copy(progress = after.progress.filterNot(_.batchId == 4L)),
      after.copy(progress = after.progress.filterNot(_.batchId == 5L)),
      after.copy(decisions = after.decisions - ("fixture" -> 4L)),
      after.copy(nativeCommits = after.nativeCommits + 0L),
      after.copy(permanentLineageHashes = Map("identity" -> "changed"))
    )
    unsafe.foreach(value => intercept[IllegalArgumentException](verifyPruning(old, before, value, at)))
    intercept[IllegalArgumentException](verifyPruning(old, before, after, at.minusSeconds(40)))
  }

  test("continuation requires a new native committed published batch and nonregressing watermark") {
    val continued =
      after.copy(progress = after.progress.filterNot(_.batchId == 5L) :+ progress(5), nativeCommits = Set(4L, 5L))
    verifyContinuation(after, continued)
    intercept[IllegalArgumentException](verifyContinuation(after, after))
    intercept[IllegalArgumentException](
      verifyContinuation(
        after,
        continued.copy(progress = continued.progress.map(_.copy(watermark = Some(at.minusSeconds(200)))))
      )
    )
  }

  test("eligible cleanup age excludes native latest watermark latest terminal and unfinished anchors") {
    assertEquals(pendingCleanup(before, at), PendingCleanup(2, Some(30000L)))
    assertEquals(pendingCleanup(after, at), PendingCleanup(0, None))
    val exactBoundary = before.copy(progress =
      before.progress.map(row => if (row.batchId == 0L) row.copy(completedAt = Some(at.minusSeconds(60))) else row)
    )
    assertEquals(pendingCleanup(exactBoundary, at), PendingCleanup(2, Some(30000L)))
    val recent = before.copy(progress =
      before.progress.map(row => if (row.batchId <= 1L) row.copy(completedAt = Some(at.minusSeconds(59))) else row)
    )
    assertEquals(pendingCleanup(recent, at), PendingCleanup(0, None))
    val distinctAnchors = old.copy(
      progress = Vector(progress(0), progress(1, "QualityBlocked")),
      nativeOffsets = Set.empty,
      nativeCommits = Set.empty
    )
    assertEquals(pendingCleanup(distinctAnchors, at), PendingCleanup(0, None))
    val anotherLineage = progress(0).copy(lineage = "another")
    assertEquals(
      pendingCleanup(before.copy(progress = before.progress :+ anotherLineage), at),
      PendingCleanup(2, Some(30000L))
    )
  }

  test("a maintenance successor covers the durable cohort only in the same generation") {
    assert(publicationCovers(4L, 7L, 4L, 7L))
    assert(publicationCovers(4L, 7L, 4L, 8L))
    assert(!publicationCovers(4L, 7L, 4L, 6L))
    assert(!publicationCovers(4L, 7L, 5L, 8L))
    assert(!publicationCovers(4L, 7L, 3L, 8L))
  }

  test("fixture growth rejects excessive bytes files and invalid inventories without overflow") {
    assert(boundedGrowth(2L, 100L, 10002L, 1073741924L))
    assert(!boundedGrowth(2L, 100L, 10003L, 100L))
    assert(!boundedGrowth(2L, 100L, 2L, 1073741925L))
    assert(!boundedGrowth(-1L, 0L, 0L, 0L))
    assert(!boundedGrowth(0L, 0L, 0L, Long.MaxValue))
    assert(boundedGrowth(100L, 1000L, 10L, 100L))
  }

  private def delivered(id: Long, minimum: Long, maximum: Long): Progress = progress(id).copy(
    delivered = Vector(DeliveredRange("source", 0, minimum, maximum)),
    sourceEnds = Vector(("source", 0, maximum + 1L))
  )

  test("cohort binds actual delivered batch after empty initial commit") {
    val actual = delivered(1L, 0L, 3L)
    assertEquals(
      bindCohort(Vector(progress(0), actual), Vector(("source", 0, 0L), ("source", 0, 3L))),
      Some(Vector(actual))
    )
  }

  test("cohort may span multiple committed publication batches") {
    val first = delivered(2L, 0L, 1L)
    val second = delivered(3L, 2L, 3L)
    assertEquals(
      bindCohort(Vector(first, second), (0L to 3L).map(offset => ("source", 0, offset)).toVector),
      Some(Vector(first, second))
    )
  }

  test("cohort rejects missing gaps duplicate coordinates and ambiguous batch coverage") {
    val row = delivered(1L, 0L, 1L)
    assertEquals(bindCohort(Vector(row), Vector(("source", 0, 2L))), None)
    assertEquals(bindCohort(Vector(row), Vector(("source", 0, 0L), ("source", 0, 0L))), None)
    assertEquals(bindCohort(Vector(row, delivered(2L, 1L, 2L)), Vector(("source", 0, 1L))), None)
    assertEquals(bindCohort(Vector(row), Vector.empty), None)
  }

  test("cohort rejects unfinished outcomes and insufficient source-end acknowledgement") {
    val row = delivered(1L, 0L, 1L)
    assertEquals(
      bindCohort(Vector(row.copy(outcome = "Prepared", completedAt = None)), Vector(("source", 0, 0L))),
      None
    )
    assertEquals(bindCohort(Vector(row.copy(sourceEnds = Vector(("source", 0, 1L)))), Vector(("source", 0, 1L))), None)
    assertEquals(bindCohort(Vector(row), Vector(("foreign", 0, 0L))), None)
  }
}
