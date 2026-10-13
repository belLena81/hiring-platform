package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.app.{StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.streaming.StreamingTerminalOutcome
import org.apache.spark.sql.functions.{array_contains, col}
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

/** Account deletion racing a streaming batch keeps the report hidden and is honoured when the batch is retried. */
final class HiringAnalyticsStreamingErasureRaceIntegrationSpec extends SharedRecoveryRuntimeSuite {
  override val munitIOTimeout: FiniteDuration = 20.minutes

  private def requestErasure(harness: Harness, subject: String): IO[Unit] =
    harness
      .sync(
        _.getCollection("analytics_erasure_requests")
          .insertOne(new Document("_id", subject).append("state", "Pending"))
      )
      .void

  private def subjectRows(harness: Harness, path: String, subject: String): IO[Long] = {
    val token = AnalyticsTestSubjectPseudonymizer.tokenValue(harness.keys, subject)
    harness.execution {
      harness.spark.read
        .format("delta")
        .load(path)
        .filter(col("subjectToken") === token || array_contains(col("subjectTokens"), token))
        .count()
    }
  }

  private def control(harness: Harness): IO[Document] =
    harness.sync(_.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first())

  private def snapshots(harness: Harness): IO[Long] =
    harness.sync(_.getCollection("analytics_report_snapshots").countDocuments())

  private def erasureAfter(
      boundary: StreamingDurableBoundary,
      harness: Harness,
      subject: String
  ): StreamingDurableBoundaryObserver[IO] = {
    val armed = new AtomicBoolean(true)
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(
          reached: StreamingDurableBoundary,
          identity: StreamingBatchIdentity
      ): IO[Unit] =
        IO.defer {
          if (reached == boundary && armed.compareAndSet(true, false))
            requestErasure(harness, subject) *> IO.raiseError(InjectedFailure)
          else IO.unit
        }
    }
  }

  test("HCS-05.3 a batch holding only a deleted subject's events leaves report, watermark and revision unchanged") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("erasure-only-deleted-subject")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        first <- timedFrame(harness, 0L, observed)
        _ <- processAt(harness, first, 0L, observed)
        watermark <- harness.journal.latestWatermark(lineage)
        before <- control(harness)
        deleted = harness.subjects.head
        _ <- requestErasure(harness, deleted)
        only <- timedFrame(harness, 1L, observed, distinctSubjects = 1)
        result <- processAt(harness, only, 1L, observed.plusSeconds(60L))
        _ = assertEquals(result.outcome, StreamingTerminalOutcome.ErasurePending)
        _ = assertEquals(result.candidateWatermark, None)
        after <- harness.journal.latestWatermark(lineage)
        _ = assertEquals(after, watermark)
        current <- control(harness)
        _ = assertEquals(current.getLong("lastPublishedRevision"), before.getLong("lastPublishedRevision"))
        remaining <- subjectRows(harness, harness.paths.silver, deleted)
        _ = assertEquals(remaining, 0L)
      } yield ()
    }
  }

  test("HCS-10.1 a deletion requested before preparation is applied to the Bronze and Silver writes") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("erasure-before-preparation")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        deleted = harness.subjects(3)
        _ <- requestErasure(harness, deleted)
        frame <- timedFrame(harness, 0L, observed)
        result <- processAt(harness, frame, 0L, observed)
        _ = assertEquals(result.outcome, StreamingTerminalOutcome.ErasurePending)
        bronze <- harness.count(harness.paths.bronze)
        silver <- harness.count(harness.paths.silver)
        bronzeSubject <- subjectRows(harness, harness.paths.bronze, deleted).attempt
        silverSubject <- subjectRows(harness, harness.paths.silver, deleted)
        _ = assertEquals(silverSubject, 0L)
        _ = assertEquals(silver, 11L)
        _ = assertEquals(bronze, 11L, "Bronze excludes the deleted subject's event")
        _ = assertEquals(bronzeSubject.toOption, Some(0L), s"marked subject rows in Bronze: $bronzeSubject")
        published <- snapshots(harness)
        _ = assertEquals(published, 0L)
      } yield ()
    }
  }

  test("HCS-10.2 a deletion requested after the Silver merge hides the report and purges the subject on retry") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("erasure-after-silver")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        deleted = harness.subjects(5)
        frame <- timedFrame(harness, 0L, observed)
        prior = Some(Option.empty[Instant])
        raced <- processAt(
          harness,
          frame,
          0L,
          observed,
          observer = Some(erasureAfter(StreamingDurableBoundary.SilverCommitted, harness, deleted)),
          priorWatermark = prior
        ).attempt
        _ = assertEquals(raced.left.toOption, Some(InjectedFailure))
        written <- subjectRows(harness, harness.paths.silver, deleted)
        _ = assertEquals(written, 1L, "the race must land after the subject was written to Silver")
        retried <- processAt(harness, frame, 0L, observed, priorWatermark = prior)
        _ = assertEquals(retried.outcome, StreamingTerminalOutcome.ErasurePending)
        remaining <- subjectRows(harness, harness.paths.silver, deleted)
        _ = assertEquals(remaining, 0L)
        published <- snapshots(harness)
        _ = assertEquals(published, 0L)
        watermark <- harness.journal.latestWatermark(lineage)
        _ = assertEquals(watermark, None)
      } yield ()
    }
  }

  test(
    "HCS-10.3 with the marker still pending the retry is reassessed as ErasurePending and the watermark does not advance"
  ) {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("erasure-watermark-recomputed")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        deleted = harness.subjects(11)
        frame <- timedFrame(harness, 0L, observed)
        prior = Some(Option.empty[Instant])
        raced <- processAt(
          harness,
          frame,
          0L,
          observed,
          observer = Some(erasureAfter(StreamingDurableBoundary.SilverCommitted, harness, deleted)),
          priorWatermark = prior
        ).attempt
        _ = assertEquals(raced.left.toOption, Some(InjectedFailure))
        retried <- processAt(harness, frame, 0L, observed, priorWatermark = prior)
        _ = assertEquals(retried.outcome, StreamingTerminalOutcome.ErasurePending)
        watermark <- harness.journal.latestWatermark(lineage)
        _ = assertEquals(watermark, None, "a pending deletion keeps the watermark from advancing at all")
        _ = assertEquals(retried.candidateWatermark, None)
        remaining <- subjectRows(harness, harness.paths.silver, deleted)
        _ = assertEquals(remaining, 0L)
      } yield ()
    }
  }

  test("HCS-10.4 a deletion requested during the replay merge is honoured when the replay is retried") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("erasure-during-replay")
        seeded <- seedReplay(harness)
        request = seeded
        deleted = harness.subjects.head
        armed = new AtomicBoolean(true)
        racing = new AnalyticsLateFactReplayStages[IO] {
          private val delegate = harness.replayStages
          override def validateHmacConfiguration = delegate.validateHmacConfiguration
          override def validateSelectedFacts(
              request: AnalyticsLateFactReplayRequest,
              activeTokens: Vector[SubjectToken],
              observedAt: Instant
          ) = delegate.validateSelectedFacts(request, activeTokens, observedAt)
          override def applyActiveDeletions(activeTokens: Vector[SubjectToken]) =
            delegate.applyActiveDeletions(activeTokens)
          override def mergeSelectedFacts(
              request: AnalyticsLateFactReplayRequest,
              activeTokens: Vector[SubjectToken],
              observedAt: Instant
          ) =
            delegate.mergeSelectedFacts(request, activeTokens, observedAt) *> IO.defer {
              if (armed.compareAndSet(true, false)) requestErasure(harness, deleted) *> IO.raiseError(InjectedFailure)
              else IO.unit
            }
          override def rebuildGoldAndExtractReport(asOf: Instant) = delegate.rebuildGoldAndExtractReport(asOf)
        }
        interrupted <- harness.replay(request, harness.publisher, cats.effect.Clock[IO], racing).attempt
        _ = assertEquals(interrupted.left.toOption, Some(InjectedFailure))
        retried <- harness.replay(request).attempt
        _ = assertEquals(retried.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
        published <- snapshots(harness)
        _ = assertEquals(published, 0L)
      } yield ()
    }
  }

  /** Twelve retained late facts selected by one explicit replay request. */
  private def seedReplay(harness: Harness): IO[AnalyticsLateFactReplayRequest] =
    LateFactReplayFixtures.seed(harness).map(_._1)
}
