package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.AnalyticsLateFactReplayOutcome
import com.example.hiring.analytics.service.streaming.StreamingTerminalOutcome
import org.bson.Document

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Facts around the allowed-lateness bound, through the real streaming coordinator, Delta tables and Mongo snapshot. */
final class HiringAnalyticsStreamingAllowedLatenessIntegrationSpec extends SharedRecoveryRuntimeSuite {
  override val munitIOTimeout: FiniteDuration = 20.minutes

  private def snapshot(harness: Harness): IO[Document] =
    harness.sync(_.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first())

  private def skills(snapshot: Document): Set[String] =
    snapshot.getList("skillPostingActivity", classOf[Document]).asScala.map(_.getString("skill").toLowerCase).toSet

  test("DDR-04.2 facts inside, at and past the bound each follow exactly one path") {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("allowed-lateness-bound")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          current <- timedFrame(harness, 0L, observed)
          currentResult <- processAt(harness, current, 0L, observed)
          _ = assertEquals(currentResult.outcome, StreamingTerminalOutcome.Published)
          // The durable watermark is the bound: a UTC day is closed once it ends at or before it.
          bound = currentResult.candidateWatermark.get
          midnight = bound.truncatedTo(ChronoUnit.DAYS)
          inside <- timedFrame(harness, 1L, midnight, skill = "InsideBound")
          insideResult <- processAt(harness, inside, 1L, observed.plusSeconds(1L))
          _ = assertEquals(insideResult.outcome, StreamingTerminalOutcome.Published)
          silverInside <- harness.count(harness.paths.silver)
          lateInside <- harness.count(harness.paths.lateFacts)
          _ = assertEquals((silverInside, lateInside), (24L, 0L), "a fact inside the bound is admitted to Silver only")

          atBound <- timedFrame(harness, 2L, midnight.minus(1L, ChronoUnit.MICROS), skill = "AtBound")
          atResult <- processAt(harness, atBound, 2L, observed.plusSeconds(2L))
          _ = assertEquals(atResult.outcome, StreamingTerminalOutcome.Published)
          silverAt <- harness.count(harness.paths.silver)
          lateAt <- harness.count(harness.paths.lateFacts)
          _ = assertEquals((silverAt, lateAt), (24L, 12L), "a fact at the bound goes to the late-fact dataset only")

          past <- timedFrame(harness, 3L, midnight.minus(5L, ChronoUnit.DAYS), skill = "PastBound")
          pastResult <- processAt(harness, past, 3L, observed.plusSeconds(3L))
          _ = assertEquals(pastResult.outcome, StreamingTerminalOutcome.Published)
          _ = assertEquals(pastResult.candidateWatermark, None, "late facts never advance the watermark")
          silverPast <- harness.count(harness.paths.silver)
          latePast <- harness.count(harness.paths.lateFacts)
          quarantined <- harness.count(harness.paths.quarantine)
          _ = assertEquals((silverPast, latePast, quarantined), (24L, 24L, 0L))
          report <- snapshot(harness)
          _ = assertEquals(skills(report), Set("scala", "insidebound"), "only the admitted fact reaches the report")
        } yield ()
      }
      .unsafeRunSync()
  }

  test(
    "DDR-04.3 a fact late after publication reaches the report through explicit replay in a newer recorded revision"
  ) {
    resource
      .use { runtime =>
        for {
          harness <- runtime.harness("allowed-lateness-replay")
          observed = harness.at.truncatedTo(ChronoUnit.MICROS)
          current <- timedFrame(harness, 0L, observed)
          first <- processAt(harness, current, 0L, observed)
          _ = assertEquals(first.outcome, StreamingTerminalOutcome.Published)
          closedAt = first.candidateWatermark.get.truncatedTo(ChronoUnit.DAYS).minusSeconds(1L)
          late <- timedFrame(harness, 1L, closedAt, skill = "LateAfterPublication")
          lateResult <- processAt(harness, late, 1L, observed.plusSeconds(60L))
          _ = assertEquals(lateResult.outcome, StreamingTerminalOutcome.Published)
          lateFacts <- harness.count(harness.paths.lateFacts)
          _ = assertEquals(lateFacts, 12L)
          published <- snapshot(harness)
          _ = assert(!skills(published).contains("lateafterpublication"), "the published report misses the late fact")
          missedRevision = published.getLong("revision").longValue()
          coordinates = (0 until 12).toVector.map(n => (harness.topic, n % 3, 4L + n / 3))
          request = AnalyticsLateFactReplayRequest.from("late-after-publication", coordinates).toOption.get
          outcome <- harness.replay(request)
          _ = assertEquals(outcome, AnalyticsLateFactReplayOutcome.Published)
          corrected <- snapshot(harness)
          record <- harness.replayJournal.load(request.requestId)
          _ = assert(
            corrected.getLong("revision").longValue() > missedRevision,
            "replay publishes a newer revision of the pinned generation"
          )
          _ = assertEquals(corrected.getString("runId"), record.get.reservation.runId.value)
          _ = assert(corrected.getString("runId").startsWith("late-replay-"))
          _ = assertEquals(corrected.getLong("generation").longValue(), record.get.reservation.generation)
          _ = assertEquals(corrected.getLong("revision").longValue(), record.get.reservation.revision)
          _ = assert(skills(corrected).contains("lateafterpublication"), "the corrected report includes the late fact")
        } yield ()
      }
      .unsafeRunSync()
  }
}
