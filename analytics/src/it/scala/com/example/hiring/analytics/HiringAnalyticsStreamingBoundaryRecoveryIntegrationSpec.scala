package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.app.{StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.domain.StreamingBatchIdentity
import com.example.hiring.analytics.service.streaming.StreamingTerminalOutcome

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** A fault after each durable boundary of one streaming batch recovers to the same published result. */
final class HiringAnalyticsStreamingBoundaryRecoveryIntegrationSpec extends SharedRecoveryRuntimeSuite {
  override val munitIOTimeout: FiniteDuration = 20.minutes

  /** Fails once, right after the first completion of each listed boundary, then stays quiet. */
  private def failingOnce(boundaries: StreamingDurableBoundary*): StreamingDurableBoundaryObserver[IO] = {
    val armed = new AtomicReference(boundaries.toList)
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
        IO.defer {
          val current = armed.get()
          if (current.headOption.contains(boundary) && armed.compareAndSet(current, current.tail))
            IO.raiseError(InjectedFailure)
          else IO.unit
        }
    }
  }

  private val quiet: StreamingDurableBoundaryObserver[IO] = failingOnce()

  private def recoversAfter(boundary: StreamingDurableBoundary, name: String): IO[Unit] =
    resource.use { runtime =>
      for {
        harness <- runtime.harness(name)
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        frame <- timedFrame(harness, 0L, observed)
        prior = Some(Option.empty[Instant])
        failed <- processAt(
          harness,
          frame,
          0L,
          observed,
          observer = Some(failingOnce(boundary)),
          priorWatermark = prior
        ).attempt
        _ = assertEquals(failed.left.toOption, Some(InjectedFailure), s"the fault after $boundary must fire")
        recovered <- processAt(harness, frame, 0L, observed, observer = Some(quiet), priorWatermark = prior)
        repeated <- processAt(harness, frame, 0L, observed, observer = Some(quiet), priorWatermark = prior)
        _ = assertEquals(recovered.outcome, StreamingTerminalOutcome.Published, boundary.toString)
        _ = assertEquals(repeated, recovered, boundary.toString)
        _ = assert(recovered.candidateWatermark.nonEmpty, "a published batch of current events advances the watermark")
        watermark <- harness.journal.latestWatermark(harness.preparation().identity.lineage)
        _ = assertEquals(watermark, recovered.candidateWatermark, boundary.toString)
        bronze <- harness.count(harness.paths.bronze)
        silver <- harness.count(harness.paths.silver)
        progress <- harness.count(harness.paths.streamingProgress)
        _ = assertEquals((bronze, silver, progress), (12L, 12L, 1L), boundary.toString)
      } yield ()
    }

  test("HCS-07.1 a fault after Prepared recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.Prepared, "boundary-prepared")
  }

  test("HCS-07.2 a fault after BronzeCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.BronzeCommitted, "boundary-bronze")
  }

  test("HCS-07.3 a fault after SilverCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.SilverCommitted, "boundary-silver")
  }

  test("HCS-07.4 a fault after LateFactsCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.LateFactsCommitted, "boundary-late-facts")
  }

  test("HCS-07.5 a fault after IngestionCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.IngestionCommitted, "boundary-ingestion")
  }

  test("HCS-07.6 a fault after PublicationCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.PublicationCommitted, "boundary-publication")
  }

  test("HCS-07.7 a fault after TerminalCommitted recovers to one published batch") {
    recoversAfter(StreamingDurableBoundary.TerminalCommitted, "boundary-terminal")
  }

  test("HCS-04.3 twelve late facts and the watermark survive two restarts after LateFactsCommitted") {
    resource.use { runtime =>
      for {
        harness <- runtime.harness("boundary-late-fact-restarts")
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        lineage = harness.preparation().identity.lineage
        current <- timedFrame(harness, 0L, observed)
        _ <- processAt(harness, current, 0L, observed)
        watermark <- harness.journal.latestWatermark(lineage)
        _ = assert(watermark.nonEmpty, "the first batch advances the watermark")
        closedAt = observed.truncatedTo(ChronoUnit.DAYS).minus(5L, ChronoUnit.DAYS)
        late <- timedFrame(harness, 1L, closedAt, skill = "ClosedOnly")
        arrival = observed.plusSeconds(60L)
        prior = Some(watermark)
        first <- processAt(
          harness,
          late,
          1L,
          arrival,
          observer = Some(failingOnce(StreamingDurableBoundary.LateFactsCommitted)),
          priorWatermark = prior
        ).attempt
        _ = assertEquals(first.left.toOption, Some(InjectedFailure))
        afterFirst <- harness.count(harness.paths.lateFacts)
        _ = assertEquals(afterFirst, 12L)
        second <- processAt(
          harness,
          late,
          1L,
          arrival,
          observer = Some(failingOnce(StreamingDurableBoundary.IngestionCommitted)),
          priorWatermark = prior
        ).attempt
        _ = assertEquals(second.left.toOption, Some(InjectedFailure))
        afterSecond <- harness.count(harness.paths.lateFacts)
        _ = assertEquals(afterSecond, 12L)
        midway <- harness.journal.latestWatermark(lineage)
        _ = assertEquals(midway, watermark)
        third <- processAt(harness, late, 1L, arrival, observer = Some(quiet), priorWatermark = prior)
        _ = assertEquals(third.outcome, StreamingTerminalOutcome.Published)
        facts <- harness.count(harness.paths.lateFacts)
        _ = assertEquals(facts, 12L)
        finalWatermark <- harness.journal.latestWatermark(lineage)
        _ = assertEquals(finalWatermark, watermark)
      } yield ()
    }
  }
}
