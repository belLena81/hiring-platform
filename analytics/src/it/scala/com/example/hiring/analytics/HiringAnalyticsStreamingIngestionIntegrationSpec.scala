package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.app.{StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.domain.StreamingBatchIdentity
import munit.CatsEffectSuite

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

/** Real Kafka, Spark, Delta and Mongo composition: committed job events reach the stored Admin report snapshot. */
final class HiringAnalyticsStreamingIngestionIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private val eventCount = 12
  private val oneBatch = 13

  private def countingBatches(batches: AtomicInteger): StreamingDurableBoundaryObserver[IO] =
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
        IO.whenA(boundary == StreamingDurableBoundary.Prepared)(IO(batches.incrementAndGet()).void)
    }

  // The real activation grant is used: the production gate and identity derivation are part of what this proves.
  test("HCS-01.1 twelve committed job events reach the stored Admin report snapshot") {
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, oneBatch)
        at = Instant.now().minusSeconds(30L)
        _ <- publish(fixture.namespace, at, Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        _ <- runToCompletion(settings).timeout(5.minutes)
        counts <- stored(fixture, settings)
        snapshot <- reportSnapshot(fixture)
      } yield {
        assertEquals(counts.bronze, eventCount.toLong, "Bronze")
        assertEquals(counts.silver, eventCount.toLong, "Silver")
        assert(snapshot.nonEmpty, "stored report snapshot is absent")
        assertEquals(snapshot.get.getString("state"), "Published")
        assertEquals(scalaPostings(snapshot.get), eventCount.toLong)
      }
    }
  }

  test("HCS-01.2 HCS-02.3 a second run on the same checkpoint runs no batch and changes nothing") {
    fixture.use { fixture =>
      val first, second = new AtomicInteger(0)
      for {
        settings <- settings(fixture, oneBatch)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        _ <- runToCompletion(settings, Some(countingBatches(first))).timeout(5.minutes)
        before <- stored(fixture, settings)
        revision <- publishedRevision(fixture)
        _ <- runToCompletion(settings, Some(countingBatches(second))).timeout(5.minutes)
        after <- stored(fixture, settings)
        revisionAfter <- publishedRevision(fixture)
      } yield {
        assertEquals(first.get(), 1, "first run batches")
        assertEquals(second.get(), 0, "restart batches")
        assertEquals(after, before)
        assertEquals(before.progress, 1L)
        assertEquals(revisionAfter, revision)
      }
    }
  }

  test("HCS-02.1 aborted transactions and their offset gaps are skipped and acknowledged") {
    fixture.use { fixture =>
      val batches = new AtomicInteger(0)
      for {
        settings <- settings(fixture, oneBatch)
        at = Instant.now().minusSeconds(30L)
        // Offsets 0-3 committed (marker 4), 5-7 aborted (marker 8), 9-11 committed (marker 12).
        _ <- publish(
          fixture.namespace,
          at,
          Vector(Transaction(0 until 4), Transaction(4 until 7, commit = false), Transaction(7 until 10))
        )
        _ <- grantActivation(fixture, settings)
        _ <- runToCompletion(settings, Some(countingBatches(batches))).timeout(5.minutes)
        counts <- stored(fixture, settings)
        ran = batches.get()
        _ <- runToCompletion(settings, Some(countingBatches(batches))).timeout(5.minutes)
      } yield {
        assertEquals(counts.bronze, 7L, "Bronze holds committed offsets only")
        assertEquals(counts.silver, 7L, "Silver")
        assert(ran >= 1, "committed records were processed")
        assertEquals(batches.get(), ran, "the restart acknowledged the gaps and found nothing to process")
      }
    }
  }

  test("HCS-02.2 the same twelve event IDs published again at new offsets keep Silver at twelve") {
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, oneBatch)
        at = Instant.now().minusSeconds(30L)
        _ <- publish(fixture.namespace, at, Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        _ <- runToCompletion(settings).timeout(5.minutes)
        _ <- publish(fixture.namespace, at, Vector(Transaction(0 until eventCount)))
        _ <- runToCompletion(settings).timeout(5.minutes)
        counts <- stored(fixture, settings)
      } yield {
        assertEquals(counts.silver, eventCount.toLong, "Silver deduplicates by event ID")
        assertEquals(counts.bronze, 2L * eventCount, "Bronze keeps both deliveries")
      }
    }
  }
}
