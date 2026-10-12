package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.InjectedFailure
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.app.{StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.domain.StreamingBatchIdentity
import munit.CatsEffectSuite

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.duration.*

/** A real stream that fails inside a batch leaves its checkpoint uncommitted, and a restart converges. */
final class HiringAnalyticsStreamingRestartIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private val eventCount = 12

  private def failingOnce(target: StreamingDurableBoundary): StreamingDurableBoundaryObserver[IO] = {
    val armed = new AtomicBoolean(true)
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
        IO.defer(
          if (boundary == target && armed.compareAndSet(true, false)) IO.raiseError(InjectedFailure) else IO.unit
        )
    }
  }

  @tailrec
  private def causeChainContains(error: Throwable, expected: Throwable, depth: Int = 0): Boolean =
    if ((error eq expected) || error.getMessage == expected.getMessage) true
    else if (depth > 32) false
    else
      Option(error.getCause) match {
        case Some(cause) if cause ne error => causeChainContains(cause, expected, depth + 1)
        case _                             => false
      }

  private def convergesAfterFailureAt(boundary: StreamingDurableBoundary): IO[Unit] =
    fixture.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until eventCount)))
        _ <- grantActivation(fixture, settings)
        failed <- runToCompletion(settings, Some(failingOnce(boundary))).timeout(5.minutes).attempt
        _ = assert(failed.isLeft, s"the stream must fail after $boundary")
        _ = assert(
          failed.left.exists(causeChainContains(_, InjectedFailure)),
          s"the injected failure is absent from the cause chain: ${failed.left.map(_.toString)}"
        )
        _ = assert(!committedBatch(fixture, 0L), "the failed batch must have no Spark commit record")
        _ <- runToCompletion(settings).timeout(5.minutes)
        counts <- stored(fixture, settings)
        snapshot <- reportSnapshot(fixture)
      } yield {
        assertEquals(counts.bronze, eventCount.toLong)
        assertEquals(counts.silver, eventCount.toLong)
        assertEquals(counts.progress, 1L)
        assertEquals(snapshot.map(_.getString("state")), Some("Published"))
        assert(committedBatch(fixture, 0L), "the restart commits the batch")
      }
    }

  test("HCS-07.8 a stream failing after BronzeCommitted leaves no commit record and a restart converges") {
    convergesAfterFailureAt(StreamingDurableBoundary.BronzeCommitted)
  }

  test("HCS-07.8 a stream failing after PublicationCommitted leaves no commit record and a restart converges") {
    convergesAfterFailureAt(StreamingDurableBoundary.PublicationCommitted)
  }
}
