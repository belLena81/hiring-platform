package com.example.hiring.analytics

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.hiring.analytics.app.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation
import com.example.hiring.analytics.service.streaming.*
import munit.CatsEffectSuite
import java.time.Instant

final class StreamingDurableBoundaryObserverSpec extends CatsEffectSuite {
  private val identity = StreamingBatchIdentity(
    StreamingLineage.from("hiring-recovery").toOption.get,
    StreamingBatchId.from(0L).toOption.get
  )
  private val at = Instant.parse("2026-10-01T00:00:00Z")
  private val fingerprint = RangeFingerprint.from("a" * 64).toOption.get
  private val preparation = StreamingInputPreparation(identity, at, None, fingerprint, Vector.empty, Vector.empty)
  private val decision = StreamingDecisionRevision(
    identity,
    0L,
    "markers",
    Some(at),
    AnalyticsReportReservation(RunId.from("hiring-recovery").toOption.get, fingerprint, 0L, 1L)
  )

  private def journal(events: Ref[IO, Vector[String]], fail: Boolean): StreamingBatchJournal[IO] =
    new StreamingBatchJournal[IO] {
      private def durable(name: String) =
        if (fail) IO.raiseError[Unit](new IllegalStateException("durable write failed")) else events.update(_ :+ name)
      override def load(value: StreamingBatchIdentity) = IO.pure(None)
      override def latestWatermark(value: StreamingLineage) = IO.pure(None)
      override def hasLineageState(value: StreamingLineage) = IO.pure(false)
      override def reconciliationStates(value: StreamingLineage, ids: Set[StreamingBatchId]) = IO.pure(Vector.empty)
      override def prepare(value: StreamingInputPreparation) = durable("prepare")
      override def markIngestionCommitted(value: StreamingBatchIdentity) = durable("ingestion")
      override def appendDecision(value: StreamingDecisionRevision) = durable("decision")
      override def complete(value: StreamingBatchIdentity, outcome: StreamingTerminalOutcome, at: Instant) = durable(
        "terminal"
      )
      override def commitPublished(value: StreamingDecisionRevision, at: Instant) = durable("published")
    }

  test("durable observer signals preparation ingestion and terminal only after delegate success") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      observer = new StreamingDurableBoundaryObserver[IO] {
        override def completed(boundary: StreamingDurableBoundary, value: StreamingBatchIdentity) =
          IO(assertEquals(value, identity)) *> events.update(_ :+ boundary.toString)
      }
      wrapped = StreamingDurableBoundaryObserver.journal(journal(events, false), observer)
      _ <- wrapped.prepare(preparation) *> wrapped.markIngestionCommitted(identity) *> wrapped.commitPublished(
        decision,
        at
      )
      observed <- events.get
    } yield assertEquals(
      observed,
      Vector("prepare", "Prepared", "ingestion", "IngestionCommitted", "published", "TerminalCommitted")
    )
  }

  test("failed durable operations cannot advertise a crash barrier") {
    for {
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      observer = new StreamingDurableBoundaryObserver[IO] {
        override def completed(boundary: StreamingDurableBoundary, value: StreamingBatchIdentity) =
          events.update(_ :+ boundary.toString)
      }
      wrapped = StreamingDurableBoundaryObserver.journal(journal(events, true), observer)
      attempts <- Vector(
        wrapped.prepare(preparation),
        wrapped.markIngestionCommitted(identity),
        wrapped.commitPublished(decision, at)
      )
        .traverse(_.attempt)
      observed <- events.get
    } yield {
      assert(attempts.forall(_.isLeft))
      assertEquals(observed, Vector.empty)
    }
  }
}
