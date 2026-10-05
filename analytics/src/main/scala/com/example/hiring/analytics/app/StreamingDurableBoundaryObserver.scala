package com.example.hiring.analytics.app

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.DeltaWriter
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsReportReservation}
import com.example.hiring.analytics.service.streaming.*
import org.apache.spark.sql.DataFrame
import java.time.Instant

/** Observation never replaces, skips or changes a durable operation. No crash controls exist in configuration. */
private[analytics] enum StreamingDurableBoundary {
  case Prepared, BronzeCommitted, SilverCommitted, LateFactsCommitted
  case IngestionCommitted, PublicationCommitted, TerminalCommitted
}

private[analytics] trait StreamingDurableBoundaryObserver[F[_]] {
  def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): F[Unit]
}

private[analytics] object StreamingDurableBoundaryObserver {
  def journal[F[_]: Async](
      delegate: StreamingBatchJournal[F],
      observer: StreamingDurableBoundaryObserver[F]
  ): StreamingBatchJournal[F] = new StreamingBatchJournal[F] {
    override def load(identity: StreamingBatchIdentity) = delegate.load(identity)
    override def latestWatermark(lineage: StreamingLineage) = delegate.latestWatermark(lineage)
    override def hasLineageState(lineage: StreamingLineage) = delegate.hasLineageState(lineage)
    override def reconciliationStates(lineage: StreamingLineage, ids: Set[StreamingBatchId]) =
      delegate.reconciliationStates(lineage, ids)
    override def prepare(value: StreamingInputPreparation) =
      delegate.prepare(value) *> observer.completed(StreamingDurableBoundary.Prepared, value.identity)
    override def markIngestionCommitted(identity: StreamingBatchIdentity) =
      delegate
        .markIngestionCommitted(identity) *> observer.completed(StreamingDurableBoundary.IngestionCommitted, identity)
    override def appendDecision(value: StreamingDecisionRevision) = delegate.appendDecision(value)
    override def complete(identity: StreamingBatchIdentity, outcome: StreamingTerminalOutcome, at: Instant) =
      delegate.complete(identity, outcome, at) *> observer.completed(
        StreamingDurableBoundary.TerminalCommitted,
        identity
      )
    override def commitPublished(value: StreamingDecisionRevision, at: Instant) =
      delegate.commitPublished(value, at) *> observer.completed(
        StreamingDurableBoundary.TerminalCommitted,
        value.identity
      )
  }

  def writer[F[_]: Async](
      delegate: DeltaWriter[F],
      paths: AnalyticsLakehousePaths,
      identity: StreamingBatchIdentity,
      observer: StreamingDurableBoundaryObserver[F]
  ): DeltaWriter[F] = new DeltaWriter[F] {
    private def observed(path: String): F[Unit] = {
      val boundary =
        if (path == paths.bronze) Some(StreamingDurableBoundary.BronzeCommitted)
        else if (path == paths.silver) Some(StreamingDurableBoundary.SilverCommitted)
        else if (path == paths.lateFacts) Some(StreamingDurableBoundary.LateFactsCommitted)
        else None
      boundary.traverse_(observer.completed(_, identity))
    }
    override def merge(frame: DataFrame, path: String, condition: String) =
      delegate.merge(frame, path, condition) *> observed(path)
    override def mergeWhenFresh(frame: DataFrame, path: String, condition: String, at: () => Instant) =
      delegate.mergeWhenFresh(frame, path, condition, at) *> observed(path)
    override def withExpiry(frame: DataFrame, at: Instant, days: Int) = delegate.withExpiry(frame, at, days)
  }

  def stages[F[_]: Async](
      delegate: StreamingBatchStages[F],
      observer: StreamingDurableBoundaryObserver[F]
  ): StreamingBatchStages[F] = new StreamingBatchStages[F] {
    override def reservePublication(value: StreamingInputPreparation, revision: Long): F[AnalyticsReportReservation] =
      delegate.reservePublication(value, revision)
    override def publicationReceipt(value: StreamingInputPreparation, decision: StreamingDecisionRevision)(using
        cats.Applicative[F]
    ) = delegate.publicationReceipt(value, decision)
    override def assess(value: StreamingInputPreparation, tokens: Vector[SubjectToken], recovery: Boolean) =
      delegate.assess(value, tokens, recovery)
    override def ingest(
        value: StreamingInputPreparation,
        tokens: Vector[SubjectToken],
        assessment: StreamingIngestionResult,
        decision: StreamingDecisionRevision,
        recovery: Boolean
    ) =
      delegate.ingest(value, tokens, assessment, decision, recovery)
    override def publish(
        value: StreamingInputPreparation,
        decision: StreamingDecisionRevision,
        tokens: Vector[SubjectToken]
    ) =
      delegate.publish(value, decision, tokens).flatTap {
        case StreamingPublicationResult.Published =>
          observer.completed(StreamingDurableBoundary.PublicationCommitted, value.identity)
        case _ => Async[F].unit
      }
  }
}
