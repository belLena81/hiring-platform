package com.example.hiring.analytics.adapter.spark

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.domain.{StreamingBatchId, StreamingBatchIdentity, StreamingLineage}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.streaming.{StreamingBatchJournal, StreamingCheckpointAcknowledgement}

/** Reconciles Spark's committed batch IDs with durable terminal analytics outcomes before query start/ack. */
private[analytics] final class DeltaStreamingCheckpointAcknowledgement[F[_]: Async](
    journal: StreamingBatchJournal[F]
) extends StreamingCheckpointAcknowledgement[F] {
  private val F = Async[F]
  private val conflict = AnalyticsError.InvalidConfiguration(
    "streaming checkpoint and durable analytics progress are inconsistent"
  )

  override def callbackMayAcknowledge(identity: StreamingBatchIdentity): F[Unit] =
    journal.load(identity).flatMap {
      case Some(state) if state.terminalOutcome.nonEmpty => F.unit
      case _                                             => F.raiseError(conflict)
    }

  override def reconcile(
      lineage: StreamingLineage,
      checkpointedBatchIds: Set[StreamingBatchId],
      checkpointEstablished: Boolean
  ): F[Unit] =
    for {
      hasJournalState <- journal.hasLineageState(lineage)
      _ <- F.raiseWhen(hasJournalState && !checkpointEstablished)(conflict)
      _ <- checkpointedBatchIds.toVector.traverse_ { batchId =>
        journal.load(StreamingBatchIdentity(lineage, batchId)).flatMap {
          case Some(state) if state.terminalOutcome.nonEmpty => F.unit
          case _                                             => F.raiseError[Unit](conflict)
        }
      }
    } yield ()
}
