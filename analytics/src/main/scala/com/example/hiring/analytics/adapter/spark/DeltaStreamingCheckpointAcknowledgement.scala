package com.example.hiring.analytics.adapter.spark

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.domain.{
  AnalyticsOffset,
  AnalyticsPartition,
  AnalyticsTopic,
  StreamingBatchIdentity,
  StreamingLineage
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.streaming.{
  StreamingBatchJournal,
  StreamingCheckpointAcknowledgement,
  StreamingCheckpointBatch
}

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
      checkpointBatches: Vector[StreamingCheckpointBatch],
      checkpointEstablished: Boolean
  ): F[Unit] =
    for {
      hasLineageState <- journal.hasLineageState(lineage)
      states <- journal.reconciliationStates(lineage, checkpointBatches.map(_.batchId).toSet)
      checkpointById = checkpointBatches.map(batch => batch.batchId -> batch).toMap
      stateById = states.map(state => state.preparation.identity.batchId -> state).toMap
      _ <- F.raiseWhen(stateById.size != states.size)(conflict)
      _ <- F.raiseWhen(states.count(_.terminalOutcome.isEmpty) > 1)(conflict)
      _ <- F.raiseWhen(checkpointById.size != checkpointBatches.size)(conflict)
      _ <- F.raiseWhen(checkpointEstablished && checkpointBatches.isEmpty)(conflict)
      _ <- F.raiseWhen(!checkpointEstablished && checkpointBatches.exists(_.committed))(conflict)
      _ <- F.raiseWhen(!checkpointEstablished && checkpointBatches.isEmpty && hasLineageState)(conflict)
      _ <- states.traverse_ { state =>
        val batchId = state.preparation.identity.batchId
        checkpointById.get(batchId) match {
          case None        => F.raiseError[Unit](conflict)
          case Some(batch) =>
            val preparedEndOffsets = state.preparation.sourceEndOffsets.map { offset =>
              (AnalyticsTopic.unwrap(offset.topic) -> AnalyticsPartition.unwrap(offset.partition)) ->
                AnalyticsOffset.unwrap(offset.offset)
            }.toMap
            val rangesMatch = state.preparation.deliveredOffsets.forall { summary =>
              val key = AnalyticsTopic.unwrap(summary.topic) -> AnalyticsPartition.unwrap(summary.partition)
              val minimumDelivered = AnalyticsOffset.unwrap(summary.minimumDeliveredOffset)
              val maximumDelivered = AnalyticsOffset.unwrap(summary.maximumDeliveredOffset)
              val checkpointEnd = batch.endOffsets.get(key)
              val priorEnd = checkpointBatches
                .find(_.batchId.value == batchId.value - 1L)
                .flatMap(_.endOffsets.get(key))
              checkpointEnd.exists(_ > maximumDelivered) && priorEnd.forall(_ <= minimumDelivered)
            }
            F.raiseWhen(preparedEndOffsets != batch.endOffsets || !rangesMatch)(conflict) *>
              F.raiseWhen(batch.committed && state.terminalOutcome.isEmpty)(conflict)
        }
      }
      _ <- checkpointBatches.traverse_ { batch =>
        stateById.get(batch.batchId) match {
          case Some(state) if !batch.committed || state.terminalOutcome.nonEmpty => F.unit
          case None if !batch.committed                                          => F.unit
          case _                                                                 => F.raiseError[Unit](conflict)
        }
      }
    } yield ()
}
