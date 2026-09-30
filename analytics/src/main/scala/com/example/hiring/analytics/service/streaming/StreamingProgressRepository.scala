package com.example.hiring.analytics.service.streaming

import com.example.hiring.analytics.domain.{
  StreamingBatchIdentity,
  StreamingBatchPreparation,
  StreamingBatchProgress,
  StreamingLineage
}

trait StreamingProgressRepository[F[_]] {
  def load(identity: StreamingBatchIdentity): F[Option[StreamingBatchProgress]]
  def loadLatest(lineage: StreamingLineage): F[Option[StreamingBatchProgress]]
  def prepare(preparation: StreamingBatchPreparation): F[Unit]
  def complete(progress: StreamingBatchProgress): F[Unit]
}
