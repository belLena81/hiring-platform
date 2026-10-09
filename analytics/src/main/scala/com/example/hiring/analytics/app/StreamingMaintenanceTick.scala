package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.mongo.{MongoAnalyticsLateFactReplayJournal, MongoAnalyticsReportPublisher}
import com.example.hiring.analytics.adapter.spark.{DeltaAnalyticsErasureLakehouse, DeltaStreamingBatchJournal}
import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.domain.{RangeFingerprint, RunId, StreamingActivationIdentity, StreamingLineage}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{ActiveDeletionMarkerSource, AnalyticsReportReservation}
import com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch

import cats.effect.Async
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.github.iltotore.iron.*

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** One idle-time maintenance tick, run under the lakehouse mutex: erasure upkeep, report refresh, retention. */
private[app] final class StreamingMaintenanceTick[F[_]: Async: UUIDGen](
    maintenance: DeltaAnalyticsErasureLakehouse[F],
    markers: ActiveDeletionMarkerSource[F],
    publisher: MongoAnalyticsReportPublisher[F],
    journal: DeltaStreamingBatchJournal[F],
    replayJournal: MongoAnalyticsLateFactReplayJournal[F],
    retention: AnalyticsRetentionSettings,
    progressRetention: FiniteDuration
) {
  def run(
      identity: StreamingActivationIdentity,
      lineage: StreamingLineage,
      retainedCheckpoint: () => F[Vector[StreamingCheckpointBatch]],
      authorize: F[Unit],
      at: Instant
  ): F[Unit] =
    for {
      _ <- authorize
      _ <- maintenance.validateHmacConfigurationLocked
      _ <- maintenance.configureRawTables
      runId <- UUIDGen[F].randomUUID.map(value => RunId.prefixed("stream-maintenance-", value.toString))
      fingerprint = RangeFingerprint.ofSha256(s"${identity.canonical}\n$at")
      existingMarkers <- markers.activeSubjectTokens
      reservation <-
        if (existingMarkers.nonEmpty) Async[F].pure(Option.empty[AnalyticsReportReservation])
        else publisher.reservePinned(runId, fingerprint, at).map(Some(_))
      activeMarkers <- markers.activeSubjectTokens
      _ <- maintenance.expireStored(at)
      _ <- Async[F].whenA(activeMarkers.nonEmpty)(maintenance.purgeMarkedSubjects(activeMarkers))
      _ <- Async[F].whenA(activeMarkers.isEmpty)(reservation.traverse_(publishRefreshedReport(_, authorize, at)))
      retained <- retainedCheckpoint()
      _ <- journal.prune(lineage, retained.map(_.batchId).toSet, at, progressRetention)
      _ <- replayJournal.ensureIndexes
      _ <- replayJournal.compactCompleted(at)
      _ <- publisher.compactPublished(
        ids => (journal.referencedPublicationRunIds(ids), replayJournal.referencedPublicationRunIds(ids)).mapN(_ ++ _),
        at
      )
      _ <- maintenance.reclaimExpiredFiles
      _ <- authorize
    } yield ()

  private def publishRefreshedReport(
      reservation: AnalyticsReportReservation,
      authorize: F[Unit],
      at: Instant
  ): F[Unit] =
    for {
      report <- maintenance.rebuildGoldAndExtractReport(at)
      currentMarkers <- markers.activeSubjectTokens
      _ <- authorize
      _ <- Async[F].whenA(currentMarkers.isEmpty)(
        publisher
          .publish(reservation, report, retention.publishedSnapshotExpiry(at))
          .recover { case AnalyticsError.GuardedErasurePublicationRejected => () }
      )
    } yield ()
}
