package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.config.AnalyticsPositiveInt.value
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsRunManifest, RangeFingerprint}
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.NonEmptyChain
import cats.effect.Async
import cats.syntax.all.*

import java.nio.charset.StandardCharsets
import java.time.Instant

/** Coordinates one bounded analytics publication without depending on Spark or storage adapters. */
final class HiringAnalyticsBatch[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    deletionMarkers: ActiveDeletionMarkerSource[F],
    reportPublisher: AnalyticsReportPublisher[F],
    manifestStore: AnalyticsRunManifestStore[F],
    lakehouse: AnalyticsBatchLakehouse[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    streamingRegistry: AnalyticsStreamingRegistry[F],
    operational: AnalyticsOperationalSettings,
    private[analytics] val nowOverride: Option[F[Instant]] = None
) {
  private val effect = Async[F]
  private val retention = operational.retention
  private val now = nowOverride.getOrElse(effect.realTimeInstant)

  def run(manifest: AnalyticsRunManifest): F[AnalyticsPublication] =
    for {
      _ <- manifest.offsetRanges.find(range => range.startOffset == range.endOffsetExclusive) match {
        case Some(range) =>
          effect.raiseError[Unit](
            AnalyticsError.EmptyRequestedRange(AnalyticsTopic.unwrap(range.topic), range.partition, range.startOffset)
          )
        case None => effect.unit
      }
      publication <- lakehouseLock.resource(paths.root).use { _ =>
        for {
          _ <- streamingRegistry.rejectBatchIfRegistered(paths.root)
          markerTokens <- deletionMarkers.activeSubjectTokens
          _ <- lakehouse.validateHmacConfiguration
          reservedAt <- now
          fingerprint <- effect.fromEither(
            RangeFingerprint
              .from(rangeFingerprint(manifest))
              .leftMap(problem => AnalyticsError.InvalidInput(NonEmptyChain.one(problem)))
          )
          reservation <- reportPublisher.reserve(manifest.runId, fingerprint, reservedAt)
          result <- lakehouse.prepare(manifest, markerTokens)
          completedAt <- now
          outcome <- finishRun(manifest, markerTokens.nonEmpty, result, completedAt, reservation)
        } yield AnalyticsPublication(
          manifest.runId,
          outcome,
          completedAt,
          paths.funnelGold,
          paths.timeToHireGold,
          paths.skillsGold,
          result.bronzeRecords,
          result.validRecords,
          result.suppressedRecords,
          result.quarantinedRecords,
          result.conflictingEventIds
        )
      }
    } yield publication

  private def finishRun(
      manifest: AnalyticsRunManifest,
      activeMarkersPresent: Boolean,
      result: AnalyticsBatchResult,
      completedAt: Instant,
      reservation: AnalyticsReportReservation
  ): F[AnalyticsRunOutcome] =
    if (result.quarantinedRecords > 0L)
      persistManifest(manifest, AnalyticsManifestStatus.QualityBlocked, completedAt)
        .as(AnalyticsRunOutcome.QualityBlocked)
    else if (activeMarkersPresent)
      persistManifest(manifest, AnalyticsManifestStatus.ErasurePending, completedAt)
        .as(AnalyticsRunOutcome.ErasurePending)
    else
      for {
        report <- lakehouse.rebuildGoldAndExtractReport(completedAt)
        _ <- reportPublisher.publish(
          reservation,
          report,
          completedAt.plusSeconds(retention.publishedSnapshotDays.value.toLong * 86400L)
        )
        _ <- persistManifest(manifest, AnalyticsManifestStatus.Published, completedAt)
      } yield AnalyticsRunOutcome.Published

  private def persistManifest(
      manifest: AnalyticsRunManifest,
      status: AnalyticsManifestStatus,
      updatedAt: Instant
  ): F[Unit] = manifestStore.persist(manifest, status, updatedAt)

  private def rangeFingerprint(manifest: AnalyticsRunManifest): String = {
    val canonical = manifest.offsetRanges
      .sortBy(range => (AnalyticsTopic.unwrap(range.topic), range.partition))
      .map(range =>
        s"${AnalyticsTopic.unwrap(range.topic)}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}"
      )
      .mkString("\n")
    AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8))
  }
}
