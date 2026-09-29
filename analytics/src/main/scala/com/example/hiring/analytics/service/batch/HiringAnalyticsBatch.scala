package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.config.AnalyticsPositiveInt.value
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsRunManifest, RangeFingerprint}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.NonEmptyChain
import cats.effect.{Async, Clock}
import cats.syntax.all.*

import java.nio.charset.StandardCharsets
import java.time.Instant

/** Coordinates one bounded analytics publication without depending on Spark or storage adapters. */
final class HiringAnalyticsBatch[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    deletionMarkers: ActiveDeletionMarkerSource[F],
    clock: Clock[F],
    reportPublisher: AnalyticsReportPublisher[F],
    manifestStore: AnalyticsRunManifestStore[F],
    lakehouse: AnalyticsBatchLakehouse[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    operational: AnalyticsOperationalSettings
) {
  private val F = Async[F]
  private val retention = operational.retention
  private def now: F[Instant] = clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))

  def run(manifest: AnalyticsRunManifest): F[AnalyticsPublication] =
    for {
      _ <- manifest.offsetRanges.find(range => range.startOffset == range.endOffsetExclusive) match {
        case Some(range) =>
          F.raiseError[Unit](
            AnalyticsError.EmptyRequestedRange(range.topic, range.partition, range.startOffset)
          )
        case None => F.unit
      }
      publication <- lakehouseLock.resource(paths.root).use { _ =>
        for {
          markerTokens <- deletionMarkers.activeSubjectTokens
          _ <- lakehouse.validateHmacConfiguration
          reservedAt <- now
          fingerprint <- F.fromEither(
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
      .sortBy(range => (range.topic, range.partition))
      .map(range => s"${range.topic}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}")
      .mkString("\n")
    AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8))
  }
}
