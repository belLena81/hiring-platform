package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, AnalyticsRunManifest, SubjectToken}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsBatchLakehouse,
  AnalyticsBatchResult,
  AnalyticsBatchMaintenance,
  AnalyticsLakehousePaths
}

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.Instant

/** Delta batch writer with idempotent natural keys. A completed manifest is written only after Bronze, Silver,
  * quarantine, and both rebuildable Gold datasets have been durably updated.
  */
private[analytics] final class SparkAnalyticsBatchLakehouse[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    source: BoundedOperationalEventSource[F],
    execution: SparkExecution[F],
    maintenance: AnalyticsBatchMaintenance[F],
    ingestionStage: AnalyticsBatchIngestionStage[F],
    silverStage: AnalyticsBatchSilverStage[F]
) extends AnalyticsBatchLakehouse[F] {
  private val async = Async[F]
  private def lakehouse[A](work: => A): F[A] = LakehouseErrors.adapt(execution(work))

  override def validateHmacConfiguration: F[Unit] = maintenance.validateHmacConfigurationLocked

  override def prepare(manifest: AnalyticsRunManifest, tokens: Vector[SubjectToken]): F[AnalyticsBatchResult] =
    cachedMarkers(tokens).use { markerTokens =>
      for {
        _ <- validateMarkerColumns(markerTokens)
        _ <- if (tokens.nonEmpty) maintenance.applyActiveDeletions(tokens) else async.unit
        result <- runWithMarkers(manifest, markerTokens, tokens.nonEmpty)
      } yield result
    }

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    for {
      hasSilver <- lakehouse(DeltaTables.exists(spark, paths.silver))
      _ <-
        if (hasSilver)
          lakehouse(DeltaTables.read(spark, paths.silver))
            .flatMap(AnalyticsGoldStage.rebuild(paths, _, execution))
        else AnalyticsGoldStage.clear(spark, paths, execution)
      report <- extractReport(spark, asOf)
    } yield report

  private def cachedMarkers(tokens: Vector[SubjectToken]): Resource[F, DataFrame] =
    Resource.make(execution(AnalyticsTableSchemas.markerFrame(spark, tokens).persist(StorageLevel.MEMORY_AND_DISK)))(
      frame => execution(frame.unpersist(blocking = true)).void
    )

  private def validateMarkerColumns(frame: DataFrame): F[Unit] =
    execution(frame.columns.toVector).flatMap { columns =>
      if (columns.contains(Columns.SubjectToken)) async.unit
      else async.raiseError(AnalyticsError.InvalidSourceSchema(Vector(Columns.SubjectToken)))
    }

  private def runWithMarkers(
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean
  ): F[AnalyticsBatchResult] =
    ingestionStage.ingest(spark, source, manifest, markerTokens, maintenance.configureRawTables).use { bronze =>
      for {
        prepared <- silverStage.separateQuarantine(spark, bronze, markerTokens, activeMarkersPresent)
        silver <- silverStage.mergeSilver(prepared, bronze.startedAt)
        _ <- execution(silver.schema)
        _ <- maintenance.expireStored(bronze.startedAt)
      } yield AnalyticsBatchResult(
        bronze.records,
        prepared.validRecords,
        prepared.suppressedRecords,
        prepared.quarantinedRecords,
        prepared.conflictingEventIds
      )
    }

  private def extractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput] =
    AnalyticsGoldStage.extract(spark, paths, asOf, execution)
}

/** Bounded batch entry point. Runtime settings and the explicit offset range load from HOCON. */
