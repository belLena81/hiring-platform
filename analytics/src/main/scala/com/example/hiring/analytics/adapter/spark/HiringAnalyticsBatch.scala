package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsReportOutput,
  AnalyticsRunManifest,
  RangeFingerprint,
  SubjectPseudonymizer
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  ActiveDeletionMarkerSource,
  AnalyticsLakehouseLock,
  AnalyticsLakehousePaths,
  AnalyticsPublication,
  AnalyticsReportPublisher,
  AnalyticsReportReservation,
  AnalyticsRunOutcome,
  BoundedOperationalEventSource,
  ManifestStore
}

import cats.effect.{Async, Clock, Resource}
import cats.data.NonEmptyChain
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant
import scala.util.control.NonFatal

/** Delta batch writer with idempotent natural keys. A completed manifest is written only after Bronze, Silver,
  * quarantine, and both rebuildable Gold datasets have been durably updated.
  */
final class HiringAnalyticsBatch[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    deletionMarkers: ActiveDeletionMarkerSource[F],
    clock: Clock[F],
    reportPublisher: AnalyticsReportPublisher[F],
    manifestStore: ManifestStore[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    operational: AnalyticsOperationalSettings,
    override protected val sparkExecution: SparkBlockingExecution[F],
    maintenance: AnalyticsBatchMaintenance[F]
) extends LakehouseOperation[F] {
  override protected val async: Async[F] = Async[F]
  private val F = Async[F]
  private val retention = operational.retention
  private def now: F[Instant] = clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))

  private def persistManifest(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      status: String,
      updatedAt: String
  ): F[Unit] =
    manifestStore.persist(spark, manifest, status, updatedAt)

  private def cachedMarkers(spark: SparkSession): Resource[F, DataFrame] =
    Resource.make(
      deletionMarkers
        .activeSubjectTokens(spark)
        .flatMap(frame => lakehouse(frame.persist(StorageLevel.MEMORY_AND_DISK)))
    )(frame => lakehouse(frame.unpersist(blocking = true)).void)

  def run(
      spark: SparkSession,
      source: BoundedOperationalEventSource[F],
      manifest: AnalyticsRunManifest
  ): F[AnalyticsPublication] =
    for {
      _ <- AnalyticsOffsetRanges.requireNonEmpty(manifest)
      publication <- lakehouseLock.resource(paths.root).use { _ =>
        cachedMarkers(spark).use { markerTokens =>
          for {
            _ <- validateMarkerColumns(markerTokens)
            activeMarkerCount <- lakehouse(markerTokens.count())
            _ <- maintenance.validateHmacConfigurationLocked(spark)
            reservedAt <- now
            fingerprint <- F.fromEither(
              RangeFingerprint
                .from(rangeFingerprint(manifest))
                .leftMap(problem => AnalyticsError.InvalidInput(NonEmptyChain.one(problem)))
            )
            reservation <- reportPublisher.reserve(manifest.runId, fingerprint, reservedAt)
            result <-
              for {
                _ <- maintenance.configureRawTables(spark)
                _ <- if (activeMarkerCount > 0L) maintenance.applyActiveDeletions(spark, markerTokens) else F.unit
                result <- runWithMarkers(
                  spark,
                  source,
                  manifest,
                  markerTokens,
                  activeMarkerCount > 0L,
                  reservation
                )
              } yield result
          } yield result
        }
      }
    } yield publication

  private def validateMarkerColumns(frame: DataFrame): F[Unit] =
    lakehouse(frame.columns.toVector).flatMap { columns =>
      if (columns.contains("subjectToken")) F.unit
      else F.raiseError(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
    }

  private val stageExecution = new SparkExecution[F] {
    override def apply[A](work: => A): F[A] = lakehouse(work)
    override def either[A](work: => Either[AnalyticsError, A]): F[A] = lakehouseEither(work)
  }
  private val deltaWriter = new DeltaWriter[F] {
    override def merge(source: DataFrame, path: String, condition: String): F[Unit] =
      mergeDelta(source, path, condition)
    override def withExpiry(frame: DataFrame, at: Instant, days: Int): DataFrame = addExpiry(frame, at, days)
  }
  private val deltaReader = new DeltaReader[F] {
    override def readOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame] =
      readDeltaOrEmpty(spark, path, schema)
  }
  private val ingestionStage = new AnalyticsBatchIngestionStage(
    IngestionStagePorts(paths, pseudonymizer, stageExecution, manifestStore, deltaWriter, clock, retention)
  )
  private val silverStage = new AnalyticsBatchSilverStage(
    SilverStagePorts(
      paths,
      pseudonymizer,
      stageExecution,
      deltaWriter,
      deltaReader,
      new QuarantineId {
        override def apply(): Column = quarantineId
      },
      retention
    )
  )

  private def runWithMarkers(
      spark: SparkSession,
      source: BoundedOperationalEventSource[F],
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean,
      reservation: AnalyticsReportReservation
  ): F[AnalyticsPublication] =
    for {
      bronze <- ingestionStage.ingest(spark, source, manifest, markerTokens)
      prepared <- silverStage.separateQuarantine(spark, bronze, markerTokens, activeMarkersPresent)
      silver <- silverStage.mergeSilver(prepared, bronze.startedAt)
      silverSchema <- lakehouse(silver.schema)
      completedAt <- now
      _ <- maintenance.expireStored(spark, bronze.startedAt)
      outcome <- finishRun(
        spark,
        manifest,
        silverSchema,
        prepared.quarantinedRecords,
        completedAt,
        activeMarkersPresent,
        reservation
      )
    } yield AnalyticsPublication(
      manifest.runId,
      outcome,
      completedAt,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold,
      bronze.records,
      prepared.validRecords,
      prepared.suppressedRecords,
      prepared.quarantinedRecords,
      prepared.conflictingEventIds
    )

  private def finishRun(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      silverSchema: StructType,
      quarantinedRecords: Long,
      completedAt: Instant,
      activeMarkersPresent: Boolean,
      reservation: AnalyticsReportReservation
  ): F[AnalyticsRunOutcome] =
    if (quarantinedRecords > 0L)
      persistManifest(spark, manifest, "QUALITY_BLOCKED", completedAt.toString).as(AnalyticsRunOutcome.QualityBlocked)
    else if (activeMarkersPresent)
      persistManifest(spark, manifest, "ERASURE_PENDING", completedAt.toString).as(AnalyticsRunOutcome.ErasurePending)
    else
      for {
        allSilver <- readDeltaOrEmpty(spark, paths.silver, silverSchema)
        _ <- AnalyticsGoldStage.rebuild(paths, allSilver, sparkExecution)
        report <- extractReport(spark, completedAt)
        _ <- reportPublisher.publish(
          reservation,
          report,
          completedAt.plusSeconds(retention.publishedSnapshotDays.toLong * 86400L)
        )
        _ <- persistManifest(spark, manifest, "PUBLISHED", completedAt.toString)
      } yield AnalyticsRunOutcome.Published

  private def extractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput] =
    AnalyticsGoldStage.extract(spark, paths, asOf, sparkExecution)

  private def rangeFingerprint(manifest: AnalyticsRunManifest): String = {
    val canonical = manifest.offsetRanges
      .sortBy(range => (range.topic, range.partition))
      .map(range => s"${range.topic}:${range.partition}:${range.startOffset}:${range.endOffsetExclusive}")
      .mkString("\n")
    AnalyticsDigest.sha256Hex(canonical.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  }

  private def mergeDelta(source: DataFrame, path: String, condition: String): F[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(source.sparkSession, path))
      DeltaTable
        .forPath(source.sparkSession, path)
        .as("target")
        .merge(source.as("source"), condition)
        .withSchemaEvolution()
        .whenNotMatched()
        .insertAll()
        .execute()
    else source.write.format("delta").mode("errorifexists").save(path)
  }

  private def readDeltaOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame] =
    lakehouse {
      if (DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path)
      else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

  private def addExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
    frame
      .withColumn(Columns.IngestedAt, lit(Timestamp.from(now)))
      .withColumn(Columns.ExpiresAt, lit(Timestamp.from(now.plus(java.time.Duration.ofDays(days.toLong)))))

  private def quarantineId: Column =
    when(
      col("rawValue").isNull,
      concat(
        lit("tombstone:"),
        to_json(struct(col("topic").as("topic"), col("partition").as("partition"), col("offset").as("offset")))
      )
    ).otherwise(sha2(col("rawValue"), 256))

}

/** Bounded batch entry point. Runtime settings and the explicit offset range load from HOCON. */
