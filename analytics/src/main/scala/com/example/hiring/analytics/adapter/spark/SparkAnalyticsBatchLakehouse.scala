package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.{
  AnalyticsReportOutput,
  AnalyticsRunManifest,
  SubjectPseudonymizer,
  SubjectToken
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsBatchLakehouse,
  AnalyticsBatchResult,
  AnalyticsBatchMaintenance,
  AnalyticsLakehousePaths,
  AnalyticsRunManifestStore
}

import cats.effect.{Async, Clock, Resource}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant

/** Delta batch writer with idempotent natural keys. A completed manifest is written only after Bronze, Silver,
  * quarantine, and both rebuildable Gold datasets have been durably updated.
  */
private[analytics] final class SparkAnalyticsBatchLakehouse[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    source: BoundedOperationalEventSource[F],
    clock: Clock[F],
    manifestStore: AnalyticsRunManifestStore[F],
    operational: AnalyticsOperationalSettings,
    override protected val sparkExecution: SparkExecution[F],
    maintenance: AnalyticsBatchMaintenance[F]
) extends LakehouseOperation[F]
    with AnalyticsBatchLakehouse[F] {
  private val F = Async[F]
  private val retention = operational.retention

  override def validateHmacConfiguration: F[Unit] = maintenance.validateHmacConfigurationLocked

  override def prepare(manifest: AnalyticsRunManifest, tokens: Vector[SubjectToken]): F[AnalyticsBatchResult] =
    cachedMarkers(tokens).use { markerTokens =>
      for {
        _ <- validateMarkerColumns(markerTokens)
        _ <- maintenance.configureRawTables
        _ <- if (tokens.nonEmpty) maintenance.applyActiveDeletions(tokens) else F.unit
        result <- runWithMarkers(manifest, markerTokens, tokens.nonEmpty)
      } yield result
    }

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    for {
      hasSilver <- lakehouse(DeltaTable.isDeltaTable(spark, paths.silver))
      _ <-
        if (hasSilver)
          lakehouse(spark.read.format("delta").load(paths.silver))
            .flatMap(AnalyticsGoldStage.rebuild(paths, _, sparkExecution))
        else AnalyticsGoldStage.clear(spark, paths, sparkExecution)
      report <- extractReport(spark, asOf)
    } yield report

  private def cachedMarkers(tokens: Vector[SubjectToken]): Resource[F, DataFrame] =
    Resource.make(lakehouse {
      import scala.jdk.CollectionConverters.*
      val schema = StructType(
        Seq(
          org.apache.spark.sql.types
            .StructField("subjectToken", org.apache.spark.sql.types.StringType, nullable = false)
        )
      )
      spark
        .createDataFrame(tokens.map(token => Row(token.value)).asJava, schema)
        .persist(StorageLevel.MEMORY_AND_DISK)
    })(frame => lakehouse(frame.unpersist(blocking = true)).void)

  private def validateMarkerColumns(frame: DataFrame): F[Unit] =
    lakehouse(frame.columns.toVector).flatMap { columns =>
      if (columns.contains("subjectToken")) F.unit
      else F.raiseError(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
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
    IngestionStagePorts(paths, pseudonymizer, this, manifestStore, deltaWriter, clock, retention)
  )
  private val silverStage = new AnalyticsBatchSilverStage(
    SilverStagePorts(
      paths,
      pseudonymizer,
      this,
      deltaWriter,
      deltaReader,
      new QuarantineId {
        override def apply(): Column = quarantineId
      },
      retention
    )
  )

  private def runWithMarkers(
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean
  ): F[AnalyticsBatchResult] =
    ingestionStage.ingest(spark, source, manifest, markerTokens).use { bronze =>
      for {
        prepared <- silverStage.separateQuarantine(spark, bronze, markerTokens, activeMarkersPresent)
        silver <- silverStage.mergeSilver(prepared, bronze.startedAt)
        _ <- lakehouse(silver.schema)
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
    AnalyticsGoldStage.extract(spark, paths, asOf, sparkExecution)

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
    else {
      val writer = source.write.format("delta").mode("errorifexists")
      val rawPath = path == paths.bronze || path == paths.quarantine
      (if (rawPath) writer.option("delta.dataSkippingNumIndexedCols", "0") else writer).save(path)
    }
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
