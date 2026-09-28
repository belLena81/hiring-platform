package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsReportOutput,
  AnalyticsRetention,
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
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.{Async, Clock, Resource}
import cats.data.NonEmptyChain
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
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
    retirementStore: HmacKeyRetirementAuthorizationStore[F],
    logger: Logger[F]
) extends LakehouseOperation[F]
    with AnalyticsErasureLakehouse[F] {
  override protected val async: Async[F] = Async[F]
  private val F = Async[F]
  private val MaximumErasureEvidenceFiles = 100000
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
            _ <- validateHmacConfigurationLocked(spark)
            reservedAt <- now
            fingerprint <- F.fromEither(
              RangeFingerprint
                .from(rangeFingerprint(manifest))
                .leftMap(problem => AnalyticsError.InvalidInput(NonEmptyChain.one(problem)))
            )
            reservation <- reportPublisher.reserve(manifest.runId, fingerprint, reservedAt)
            result <-
              for {
                _ <- configureRawTablePrivacy(spark)
                _ <- if (activeMarkerCount > 0L) applyActiveDeletions(spark, markerTokens) else F.unit
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

  /** A new primary HMAC key cannot split contributor identity while unexpired Silver rows use an older key. */
  /** Raw Bronze and quarantine keep replay data, but their Delta logs must not index raw values. */
  private def configureRawTablePrivacy(spark: SparkSession): F[Unit] = lakehouse {
    spark.conf.set("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols", "0")
    spark.conf.set(
      "spark.databricks.delta.properties.defaults.logRetentionDuration",
      s"interval ${AnalyticsRetention.DeltaLogRetentionDays} days"
    )
    Vector(paths.bronze, paths.quarantine).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) {
        val escaped = path.replace("`", "``")
        val properties = DeltaTable
          .forPath(spark, path)
          .detail()
          .select("properties")
          .head()
          .getAs[scala.collection.Map[String, String]]("properties")
        val desiredLogRetention = s"interval ${AnalyticsRetention.DeltaLogRetentionDays} days"
        if (
          properties.get("delta.dataSkippingNumIndexedCols").forall(_ != "0") ||
          properties.get("delta.logRetentionDuration").forall(_ != desiredLogRetention)
        )
          spark.sql(
            s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES " +
              s"('delta.dataSkippingNumIndexedCols' = '0', 'delta.logRetentionDuration' = '$desiredLogRetention')"
          )
      }
    }
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
  private val keyContinuityStage = new AnalyticsKeyContinuityStage(
    KeyContinuityStagePorts(
      paths,
      pseudonymizer,
      stageExecution,
      clock,
      new KeyRetirementLookup[F] {
        override def list(lakehouseRoot: String) = retirementStore.list(lakehouseRoot)
      }
    )
  )

  private def validateHmacConfigurationLocked(spark: SparkSession): F[Unit] =
    keyContinuityStage.validateHmacConfiguration(spark)

  override def validateHmacConfiguration(spark: SparkSession): F[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked(spark))

  private def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
    for {
      deletionTime <- now
      _ <- expire(spark, paths.bronze, deletionTime)
      _ <- expire(spark, paths.quarantine, deletionTime)
      _ <- expire(spark, paths.silver, deletionTime)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.bronze, markerTokens)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.quarantine, markerTokens)
      _ <- erasureStage.purgeMarkedSubjectRows(spark, paths.silver, markerTokens)
      _ <- rebuildGoldFromStoredSilver(spark)
      _ <- vacuumExpiredFiles(spark).void
    } yield ()

  override def applyDeletionMarkers(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
    configureRawTablePrivacy(spark) *> applyActiveDeletions(spark, markerTokens)

  override def reclaimRetainedFiles(spark: SparkSession): F[Long] =
    configureRawTablePrivacy(spark) *> vacuumExpiredFiles(spark).flatTap(_ => checkpointRawTableLogs(spark))

  override def rebuildGoldAndExtractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> extractReport(spark, asOf)

  private val erasureStage = new AnalyticsBatchErasureStage(
    ErasureStagePorts(paths, stageExecution, configureRawTablePrivacy, MaximumErasureEvidenceFiles)
  )

  override def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
    erasureStage.verifyMarkedSubjectsAbsent(spark, markerTokens)

  override def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): F[Long] =
    erasureStage.countMarkedRows(spark, markerTokens)

  override def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): F[Vector[String]] =
    erasureStage.captureMarkedFiles(spark, markerTokens)

  override def checkpointPurgedRawLogs(spark: SparkSession): F[Vector[String]] =
    erasureStage.checkpointPurgedRawLogs(spark)

  override def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): F[Unit] =
    erasureStage.verifyFilesAbsent(spark, files)

  override def checkpointRawTableLogs(spark: SparkSession): F[Unit] =
    erasureStage.checkpointRawTableLogs(spark)

  override def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): F[Unit] =
    erasureStage.purgeMarkedSubjectRows(spark, path, markerTokens)

  /** Deletion is applied to rebuildable Gold immediately, even if the new Kafka range later quality-blocks. */
  private def rebuildGoldFromStoredSilver(spark: SparkSession): F[Unit] =
    lakehouse(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        lakehouse(spark.read.format("delta").load(paths.silver)).flatMap(AnalyticsGoldStage.rebuild(paths, _))
      case false => AnalyticsGoldStage.clear(spark, paths)
    }

  private val ingestionStage = new AnalyticsBatchIngestionStage(
    IngestionStagePorts(paths, pseudonymizer, stageExecution, manifestStore, deltaWriter, clock)
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
      }
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
      _ <- expireStored(spark, bronze.startedAt)
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

  private def expireStored(spark: SparkSession, startedAt: Instant): F[Unit] =
    for {
      _ <- expire(spark, paths.bronze, startedAt)
      _ <- expire(spark, paths.quarantine, startedAt)
      _ <- expire(spark, paths.silver, startedAt)
    } yield ()

  private def vacuumExpiredFiles(spark: SparkSession): F[Long] =
    Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold
    ).foldLeft(F.pure(0L)) { (removedFiles, path) =>
      removedFiles.flatMap { count =>
        lakehouse(DeltaTable.isDeltaTable(spark, path)).flatMap {
          case false => F.pure(count)
          case true  =>
            val temporaryPath = s"${paths.root.stripSuffix("/")}/control/purge-rewrite-${UUID.randomUUID()}"
            DeltaPurgeRewrite.temporaryPath[F](spark, temporaryPath).use { _ =>
              lakehouse {
                spark.read.format("delta").load(path).write.format("delta").mode("overwrite").save(temporaryPath)
                spark.read
                  .format("delta")
                  .load(temporaryPath)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .save(path)
                // Respect Delta's retention safety horizon. Erasure completes only after this reclaim horizon passes.
                count + DeltaTable.forPath(spark, path).vacuum().count()
              }
            }
        }
      }
    }.handleErrorWith { error =>
      logger.error(s"lakehouse expired-file vacuum failed (${error.getClass.getSimpleName})") *>
        F.raiseError(error)
    }

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
        _ <- AnalyticsGoldStage.rebuild(paths, allSilver)
        report <- extractReport(spark, completedAt)
        _ <- reportPublisher.publish(
          reservation,
          report,
          completedAt.plusSeconds(AnalyticsRetention.PublishedSnapshotDays.toLong * 86400L)
        )
        _ <- persistManifest(spark, manifest, "PUBLISHED", completedAt.toString)
      } yield AnalyticsRunOutcome.Published

  private def extractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput] =
    AnalyticsGoldStage.extract(spark, paths, asOf)

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
      .withColumn("ingestedAt", lit(Timestamp.from(now)))
      .withColumn("expiresAt", lit(Timestamp.from(now.plusSeconds(days.toLong * 24L * 60L * 60L))))

  private def quarantineId: Column =
    when(
      col("rawValue").isNull,
      concat(
        lit("tombstone:"),
        to_json(struct(col("topic").as("topic"), col("partition").as("partition"), col("offset").as("offset")))
      )
    ).otherwise(sha2(col("rawValue"), 256))

  private def expire(spark: SparkSession, path: String, now: Instant): F[Unit] = lakehouse {
    if (DeltaTable.isDeltaTable(spark, path))
      DeltaTable.forPath(spark, path).delete(col("expiresAt") <= lit(Timestamp.from(now)))
  }

}

private[analytics] object DeltaPurgeRewrite {
  def temporaryPath[F[_]: Async](spark: SparkSession, temporaryPath: String): Resource[F, Unit] =
    Resource
      .make(
        Async[F].blocking {
          val path = new org.apache.hadoop.fs.Path(temporaryPath)
          (path.getFileSystem(spark.sparkContext.hadoopConfiguration), path)
        }
      ) { case (fileSystem, path) =>
        Async[F].blocking {
          val removed = fileSystem.delete(path, true)
          if (!removed && fileSystem.exists(path))
            throw new java.io.IOException("temporary purge rewrite path remains")
        }
      }
      .void
}

private[analytics] object KafkaRecordColumns {
  def validate[F[_]: Async](schema: StructType): F[Unit] = {
    val required = Set("topic", "partition", "offset", "timestamp", "value")
    val missing = required.diff(schema.fieldNames.toSet).toVector.sorted
    if (missing.isEmpty) Async[F].unit else Async[F].raiseError(AnalyticsError.InvalidSourceSchema(missing))
  }
}

/** Bounded batch entry point. Runtime settings and the explicit offset range load from HOCON. */
