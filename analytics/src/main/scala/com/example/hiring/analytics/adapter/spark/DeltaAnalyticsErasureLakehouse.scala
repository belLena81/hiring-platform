package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, SubjectPseudonymizer, SubjectToken}
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsBatchMaintenance,
  AnalyticsLakehouseLock,
  AnalyticsLakehousePaths
}
import com.example.hiring.analytics.service.erasure.AnalyticsErasureLakehouse
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Coordinates Delta-backed erasure maintenance independently of batch publication. */
private[analytics] final class DeltaAnalyticsErasureLakehouse[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    clock: Clock[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    retirementStore: HmacKeyRetirementAuthorizationStore[F],
    operational: AnalyticsOperationalSettings,
    override protected val sparkExecution: SparkExecution[F],
    logger: Logger[F]
) extends AnalyticsErasureLakehouse[F]
    with AnalyticsBatchMaintenance[F]
    with LakehouseOperation[F] {
  private val retention = new AnalyticsDeltaRetention[F](paths, operational, this, logger)
  private val keyContinuity = new AnalyticsKeyContinuityStage(
    KeyContinuityStagePorts(
      paths,
      pseudonymizer,
      this,
      clock,
      new KeyRetirementLookup[F] {
        override def list(lakehouseRoot: String) = retirementStore.list(lakehouseRoot)
      }
    )
  )
  private val erasure = new AnalyticsBatchErasureStage(
    ErasureStagePorts(paths, this, retention.configureRawTables, operational.maximumErasureEvidenceFiles)
  )

  override def validateHmacConfigurationLocked: F[Unit] =
    keyContinuity.validateHmacConfiguration(spark)

  override def configureRawTables: F[Unit] = retention.configureRawTables(spark)

  override def expireStored(at: Instant): F[Unit] =
    retention.expire(spark, paths.bronze, at) *>
      retention.expire(spark, paths.quarantine, at) *>
      retention.expire(spark, paths.silver, at)

  override def validateHmacConfiguration: F[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked)

  override def reclaimRetainedFiles: F[Long] =
    retention.configureRawTables(spark) *>
      retention.vacuumExpiredFiles(spark).flatTap(_ => erasure.checkpointRawTableLogs(spark))

  override def verifyMarkedSubjectsAbsent(markerTokens: Vector[SubjectToken]): F[Unit] =
    withMarkerTokens(markerTokens)(frame => erasure.verifyMarkedSubjectsAbsent(spark, frame))

  override def countMarkedRows(markerTokens: Vector[SubjectToken]): F[Long] =
    withMarkerTokens(markerTokens)(frame => erasure.countMarkedRows(spark, frame))

  override def captureMarkedFiles(markerTokens: Vector[SubjectToken]): F[Vector[String]] =
    withMarkerTokens(markerTokens)(frame => erasure.captureMarkedFiles(spark, frame))

  override def checkpointPurgedRawLogs: F[Vector[String]] = erasure.checkpointPurgedRawLogs(spark)

  override def verifyFilesAbsent(files: Vector[String]): F[Unit] =
    erasure.verifyFilesAbsent(spark, files)

  override def checkpointRawTableLogs: F[Unit] =
    erasure.checkpointRawTableLogs(spark)

  override def purgeMarkedSubjectRows(path: String, markerTokens: Vector[SubjectToken]): F[Unit] =
    withMarkerTokens(markerTokens)(frame => erasure.purgeMarkedSubjectRows(spark, path, frame))

  override def applyDeletionMarkers(markerTokens: Vector[SubjectToken]): F[Unit] =
    retention.configureRawTables(spark) *> applyActiveDeletions(markerTokens)

  override def applyActiveDeletions(markerTokens: Vector[SubjectToken]): F[Unit] =
    withMarkerTokens(markerTokens)(applyActiveDeletionsFrame)

  private def applyActiveDeletionsFrame(markerTokens: DataFrame): F[Unit] =
    for {
      deletionTime <- clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))
      _ <- retention.expire(spark, paths.bronze, deletionTime)
      _ <- retention.expire(spark, paths.quarantine, deletionTime)
      _ <- retention.expire(spark, paths.silver, deletionTime)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.bronze, markerTokens)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.quarantine, markerTokens)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.silver, markerTokens)
      _ <- rebuildGoldFromStoredSilver(spark)
      _ <- retention.vacuumExpiredFiles(spark).void
    } yield ()

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> AnalyticsGoldStage.extract(spark, paths, asOf, this)

  private def withMarkerTokens[A](tokens: Vector[SubjectToken])(use: DataFrame => F[A]): F[A] =
    apply {
      val schema = org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types
            .StructField("subjectToken", org.apache.spark.sql.types.StringType, nullable = false)
        )
      )
      spark.createDataFrame(tokens.map(token => org.apache.spark.sql.Row(token.value)).asJava, schema)
    }.flatMap(use)

  private def rebuildGoldFromStoredSilver(spark: SparkSession): F[Unit] =
    apply(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        apply(spark.read.format("delta").load(paths.silver)).flatMap(
          AnalyticsGoldStage.rebuild(paths, _, this)
        )
      case false => AnalyticsGoldStage.clear(spark, paths, this)
    }
}
