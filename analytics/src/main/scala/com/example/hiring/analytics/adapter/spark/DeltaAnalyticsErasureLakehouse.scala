package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, SubjectPseudonymizer, SubjectToken}
import com.example.hiring.analytics.config.{AnalyticsOperationalSettings, MaximumErasureEvidenceFiles}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsBatchMaintenance,
  AnalyticsLakehouseLock,
  AnalyticsLakehousePaths
}
import com.example.hiring.analytics.service.erasure.AnalyticsErasureLakehouse
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.Async
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
    lakehouseLock: AnalyticsLakehouseLock[F],
    retirementStore: HmacKeyRetirementAuthorizationStore[F],
    operational: AnalyticsOperationalSettings,
    execution: SparkExecution[F],
    logger: Logger[F],
    private[analytics] val nowOverride: Option[F[Instant]] = None
) extends AnalyticsErasureLakehouse[F]
    with AnalyticsBatchMaintenance[F] {
  private val now = nowOverride.getOrElse(Async[F].realTimeInstant)
  private val retention = new AnalyticsDeltaRetention[F](paths, operational, execution, logger)
  private val keyContinuity = new AnalyticsKeyContinuityStage(
    paths,
    pseudonymizer,
    execution,
    (lakehouseRoot: String) => retirementStore.list(lakehouseRoot),
    Some(now)
  )
  private val erasure = new AnalyticsBatchErasureStage(
    paths,
    execution,
    retention.configureRawTablePrivacy,
    MaximumErasureEvidenceFiles.unwrap(operational.maximumErasureEvidenceFiles)
  )

  override def validateHmacConfigurationLocked: F[Unit] =
    retention.recoverAbandonedRewrites(spark) *> keyContinuity.validateHmacConfiguration(spark)

  override def configureRawTables: F[Unit] = retention.configureRawTables(spark)

  override def expireStored(at: Instant): F[Unit] =
    retention.expire(spark, paths.bronze, at) *>
      retention.expire(spark, paths.quarantine, at) *>
      retention.expire(spark, paths.silver, at) *>
      retention.expire(spark, paths.lateFacts, at)

  override def validateHmacConfiguration: F[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked)

  override def reclaimRetainedFiles: F[Long] =
    retention.configureRawTablePrivacy(spark) *>
      retention.vacuumExpiredFiles(spark).flatTap(_ => erasure.checkpointRawTableLogs(spark))

  override def verifyMarkedSubjectsAbsent(markerTokens: Vector[SubjectToken]): F[Unit] =
    retention.recoverAbandonedRewrites(spark) *>
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
    retention.configureRawTablePrivacy(spark) *> applyActiveDeletions(markerTokens)

  override def applyActiveDeletions(markerTokens: Vector[SubjectToken]): F[Unit] =
    withMarkerTokens(markerTokens)(applyActiveDeletionsFrame)

  private def applyActiveDeletionsFrame(markerTokens: DataFrame): F[Unit] =
    for {
      deletionTime <- now
      _ <- retention.expire(spark, paths.bronze, deletionTime)
      _ <- retention.expire(spark, paths.quarantine, deletionTime)
      _ <- retention.expire(spark, paths.silver, deletionTime)
      _ <- retention.expire(spark, paths.lateFacts, deletionTime)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.bronze, markerTokens)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.quarantine, markerTokens)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.silver, markerTokens)
      _ <- erasure.purgeMarkedSubjectRows(spark, paths.lateFacts, markerTokens)
      _ <- rebuildGoldFromStoredSilver(spark)
      _ <- retention.vacuumExpiredFiles(spark).void
    } yield ()

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> AnalyticsGoldStage.extract(spark, paths, asOf, execution)

  private def withMarkerTokens[A](tokens: Vector[SubjectToken])(use: DataFrame => F[A]): F[A] =
    execution {
      val schema = org.apache.spark.sql.types.StructType(
        Seq(
          org.apache.spark.sql.types
            .StructField("subjectToken", org.apache.spark.sql.types.StringType, nullable = false)
        )
      )
      spark.createDataFrame(tokens.map(token => org.apache.spark.sql.Row(token.value)).asJava, schema)
    }.flatMap(use)

  private def rebuildGoldFromStoredSilver(spark: SparkSession): F[Unit] =
    execution(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        execution(spark.read.format("delta").load(paths.silver)).flatMap(
          AnalyticsGoldStage.rebuild(paths, _, execution)
        )
      case false => AnalyticsGoldStage.clear(spark, paths, execution)
    }
}
