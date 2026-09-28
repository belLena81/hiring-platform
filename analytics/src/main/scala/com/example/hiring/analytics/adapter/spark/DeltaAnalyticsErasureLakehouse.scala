package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsReportOutput
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsLakehousePaths}
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.typelevel.log4cats.Logger

import java.time.Instant

/** Coordinates Delta-backed erasure maintenance independently of batch publication. */
private[analytics] final class DeltaAnalyticsErasureLakehouse[F[_]: Async](
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

  def validateHmacConfigurationLocked(spark: SparkSession): F[Unit] =
    keyContinuity.validateHmacConfiguration(spark)

  def configureRawTables(spark: SparkSession): F[Unit] = retention.configureRawTables(spark)

  def expireStored(spark: SparkSession, at: Instant): F[Unit] =
    retention.expire(spark, paths.bronze, at) *>
      retention.expire(spark, paths.quarantine, at) *>
      retention.expire(spark, paths.silver, at)

  override def validateHmacConfiguration(spark: SparkSession): F[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked(spark))

  override def reclaimRetainedFiles(spark: SparkSession): F[Long] =
    retention.configureRawTables(spark) *>
      retention.vacuumExpiredFiles(spark).flatTap(_ => erasure.checkpointRawTableLogs(spark))

  override def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
    erasure.verifyMarkedSubjectsAbsent(spark, markerTokens)

  override def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): F[Long] =
    erasure.countMarkedRows(spark, markerTokens)

  override def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): F[Vector[String]] =
    erasure.captureMarkedFiles(spark, markerTokens)

  override def checkpointPurgedRawLogs(spark: SparkSession): F[Vector[String]] =
    erasure.checkpointPurgedRawLogs(spark)

  override def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): F[Unit] =
    erasure.verifyFilesAbsent(spark, files)

  override def checkpointRawTableLogs(spark: SparkSession): F[Unit] =
    erasure.checkpointRawTableLogs(spark)

  override def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): F[Unit] =
    erasure.purgeMarkedSubjectRows(spark, path, markerTokens)

  override def applyDeletionMarkers(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
    retention.configureRawTables(spark) *> applyActiveDeletions(spark, markerTokens)

  def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): F[Unit] =
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

  override def rebuildGoldAndExtractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> AnalyticsGoldStage.extract(spark, paths, asOf, this)

  private def rebuildGoldFromStoredSilver(spark: SparkSession): F[Unit] =
    apply(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        apply(spark.read.format("delta").load(paths.silver)).flatMap(
          AnalyticsGoldStage.rebuild(paths, _, this)
        )
      case false => AnalyticsGoldStage.clear(spark, paths, this)
    }
}
