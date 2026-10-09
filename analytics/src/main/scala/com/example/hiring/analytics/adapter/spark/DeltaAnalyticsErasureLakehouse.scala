package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, SubjectPseudonymizer, SubjectToken}
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.service.batch.{
  AnalyticsBatchMaintenance,
  AnalyticsLakehouseLock,
  AnalyticsLakehousePaths
}
import com.example.hiring.analytics.service.erasure.AnalyticsErasureLakehouse
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore

import cats.effect.{Async, Clock}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.typelevel.log4cats.Logger

import java.time.Instant

/** Coordinates Delta-backed erasure maintenance independently of batch publication. */
private[analytics] final class DeltaAnalyticsErasureLakehouse[F[_]: Async: UUIDGen](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    lakehouseLock: AnalyticsLakehouseLock[F],
    retirementStore: HmacKeyRetirementAuthorizationStore[F],
    operational: AnalyticsOperationalSettings,
    execution: SparkExecution[F],
    logger: Logger[F],
    clock: Clock[F],
    deltaLogFactory: DeltaLogFactory = DeltaLogFactory.system
) extends AnalyticsErasureLakehouse[F]
    with AnalyticsBatchMaintenance[F] {
  private val now = clock.realTimeInstant
  private val retention = new AnalyticsDeltaRetention[F](paths, operational, execution, logger)
  private val keyContinuity = new AnalyticsKeyContinuityStage(
    paths,
    pseudonymizer,
    execution,
    (lakehouseRoot: String) => retirementStore.list(lakehouseRoot),
    clock
  )
  private val erasure = new AnalyticsBatchErasureStage(
    paths,
    execution,
    retention.configureRawTablePrivacy,
    operational.maximumErasureEvidenceFiles,
    deltaLogFactory
  )

  override def validateHmacConfigurationLocked: F[Unit] =
    retention.recoverAbandonedRewrites(spark) *> keyContinuity.validateHmacConfiguration(spark)

  override def configureRawTables: F[Unit] = retention.configureRawTables(spark)

  override def expireStored(at: Instant): F[Unit] =
    paths.inventory.subjectDelta.traverse_(surface => retention.expire(spark, surface.location, at))

  override def validateHmacConfiguration: F[Unit] =
    lakehouseLock.resource(paths.root).use(_ => validateHmacConfigurationLocked)

  override def reclaimRetainedFiles: F[Long] =
    retention.configureRawTablePrivacy(spark) *>
      retention.vacuumExpiredFiles(spark).flatTap(_ => erasure.checkpointRawTableLogs(spark))

  /** Idle streaming maintenance: reclaim only obsolete files, without creating replacement table snapshots. */
  def reclaimExpiredFiles: F[Long] =
    retention.configureRawTablePrivacy(spark) *>
      retention.vacuumUnreferencedFiles(spark).flatTap(_ => erasure.checkpointRawTableLogs(spark))

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

  /** Remove marked rows and rebuild derived data; physical erasure remains owned by the erasure worker. */
  def purgeMarkedSubjects(markerTokens: Vector[SubjectToken]): F[Unit] =
    retention.configureRawTablePrivacy(spark) *> withMarkerTokens(markerTokens)(purgeMarkedSubjectsFrame)

  private def applyActiveDeletionsFrame(markerTokens: DataFrame): F[Unit] =
    for {
      deletionTime <- now
      _ <- expireStored(deletionTime)
      _ <- purgeMarkedSubjectsFrame(markerTokens)
      _ <- retention.vacuumExpiredFiles(spark).void
    } yield ()

  private def purgeMarkedSubjectsFrame(markerTokens: DataFrame): F[Unit] =
    paths.inventory.subjectDelta.traverse_(surface =>
      erasure.purgeMarkedSubjectRows(spark, surface.location, markerTokens)
    ) *> rebuildGoldFromStoredSilver(spark)

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    rebuildGoldFromStoredSilver(spark) *> AnalyticsGoldStage.extract(spark, paths, asOf, execution)

  private def withMarkerTokens[A](tokens: Vector[SubjectToken])(use: DataFrame => F[A]): F[A] =
    execution(AnalyticsTableSchemas.markerFrame(spark, tokens)).flatMap(use)

  private def rebuildGoldFromStoredSilver(spark: SparkSession): F[Unit] =
    execution(DeltaTables.exists(spark, paths.silver)).flatMap {
      case true =>
        execution(DeltaTables.read(spark, paths.silver)).flatMap(
          AnalyticsGoldStage.rebuild(paths, _, execution)
        )
      case false => AnalyticsGoldStage.clear(spark, paths, execution)
    }
}
