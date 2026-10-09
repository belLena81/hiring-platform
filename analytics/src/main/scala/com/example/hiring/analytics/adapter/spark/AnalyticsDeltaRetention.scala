package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsStoragePrivacy}

import cats.effect.Async
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.typelevel.log4cats.Logger

import java.sql.Timestamp
import java.time.Instant

/** Applies Delta retention/privacy properties and reclaims expired table files. */
private[analytics] final class AnalyticsDeltaRetention[F[_]: Async: UUIDGen](
    paths: AnalyticsLakehousePaths,
    operational: AnalyticsOperationalSettings,
    execution: SparkExecution[F],
    logger: Logger[F]
) {
  private val retention = operational.retention

  /** Must run while the caller owns the shared lakehouse mutex. */
  def recoverAbandonedRewrites(spark: SparkSession): F[Unit] =
    DeltaPurgeRewrite.recover(
      paths.root,
      spark.sparkContext.hadoopConfiguration,
      operational.maximumErasureEvidenceFiles,
      execution
    )

  def configureRawTables(spark: SparkSession): F[Unit] =
    execution {
      AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, AnalyticsTableSchemas.bronze, raw = true)
      AnalyticsTableSchemas.createOrValidate(spark, paths.quarantine, AnalyticsTableSchemas.quarantine, raw = true)
      AnalyticsTableSchemas.createOrValidate(
        spark,
        paths.silver,
        AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry
      )
      AnalyticsTableSchemas.createOrValidate(spark, paths.lateFacts, AnalyticsTableSchemas.lateFacts, raw = true)
    } *> configureRawTablePrivacy(spark)

  def configureRawTablePrivacy(spark: SparkSession): F[Unit] = execution {
    val desiredVacuumRetention = s"interval ${retention.deltaVacuumSafety}"
    val desiredLogRetention = s"interval ${retention.deltaLogRetention}"
    val rawPaths = paths.inventory.subjectDelta.map(_.location).toSet
    val tables = paths.inventory.delta.map(_.location)
    spark.conf.set(
      "spark.databricks.delta.properties.defaults.deletedFileRetentionDuration",
      desiredVacuumRetention
    )
    spark.conf.set("spark.databricks.delta.properties.defaults.logRetentionDuration", desiredLogRetention)
    spark.conf.set(
      "spark.databricks.delta.retentionDurationCheck.enabled",
      retention.deltaVacuumSafetyCheckEnabled.toString
    )
    tables.foreach { path =>
      if (DeltaTables.exists(spark, path)) {
        val escaped = SparkPhysicalLocation.resolve(path).replace("`", "``")
        // DESCRIBE DETAIL computes file counts and sizes even when only properties are selected.
        // Read the same fresh snapshot's metadata without reconstructing those unused statistics.
        val properties = DeltaLogFactory.system(spark, path).update().metadata.configuration
        val desired = Map(
          "delta.deletedFileRetentionDuration" -> desiredVacuumRetention,
          "delta.logRetentionDuration" -> desiredLogRetention
        ) ++ (if (rawPaths.contains(path)) Map("delta.dataSkippingNumIndexedCols" -> "0") else Map.empty)
        val changes = desired.filter { case (key, value) => properties.get(key).forall(_ != value) }
        if (changes.nonEmpty) {
          val rendered = changes.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
          val _ = spark.sql(s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES ($rendered)")
        }
      }
    }
  }

  def expire(spark: SparkSession, path: String, at: Instant): F[Unit] = execution {
    if (DeltaTables.exists(spark, path))
      DeltaTables
        .forPath(spark, path)
        .delete(col(Columns.ExpiresAt) <= lit(Timestamp.from(at)))
  }

  def vacuumExpiredFiles(spark: SparkSession): F[Long] =
    recoverAbandonedRewrites(spark) *> paths.inventory.delta
      .filter(_.privacy != AnalyticsStoragePrivacy.SanitizedControl)
      .map(_.location)
      .foldM(0L) { (count, path) =>
        execution(DeltaTables.exists(spark, path)).flatMap {
          case false => Async[F].pure(count)
          case true  =>
            UUIDGen[F].randomUUID.flatMap { id =>
              val temporaryPath = s"${paths.root.stripSuffix("/")}/control/purge-rewrite-$id"
              DeltaPurgeRewrite.temporaryPath[F](spark, temporaryPath, execution).use { _ =>
                execution {
                  DeltaTables
                    .read(spark, path)
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .save(SparkPhysicalLocation.resolve(temporaryPath))
                  DeltaTables
                    .read(spark, temporaryPath)
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .save(SparkPhysicalLocation.resolve(path))
                  // Respect Delta's retention safety horizon. Erasure completes only after this reclaim horizon passes.
                  val retentionHours = retention.deltaVacuumSafety.toMillis.toDouble / 3600000d
                  count + DeltaTables
                    .forPath(spark, path)
                    .vacuum(retentionHours)
                    .count()
                }
              }
            }
        }
      }
      .handleErrorWith { error =>
        logger.error(s"lakehouse expired-file vacuum failed (${error.getClass.getSimpleName})") *>
          Async[F].raiseError(error)
      }

  /** Ordinary idle maintenance never rewrites current data or control records. Empty VACUUM runs avoid log commits. */
  def vacuumUnreferencedFiles(spark: SparkSession): F[Long] =
    recoverAbandonedRewrites(spark) *> paths.inventory.delta
      .traverse { surface =>
        execution {
          val path = surface.location
          if (!DeltaTables.exists(spark, path)) 0L
          else {
            val escaped = SparkPhysicalLocation.resolve(path).replace("`", "``")
            val hours = retention.deltaVacuumSafety.toMillis.toDouble / 3600000d
            val noCandidates = DeltaVacuumEligibility.canSkip(
              spark,
              path,
              hours,
              operational.maximumErasureEvidenceFiles
            )
            val candidates =
              if (noCandidates) 0L
              else spark.sql(s"VACUUM delta.`$escaped` RETAIN $hours HOURS DRY RUN").limit(1).count()
            if (candidates == 0L) 0L
            else DeltaTables.forPath(spark, path).vacuum(hours).count()
          }
        }
      }
      .map(_.sum)
}
