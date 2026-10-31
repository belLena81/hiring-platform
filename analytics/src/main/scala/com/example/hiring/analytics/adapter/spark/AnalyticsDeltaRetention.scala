package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError
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

  /** Creates or validates every subject-attributed table. Only the first initialization of the stage times (the table
    * did not exist) backfills them from Silver; an existing table, even an empty one, is never backfilled, so rows
    * removed by an erasure purge are not rebuilt from the Silver events that survive it.
    */
  def configureRawTables(spark: SparkSession, markerTokens: Vector[SubjectToken]): F[Unit] =
    execution {
      val firstInitialization = !DeltaTables.exists(spark, paths.applicationStageTimes)
      AnalyticsDeltaCatalog.locations(paths).filter(_._2.subjectAttributed).foreach { case (path, spec) =>
        AnalyticsTableSchemas.createOrValidate(spark, path, spec)
      }
      if (firstInitialization)
        ApplicationStageTimes.backfillFromSilver(
          spark,
          paths,
          retention,
          AnalyticsTableSchemas.markerFrame(spark, markerTokens)
        )
    } *> configureRawTablePrivacy(spark)

  def configureRawTablePrivacy(spark: SparkSession): F[Unit] =
    execution(applyTableProperties(spark)).flatMap(repaired =>
      repaired.traverse_(table =>
        logger.warn(
          s"DELTA_STATS_PROPERTY_REPAIRED table=$table: a positional statistics index was removed; files written " +
            "while it was set keep their statistics in the log until they are rewritten"
        )
      )
    )

  /** Returns the names of the tables whose drifted positional statistics property was removed. */
  private def applyTableProperties(spark: SparkSession): Vector[String] = {
    val desiredVacuumRetention = s"interval ${retention.deltaVacuumSafety}"
    val desiredLogRetention = s"interval ${retention.deltaLogRetention}"
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
    tables.flatMap { path =>
      if (!DeltaTables.exists(spark, path)) None
      else {
        // DESCRIBE DETAIL computes file counts and sizes even when only properties are selected.
        // Read the same fresh snapshot's metadata without reconstructing those unused statistics.
        val metadata = DeltaLogFactory.system(spark, path).update().metadata
        val properties = metadata.configuration
        // Deletion vectors would keep purged rows in files that only REORG ... APPLY (PURGE) rewrites.
        // An enabled table is never silently flipped: fail closed so an operator decides how to rewrite it.
        if (properties.get(AnalyticsDeltaCatalog.DeletionVectorsProperty).contains("true"))
          throw AnalyticsError.DeltaSchemaMismatch(path)
        // Static properties come from the table's catalog declaration; retention windows come from configuration.
        val spec = AnalyticsDeltaCatalog.specAt(paths, path)
        // A table of an earlier shape may lack allow-listed columns, which Delta rejects in the property. It gets the
        // allow-list's existing columns, or no statistics at all when none exist, so it is never left less safe.
        val allowed = spec.filter(_.subjectAttributed).map(_.statsColumns.filter(metadata.schema.fieldNames.contains))
        val statistics = allowed.fold(Map.empty[String, String]) { columns =>
          if (columns.nonEmpty) Map(DeltaStatsPolicy.StatsColumnsProperty -> DeltaStatsPolicy.render(columns))
          else Map(DeltaStatsPolicy.NumIndexedColumnsProperty -> "0")
        }
        val declared =
          spec.fold(Map.empty[String, String])(_.properties - DeltaStatsPolicy.StatsColumnsProperty) ++ statistics
        val desired = Map(
          "delta.deletedFileRetentionDuration" -> desiredVacuumRetention,
          "delta.logRetentionDuration" -> desiredLogRetention
        ) ++ declared
        val changes = desired.filter { case (key, value) => properties.get(key).forall(_ != value) }
        if (changes.nonEmpty) {
          val rendered = changes.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
          val _ = spark.sql(s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} SET TBLPROPERTIES ($rendered)")
        }
        // A positional index would still cover the leading columns beside the allow-list. Erasure must be able to
        // clean a table that drifted, so the property is repaired here; ingestion's strict create-or-validate still
        // refuses such a table. Files written while the property was set keep token statistics in their add actions
        // until they are rewritten (purge or OPTIMIZE) and the log checkpoint ages out; the repair only stops new ones.
        if (allowed.exists(_.nonEmpty) && properties.contains(DeltaStatsPolicy.NumIndexedColumnsProperty)) {
          val _ = spark.sql(
            s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} UNSET TBLPROPERTIES IF EXISTS " +
              s"('${DeltaStatsPolicy.NumIndexedColumnsProperty}')"
          )
          Some(spec.fold("unknown")(_.name))
        } else None
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
                    // The scratch copy holds subject tokens and is not a catalog table, so it gathers no statistics.
                    .option(DeltaStatsPolicy.NumIndexedColumnsProperty, "0")
                    .save(SparkPhysicalLocation.resolve(temporaryPath))
                  DeltaTables
                    .read(spark, temporaryPath)
                    .write
                    .format("delta")
                    .mode("overwrite")
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
            val hours = retention.deltaVacuumSafety.toMillis.toDouble / 3600000d
            val noCandidates = DeltaVacuumEligibility.canSkip(
              spark,
              path,
              hours,
              operational.maximumErasureEvidenceFiles
            )
            val candidates =
              if (noCandidates) 0L
              else spark.sql(s"VACUUM ${DeltaTables.sqlIdentifier(path)} RETAIN $hours HOURS DRY RUN").limit(1).count()
            if (candidates == 0L) 0L
            else DeltaTables.forPath(spark, path).vacuum(hours).count()
          }
        }
      }
      .map(_.sum)
}
