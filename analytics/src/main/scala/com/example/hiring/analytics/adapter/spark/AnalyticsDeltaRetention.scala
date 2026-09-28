package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.typelevel.log4cats.Logger

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

/** Applies Delta retention/privacy properties and reclaims expired table files. */
private[analytics] final class AnalyticsDeltaRetention[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    operational: AnalyticsOperationalSettings,
    execution: SparkExecution[F],
    logger: Logger[F]
) {
  private val retention = operational.retention

  def configureRawTables(spark: SparkSession): F[Unit] = execution {
    val desiredVacuumRetention = s"interval ${retention.deltaVacuumSafetyDays} days"
    val desiredLogRetention = s"interval ${retention.deltaLogRetentionDays} days"
    val rawPaths = Set(paths.bronze, paths.quarantine)
    val tables = Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold
    )
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
      if (DeltaTable.isDeltaTable(spark, path)) {
        val escaped = path.replace("`", "``")
        val properties = DeltaTable
          .forPath(spark, path)
          .detail()
          .select("properties")
          .head()
          .getAs[scala.collection.Map[String, String]]("properties")
        val desired = Map(
          "delta.deletedFileRetentionDuration" -> desiredVacuumRetention,
          "delta.logRetentionDuration" -> desiredLogRetention
        ) ++ (if (rawPaths.contains(path)) Map("delta.dataSkippingNumIndexedCols" -> "0") else Map.empty)
        val changes = desired.filter { case (key, value) => properties.get(key).forall(_ != value) }
        if (changes.nonEmpty) {
          val rendered = changes.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
          spark.sql(s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES ($rendered)")
        }
      }
    }
  }

  def expire(spark: SparkSession, path: String, at: Instant): F[Unit] = execution {
    if (DeltaTable.isDeltaTable(spark, path))
      DeltaTable.forPath(spark, path).delete(col("expiresAt") <= lit(Timestamp.from(at)))
  }

  def vacuumExpiredFiles(spark: SparkSession): F[Long] =
    Vector(
      paths.bronze,
      paths.quarantine,
      paths.silver,
      paths.funnelGold,
      paths.timeToHireGold,
      paths.skillsGold
    ).foldLeft(Async[F].pure(0L)) { (removedFiles, path) =>
      removedFiles.flatMap { count =>
        execution(DeltaTable.isDeltaTable(spark, path)).flatMap {
          case false => Async[F].pure(count)
          case true  =>
            val temporaryPath = s"${paths.root.stripSuffix("/")}/control/purge-rewrite-${UUID.randomUUID()}"
            DeltaPurgeRewrite.temporaryPath[F](spark, temporaryPath, execution).use { _ =>
              execution {
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
        Async[F].raiseError(error)
    }
}
