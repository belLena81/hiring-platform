package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

import scala.util.Try

/** Owns erasure matching, Delta evidence capture, checkpointing, and physical-presence verification. */
private[spark] final class AnalyticsBatchErasureStage[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F],
    configureRawTablePrivacy: SparkSession => F[Unit],
    maximumEvidenceFiles: Int,
    deltaLogFactory: DeltaLogFactory = DeltaLogFactory.system
) {
  private val blocking = execution
  private val MaximumErasureEvidenceFiles = maximumEvidenceFiles

  def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): F[Unit] = blocking.either {
    val marker = BatchSubjectMatching.markerRows(markerTokens)
    paths.inventory.subjectDelta
      .map(_.location)
      .foldLeft[Either[AnalyticsError, Unit]](Right(())) { (result, path) =>
        result.flatMap { _ =>
          if (DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) {
            val matched = BatchSubjectMatching.matchedBySubject(
              spark.read.format("delta").load(SparkPhysicalLocation.resolve(path)),
              marker
            )
            Either.cond(
              matched.limit(1).count() == 0L,
              (),
              AnalyticsError.LakehouseFailure(
                new IllegalStateException(s"marked subject remains in Delta dataset $path")
              )
            )
          } else Right(())
        }
      }
  }

  def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): F[Long] = blocking {
    val marker = BatchSubjectMatching.markerRows(markerTokens)
    paths.inventory.subjectDelta.map(_.location).foldLeft(0L) { (total, path) =>
      if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) total
      else
        total + BatchSubjectMatching
          .matchedBySubject(spark.read.format("delta").load(SparkPhysicalLocation.resolve(path)), marker)
          .count()
    }
  }

  def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): F[Vector[String]] =
    configureRawTablePrivacy(spark) *> blocking.either {
      val marker = BatchSubjectMatching.markerRows(markerTokens)
      val subjectBearingPaths = Set(paths.bronze, paths.quarantine, paths.lateFacts)
      val files = paths.inventory.subjectDelta
        .map(_.location)
        .flatMap { path =>
          if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) Vector.empty
          else {
            val frame = spark.read.format("delta").load(SparkPhysicalLocation.resolve(path))
            val columns = frame.columns.toSet
            val attributed = BatchSubjectMatching.matchedBySubject(frame, marker)
            val affected =
              if (subjectBearingPaths.contains(path) && !columns.contains("subjectTokens")) frame
              else if (subjectBearingPaths.contains(path) && columns.contains("subjectTokens")) {
                val unattributed = frame.filter(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
                attributed.unionByName(unattributed, allowMissingColumns = true)
              } else attributed
            val dataFiles = affected
              .select(input_file_name().as("filePath"))
              .distinct()
              .limit(MaximumErasureEvidenceFiles + 1)
              .collect()
              .toVector
              .map(_.getString(0))
              .distinct
            dataFiles ++ rawLogFiles(spark, path, None)
          }
        }
        .distinct
      Either.cond(
        files.size <= MaximumErasureEvidenceFiles,
        files,
        AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
      )
    }

  def checkpointPurgedRawLogs(spark: SparkSession): F[Vector[String]] = blocking.either {
    val retiredLogs = paths.inventory.subjectDelta
      .map(_.location)
      .flatMap { path =>
        if (!DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) Vector.empty
        else {
          val log = deltaLogFactory(spark, path)
          val tableIdentifier = SparkPhysicalLocation.resolve(path).replace("`", "``")
          spark.sql(
            s"ALTER TABLE delta.`$tableIdentifier` SET TBLPROPERTIES ('analytics.erasureCheckpointNonce' = '${java.util.UUID.randomUUID()}')"
          )
          val snapshot = log.update()
          val oldLogs = rawLogFiles(spark, path, Some(snapshot.version))
          log.checkpointAndCleanUpDeltaLog(snapshot, None)
          oldLogs
        }
      }
      .distinct
    Either.cond(
      retiredLogs.size <= MaximumErasureEvidenceFiles,
      retiredLogs,
      AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
    )
  }

  def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): F[Unit] = blocking.either {
    val configuration = spark.sparkContext.hadoopConfiguration
    val remaining = files.filter { value =>
      // Captured Spark/Hadoop file references can escape "+". They are evidence, not configured ownership roots.
      val path =
        if (value.regionMatches(true, 0, "file:", 0, 5))
          new org.apache.hadoop.fs.Path(new java.net.URI(value))
        else new org.apache.hadoop.fs.Path(value)
      path.getFileSystem(configuration).exists(path)
    }
    Either.cond(remaining.isEmpty, (), AnalyticsError.PhysicalReclamationUnverified)
  }

  def checkpointRawTableLogs(spark: SparkSession): F[Unit] = blocking {
    paths.inventory.delta.map(_.location).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) {
        val log = deltaLogFactory(spark, path)
        log.checkpointAndCleanUpDeltaLog(log.update(), None)
      }
    }
  }

  def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): F[Unit] = blocking {
    if (DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(path))) {
      val markedSubjects = markerTokens.select(col("subjectToken")).filter(col("subjectToken").isNotNull).distinct()
      val columns = spark.read.format("delta").load(SparkPhysicalLocation.resolve(path)).columns.toSet
      val rawScope = path == paths.bronze || path == paths.quarantine || path == paths.lateFacts
      if (rawScope) {
        val table = DeltaTable.forPath(spark, SparkPhysicalLocation.resolve(path))
        if (columns.contains("subjectTokens"))
          table.delete(col("subjectTokens").isNull || size(col("subjectTokens")) === 0)
        else table.delete()
      }
      val condition =
        if (columns.contains("subjectTokens"))
          "array_contains(target.subjectTokens, source.subjectToken)" +
            (if (columns.contains("subjectToken")) " OR target.subjectToken = source.subjectToken" else "")
        else if (columns.contains("subjectToken")) "target.subjectToken = source.subjectToken"
        else {
          DeltaTable.forPath(spark, SparkPhysicalLocation.resolve(path)).delete()
          ""
        }
      if (condition.nonEmpty)
        DeltaTable
          .forPath(spark, SparkPhysicalLocation.resolve(path))
          .as("target")
          .merge(markedSubjects.as("source"), condition)
          .whenMatched()
          .delete()
          .execute()
    }
  }

  private def rawLogFiles(spark: SparkSession, tablePath: String, beforeVersion: Option[Long]): Vector[String] = {
    val logDirectory = new org.apache.hadoop.fs.Path(SparkPhysicalLocation.resolve(s"$tablePath/_delta_log"))
    val fileSystem = logDirectory.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(logDirectory)) Vector.empty
    else
      Iterator
        .unfold(fileSystem.listFiles(logDirectory, false)) { entries =>
          if (entries.hasNext) Some(entries.next() -> entries) else None
        }
        .filter { status =>
          val prefix = status.getPath.getName.take(20)
          Try(prefix.toLong).exists(version => beforeVersion.forall(version < _))
        }
        .map(_.getPath.toUri.toASCIIString)
        .take(MaximumErasureEvidenceFiles + 1)
        .toVector
  }
}
