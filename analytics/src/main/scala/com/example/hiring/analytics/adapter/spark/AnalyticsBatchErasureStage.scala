package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

import scala.util.Try

/** Owns erasure matching, Delta evidence capture, checkpointing, and physical-presence verification. */
private[spark] final class AnalyticsBatchErasureStage[F[_]: Async: UUIDGen](
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
      .traverse_ { path =>
        DeltaTables.readIfExists(spark, path).fold[Either[AnalyticsError, Unit]](Right(())) { frame =>
          Either.cond(
            BatchSubjectMatching.matchedBySubject(frame, marker).limit(1).count() == 0L,
            (),
            AnalyticsError.MarkedSubjectRetained(path)
          )
        }
      }
  }

  def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): F[Long] = blocking {
    val marker = BatchSubjectMatching.markerRows(markerTokens)
    paths.inventory.subjectDelta.map(_.location).foldLeft(0L) { (total, path) =>
      total + DeltaTables
        .readIfExists(spark, path)
        .fold(0L)(BatchSubjectMatching.matchedBySubject(_, marker).count())
    }
  }

  def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): F[Vector[String]] =
    configureRawTablePrivacy(spark) *> blocking.either {
      val marker = BatchSubjectMatching.markerRows(markerTokens)
      val subjectBearingPaths = Set(paths.bronze, paths.quarantine, paths.lateFacts)
      val files = paths.inventory.subjectDelta
        .map(_.location)
        .flatMap { path =>
          if (!DeltaTables.exists(spark, path)) Vector.empty
          else {
            val frame = DeltaTables.read(spark, path)
            val columns = frame.columns.toSet
            val attributed = BatchSubjectMatching.matchedBySubject(frame, marker)
            val affected =
              if (subjectBearingPaths.contains(path) && !columns.contains(Columns.SubjectTokens)) frame
              else if (subjectBearingPaths.contains(path) && columns.contains(Columns.SubjectTokens)) {
                val unattributed =
                  frame.filter(col(Columns.SubjectTokens).isNull || size(col(Columns.SubjectTokens)) === 0)
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

  def checkpointPurgedRawLogs(spark: SparkSession): F[Vector[String]] =
    // One fresh nonce per table forces a log commit, so each nonce is drawn through UUIDGen before the Spark block.
    paths.inventory.subjectDelta.map(_.location).traverse(path => UUIDGen[F].randomUUID.map(path -> _)).flatMap {
      nonces =>
        blocking.either {
          val retiredLogs = nonces.flatMap { case (path, nonce) =>
            if (!DeltaTables.exists(spark, path)) Vector.empty
            else {
              val log = deltaLogFactory(spark, path)
              val tableIdentifier = SparkPhysicalLocation.resolve(path).replace("`", "``")
              spark.sql(
                s"ALTER TABLE delta.`$tableIdentifier` SET TBLPROPERTIES ('analytics.erasureCheckpointNonce' = '$nonce')"
              )
              val snapshot = log.update()
              val oldLogs = rawLogFiles(spark, path, Some(snapshot.version))
              log.checkpointAndCleanUpDeltaLog(snapshot, None)
              oldLogs
            }
          }.distinct
          Either.cond(
            retiredLogs.size <= MaximumErasureEvidenceFiles,
            retiredLogs,
            AnalyticsError.InvalidConfiguration("analytics erasure exceeds the bounded physical evidence file limit")
          )
        }
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
      if (DeltaTables.exists(spark, path)) {
        val log = deltaLogFactory(spark, path)
        log.checkpointAndCleanUpDeltaLog(log.update(), None)
      }
    }
  }

  def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): F[Unit] = blocking {
    if (DeltaTables.exists(spark, path)) {
      val markedSubjects =
        markerTokens.select(col(Columns.SubjectToken)).filter(col(Columns.SubjectToken).isNotNull).distinct()
      val columns = DeltaTables.read(spark, path).columns.toSet
      val rawScope = path == paths.bronze || path == paths.quarantine || path == paths.lateFacts
      if (rawScope) {
        val table = DeltaTables.forPath(spark, path)
        if (columns.contains(Columns.SubjectTokens))
          table.delete(col(Columns.SubjectTokens).isNull || size(col(Columns.SubjectTokens)) === 0)
        else table.delete()
      }
      val condition =
        if (columns.contains(Columns.SubjectTokens))
          "array_contains(target.subjectTokens, source.subjectToken)" +
            (if (columns.contains(Columns.SubjectToken))
               s" OR target.${Columns.SubjectToken} = source.${Columns.SubjectToken}"
             else "")
        else if (columns.contains(Columns.SubjectToken))
          s"target.${Columns.SubjectToken} = source.${Columns.SubjectToken}"
        else {
          DeltaTables.forPath(spark, path).delete()
          ""
        }
      if (condition.nonEmpty) {
        val _ = DeltaTables
          .forPath(spark, path)
          .as("target")
          .merge(markedSubjects.as("source"), condition)
          .whenMatched()
          .delete()
          .execute()
      }
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
