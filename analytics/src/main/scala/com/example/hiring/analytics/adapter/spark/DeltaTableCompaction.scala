package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Try

/** What one compaction run did to a table, for the log and for tests. Carries counts only. */
private[analytics] final case class DeltaCompactionResult(table: String, filesBefore: Long, filesAfter: Long)

/** Compacts the small files that streaming micro-batches leave in the subject-attributed tables.
  *
  * Runs only under the lakehouse mutex, so it never overlaps a writer or an erasure purge. `OPTIMIZE` rewrites data
  * files without changing table contents (`dataChange = false`) and is idempotent. The files it replaces stay on
  * storage, and keep their subject tokens, until VACUUM, which is why the erasure horizon also looks at the last
  * compaction (see `lastCompactionAt`).
  *
  * The time of the last run (for scheduling) and its completion time (for the horizon) are table properties, so it is
  * durable with the table, needs no second store, and is lost together with the table on a lakehouse reset.
  */
private[analytics] final class DeltaTableCompaction[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F],
    logger: Logger[F]
) {
  import DeltaTableCompaction.*

  /** Runs `OPTIMIZE` on each subject-attributed table whose interval has elapsed. A zero interval disables it. */
  def compactDue(spark: SparkSession, at: Instant, interval: FiniteDuration): F[Vector[DeltaCompactionResult]] =
    if (interval <= Duration.Zero) Async[F].pure(Vector.empty)
    else
      paths.inventory.subjectDelta.map(_.location).traverseFilter { path =>
        execution(due(spark, path, at, interval)).flatMap {
          case None        => Async[F].pure(Option.empty[DeltaCompactionResult])
          case Some(table) => compact(spark, path, table, at).flatTap(report).map(Some(_))
        }
      }

  /** An upper bound of the latest `OPTIMIZE` commit across the subject-attributed tables, if there is one.
    *
    * Each scheduled run records its completion time and the table version its `OPTIMIZE` produced. The history is then
    * read only for the versions after that one, so any later `OPTIMIZE`, scheduled or run by hand, is seen, and the
    * read is bounded by the commits since the last scheduled run, not by the retained log. With no recorded version, an
    * unparsable one, or a run marked pending (crash between the `OPTIMIZE` commit and its record, or a run in progress)
    * the full history is read, which is exact. The completion time is taken after the commit, so the result is never
    * earlier than the real one.
    */
  def lastCompactionAt(spark: SparkSession): F[Option[Instant]] =
    execution {
      paths.inventory.subjectDelta
        .map(_.location)
        .flatMap { path =>
          if (!DeltaTables.exists(spark, path)) None
          else {
            val snapshot = DeltaLogFactory.system(spark, path).update()
            val configuration = snapshot.metadata.configuration
            val recorded = configuration.get(CompletedProperty).flatMap(value => Try(Instant.parse(value)).toOption)
            val recordedVersion = configuration.get(CompletedVersionProperty).flatMap(_.toLongOption)
            val history = DeltaTables.forPath(spark, path)
            def newest(rows: org.apache.spark.sql.DataFrame): Option[Instant] =
              Option(
                rows.filter(col("operation") === "OPTIMIZE").agg(max(col("timestamp"))).head().getTimestamp(0)
              ).map(_.toInstant)
            val inspected =
              if (configuration.contains(PendingProperty)) newest(history.history())
              else
                recordedVersion.filter(_ <= snapshot.version) match {
                  case Some(version) if recorded.isDefined =>
                    val newer = snapshot.version - version
                    if (newer <= 0L) None else newest(history.history(Math.toIntExact(newer)))
                  case _ => newest(history.history())
                }
            Vector(recorded, inspected).flatten.maxOption
          }
        }
        .maxOption
    }

  private def due(spark: SparkSession, path: String, at: Instant, interval: FiniteDuration): Option[String] =
    Option
      .when(DeltaTables.exists(spark, path)) {
        val last = DeltaLogFactory
          .system(spark, path)
          .update()
          .metadata
          .configuration
          .get(LastOptimizedProperty)
          .flatMap(value => Try(Instant.parse(value)).toOption)
        Option.when(last.forall(previous => !at.isBefore(previous.plusMillis(interval.toMillis))))(tableName(path))
      }
      .flatten

  private def tableName(path: String): String =
    AnalyticsDeltaCatalog.specAt(paths, path).fold("unknown")(_.name)

  private def compact(spark: SparkSession, path: String, table: String, at: Instant): F[DeltaCompactionResult] =
    for {
      startedAt <- Async[F].realTimeInstant
      // Marked before the run so a crash after the OPTIMIZE commit still leaves a trace that lastCompactionAt reads.
      _ <- execution(setProperties(spark, path, Map(PendingProperty -> startedAt.toString)))
      before <- execution(DeltaLogFactory.system(spark, path).update().numOfFiles)
      _ <- execution(DeltaTables.forPath(spark, path).optimize().executeCompaction())
      after <- execution(DeltaLogFactory.system(spark, path).update().numOfFiles)
      // Taken after the commit, so it is never earlier than it. Recorded also when there was nothing to compact, so a
      // table is not retried on every tick.
      finishedAt <- Async[F].realTimeInstant
      producedVersion <- execution(DeltaLogFactory.system(spark, path).update().version)
      _ <- execution {
        setProperties(
          spark,
          path,
          Map(
            LastOptimizedProperty -> at.toString,
            CompletedProperty -> finishedAt.toString,
            CompletedVersionProperty -> producedVersion.toString
          )
        )
        val _ = spark.sql(
          s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} UNSET TBLPROPERTIES IF EXISTS ('$PendingProperty')"
        )
      }
    } yield DeltaCompactionResult(table, before, after)

  private def setProperties(spark: SparkSession, path: String, properties: Map[String, String]): Unit = {
    val rendered = properties.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
    val _ = spark.sql(s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} SET TBLPROPERTIES ($rendered)")
  }

  private def report(result: DeltaCompactionResult): F[Unit] =
    logger.info(
      s"DELTA_COMPACTION table=${result.table} filesBefore=${result.filesBefore} filesAfter=${result.filesAfter}"
    )
}

private[analytics] object DeltaTableCompaction {

  /** Table property holding the UTC time of the last compaction run. */
  val LastOptimizedProperty = "analytics.lastOptimizedAt"

  /** Table property holding the wall-clock time at which the last run finished, after its commit. It, not the
    * scheduling time above, bounds the erasure horizon.
    */
  val CompletedProperty = "analytics.lastOptimizeCompletedAt"

  /** Table version at which the last scheduled run finished its `OPTIMIZE`; later history is searched for others. */
  val CompletedVersionProperty = "analytics.lastOptimizeCompletedVersion"

  /** Table property set while a compaction run is in progress and removed when it has been recorded. */
  val PendingProperty = "analytics.optimizePendingAt"

}
