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
  * The time of the last run is a table property, so it is durable with the table, needs no second store, and is lost
  * together with the table on a lakehouse reset.
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

  /** The latest `OPTIMIZE` commit across the subject-attributed tables, if any is still in a retained log. */
  def lastCompactionAt(spark: SparkSession): F[Option[Instant]] =
    execution {
      paths.inventory.subjectDelta
        .map(_.location)
        .flatMap { path =>
          if (!DeltaTables.exists(spark, path)) None
          else
            Option(
              DeltaTables
                .forPath(spark, path)
                .history()
                .filter(col("operation") === "OPTIMIZE")
                .agg(max(col("timestamp")))
                .head()
                .getTimestamp(0)
            ).map(_.toInstant)
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
    execution {
      val before = DeltaLogFactory.system(spark, path).update().numOfFiles
      val _ = DeltaTables.forPath(spark, path).optimize().executeCompaction()
      val after = DeltaLogFactory.system(spark, path).update().numOfFiles
      // Recorded after the run, also when there was nothing to compact, so a table is not retried on every tick.
      val _ = spark.sql(
        s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} SET TBLPROPERTIES ('$LastOptimizedProperty' = '$at')"
      )
      DeltaCompactionResult(table, before, after)
    }

  private def report(result: DeltaCompactionResult): F[Unit] =
    logger.info(
      s"DELTA_COMPACTION table=${result.table} filesBefore=${result.filesBefore} filesAfter=${result.filesAfter}"
    )
}

private[analytics] object DeltaTableCompaction {

  /** Table property holding the UTC time of the last compaction run. */
  val LastOptimizedProperty = "analytics.lastOptimizedAt"
}
