package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*

import cats.effect.IO
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.Instant
import scala.util.control.NonFatal

/** Gold table rebuilds and bounded report extraction, kept outside the batch coordinator. */
private[batch] object AnalyticsGoldStage {
  private val MaximumReportRows = 10000

  private def lakehouse[A](work: => A): IO[A] = IO.blocking(work).adaptError {
    case error: AnalyticsError => error
    case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
  }

  def rebuild(paths: AnalyticsLakehousePaths, silver: DataFrame): IO[Unit] =
    for {
      funnel <- lakehouse(HiringGoldTransforms.wideFunnelDay(silver))
      _ <- write(funnel, paths.funnelGold)
      timeToHire <- HiringGoldTransforms.timeToHire(silver).adaptError { case NonFatal(cause) =>
        AnalyticsError.LakehouseFailure(cause)
      }
      _ <- write(timeToHire, paths.timeToHireGold)
      skills <- lakehouse(HiringGoldTransforms.skillPostingActivity(silver))
      _ <- write(skills, paths.skillsGold)
    } yield ()

  def clear(spark: SparkSession, paths: AnalyticsLakehousePaths): IO[Unit] = lakehouse {
    Vector(paths.funnelGold, paths.timeToHireGold, paths.skillsGold).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) DeltaTable.forPath(spark, path).delete()
    }
  }

  def extract(spark: SparkSession, paths: AnalyticsLakehousePaths, asOf: Instant): IO[AnalyticsReportOutput] =
    for {
      funnelRows <- rows(spark, paths.funnelGold)
      funnel = funnelRows.map(row =>
        AnalyticsFunnelDayOutput(
          row.getAs[Timestamp]("day").toInstant,
          row.getAs[Long]("created"),
          row.getAs[Long]("accepted"),
          row.getAs[Long]("declined"),
          row.getAs[Long]("interview"),
          row.getAs[Long]("hired"),
          row.getAs[Long]("rejected")
        )
      )
      timeRows <- rows(spark, paths.timeToHireGold)
      _ <-
        if (timeRows.size > 1)
          IO.raiseError[Unit](
            AnalyticsError.LakehouseFailure(new IllegalStateException("time-to-hire report is not singular"))
          )
        else IO.unit
      timeToHire = timeRows.headOption.map(row =>
        AnalyticsTimeToHireOutput(
          row.getAs[Double]("p50Hours"),
          row.getAs[Double]("p75Hours"),
          row.getAs[Double]("p90Hours"),
          row.getAs[Double]("p95Hours"),
          row.getAs[Long]("eligibleCount"),
          row.getAs[Long]("excludedCount")
        )
      )
      skillRows <- rows(spark, paths.skillsGold)
      skills = skillRows.map(row =>
        AnalyticsSkillPostingDayOutput(
          row.getAs[Timestamp]("day").toInstant,
          row.getAs[String]("skill"),
          row.getAs[Long]("postings")
        )
      )
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, skills)

  private def rows(spark: SparkSession, path: String): IO[Vector[Row]] =
    lakehouse {
      if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
      else spark.read.format("delta").load(path).limit(MaximumReportRows + 1).collect().toVector
    }.flatMap { result =>
      if (result.size > MaximumReportRows)
        IO.raiseError(
          AnalyticsError.LakehouseFailure(new IllegalStateException(s"report output exceeds $MaximumReportRows rows"))
        )
      else IO.pure(result)
    }

  private def write(frame: DataFrame, path: String): IO[Unit] =
    lakehouse(frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
}
