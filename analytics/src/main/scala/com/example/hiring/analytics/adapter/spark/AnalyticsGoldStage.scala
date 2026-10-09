package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsFunnelDayOutput
import com.example.hiring.analytics.domain.AnalyticsReportOutput
import com.example.hiring.analytics.domain.AnalyticsSkillPostingDayOutput
import com.example.hiring.analytics.domain.AnalyticsTimeToHireOutput
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

import java.sql.Timestamp
import java.time.Instant

/** Gold table rebuilds and bounded report extraction, kept outside the batch coordinator. */
private[spark] object AnalyticsGoldStage {
  private val MaximumReportRows = 10000

  private[spark] def validateOutputSchema(
      actual: StructType,
      expected: Vector[(String, DataType)]
  ): Either[AnalyticsError.InvalidGoldSchema.type, Unit] = {
    Either.cond(AnalyticsTableSchemas.matches(actual, expected), (), AnalyticsError.InvalidGoldSchema)
  }

  def rebuild[F[_]: Async](
      paths: AnalyticsLakehousePaths,
      silver: DataFrame,
      sparkExecution: SparkExecution[F]
  ): F[Unit] =
    for {
      funnel <- lakehouseEither[F, DataFrame](HiringGoldTransforms.wideFunnelDay(silver), sparkExecution)
      _ <- write(funnel, paths.funnelGold, sparkExecution)
      timeToHire <- lakehouseIO[F, DataFrame](HiringGoldTransforms.timeToHireAction[F](silver, sparkExecution))
      _ <- write(timeToHire, paths.timeToHireGold, sparkExecution)
      skills <- lakehouseEither[F, DataFrame](HiringGoldTransforms.skillPostingActivity(silver), sparkExecution)
      _ <- write(skills, paths.skillsGold, sparkExecution)
    } yield ()

  def clear[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      sparkExecution: SparkExecution[F]
  ): F[Unit] = lakehouse[F, Unit](
    {
      Vector(paths.funnelGold, paths.timeToHireGold, paths.skillsGold).foreach { path =>
        if (DeltaTables.exists(spark, path))
          DeltaTables.forPath(spark, path).delete()
      }
    },
    sparkExecution
  )

  def extract[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[AnalyticsReportOutput] =
    for {
      funnelRows <- rows(spark, paths.funnelGold, AnalyticsTableSchemas.funnel, sparkExecution)
      funnel = funnelRows.map(row =>
        AnalyticsFunnelDayOutput(
          row.getAs[Timestamp](Columns.Day).toInstant,
          row.getAs[Long](Columns.Created),
          row.getAs[Long](Columns.Accepted),
          row.getAs[Long](Columns.Declined),
          row.getAs[Long](Columns.Interview),
          row.getAs[Long](Columns.Hired),
          row.getAs[Long](Columns.Rejected)
        )
      )
      timeRows <- rows(spark, paths.timeToHireGold, AnalyticsTableSchemas.timeToHire, sparkExecution)
      _ <-
        if (timeRows.size > 1)
          Async[F].raiseError[Unit](AnalyticsError.ReportNotSingular)
        else Async[F].unit
      timeToHire = timeRows.headOption.map(row =>
        AnalyticsTimeToHireOutput(
          row.getAs[Double](Columns.P50Hours),
          row.getAs[Double](Columns.P75Hours),
          row.getAs[Double](Columns.P90Hours),
          row.getAs[Double](Columns.P95Hours),
          row.getAs[Long](Columns.EligibleCount),
          row.getAs[Long](Columns.ExcludedCount)
        )
      )
      skillRows <- rows(spark, paths.skillsGold, AnalyticsTableSchemas.skills, sparkExecution)
      skills = skillRows.map(row =>
        AnalyticsSkillPostingDayOutput(
          row.getAs[Timestamp](Columns.Day).toInstant,
          row.getAs[String](Columns.Skill),
          row.getAs[Long](Columns.Postings)
        )
      )
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, skills)

  private def rows[F[_]: Async](
      spark: SparkSession,
      path: String,
      expected: Vector[(String, DataType)],
      sparkExecution: SparkExecution[F]
  ): F[Vector[Row]] =
    lakehouseEither[F, Vector[Row]](
      {
        DeltaTables.readIfExists(spark, path).fold[Either[AnalyticsError, Vector[Row]]](Right(Vector.empty)) { frame =>
          validateOutputSchema(frame.schema, expected).map(_ => frame.limit(MaximumReportRows + 1).collect().toVector)
        }
      },
      sparkExecution
    ).flatMap { result =>
      if (result.size > MaximumReportRows)
        Async[F].raiseError(AnalyticsError.ReportRowLimitExceeded(MaximumReportRows))
      else if (result.exists(_.anyNull)) Async[F].raiseError(AnalyticsError.InvalidGoldSchema)
      else Async[F].pure(result)
    }

  private def write[F[_]: Async](frame: DataFrame, path: String, sparkExecution: SparkExecution[F]): F[Unit] =
    lakehouse[F, Unit](
      frame.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SparkPhysicalLocation.resolve(path)),
      sparkExecution
    )

  private def lakehouse[F[_]: Async, A](work: => A, sparkExecution: SparkExecution[F]): F[A] =
    adapt(sparkExecution(work))

  private def lakehouseIO[F[_]: Async, A](work: F[A]): F[A] = adapt(work)

  private def lakehouseEither[F[_]: Async, A](
      work: => Either[AnalyticsError, A],
      sparkExecution: SparkExecution[F]
  ): F[A] =
    lakehouse[F, Either[AnalyticsError, A]](work, sparkExecution).flatMap(Async[F].fromEither)

  private def adapt[F[_]: Async, A](work: F[A]): F[A] = LakehouseErrors.adapt(work)
}
