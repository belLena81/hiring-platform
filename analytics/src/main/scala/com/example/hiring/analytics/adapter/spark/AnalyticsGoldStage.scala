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

import java.time.{Instant, LocalDate, ZoneOffset}

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
  ): F[Unit] = {
    val lakehouse = new LakehouseOperation[F](sparkExecution)
    for {
      funnel <- lakehouse.either(HiringGoldTransforms.wideFunnelDay(silver))
      _ <- write(funnel, paths.funnelGold, AnalyticsDeltaCatalog.FunnelGold, lakehouse)
      timeToHire <- lakehouse.effect(HiringGoldTransforms.timeToHireAction[F](silver, sparkExecution))
      _ <- write(timeToHire, paths.timeToHireGold, AnalyticsDeltaCatalog.TimeToHireGold, lakehouse)
      skills <- lakehouse.either(HiringGoldTransforms.skillPostingActivity(silver))
      _ <- write(skills, paths.skillsGold, AnalyticsDeltaCatalog.SkillsGold, lakehouse)
    } yield ()
  }

  def clear[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      sparkExecution: SparkExecution[F]
  ): F[Unit] = new LakehouseOperation[F](sparkExecution)(
    Vector(paths.funnelGold, paths.timeToHireGold, paths.skillsGold).foreach { path =>
      if (DeltaTables.exists(spark, path))
        DeltaTables.forPath(spark, path).delete()
    }
  )

  /** Rebuilds Gold from stored Silver, or clears it when no Silver exists, then extracts the bounded report. */
  def rebuildAndExtract[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[AnalyticsReportOutput] =
    rebuildFromStoredSilver(spark, paths, sparkExecution) *> extract(spark, paths, asOf, sparkExecution)

  def rebuildFromStoredSilver[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      sparkExecution: SparkExecution[F]
  ): F[Unit] = {
    val lakehouse = new LakehouseOperation[F](sparkExecution)
    lakehouse(DeltaTables.exists(spark, paths.silver)).flatMap {
      case true  => lakehouse(DeltaTables.read(spark, paths.silver)).flatMap(rebuild(paths, _, sparkExecution))
      case false => clear(spark, paths, sparkExecution)
    }
  }

  /** The UTC start of a Gold calendar day, the instant the report contract carries. */
  private[spark] def dayStart(day: LocalDate): Instant = day.atStartOfDay(ZoneOffset.UTC).toInstant

  private type Decoded[A] = Either[AnalyticsError.InvalidGoldSchema.type, A]

  /** A present, correctly typed cell, or `InvalidGoldSchema`. */
  private def cell[A](row: Row, name: String)(typed: PartialFunction[Any, A]): Decoded[A] =
    Either
      .catchNonFatal(row.fieldIndex(name))
      .toOption
      .flatMap(index => Option(row.get(index)))
      .flatMap(typed.lift)
      .toRight(AnalyticsError.InvalidGoldSchema)

  private def stringCell(row: Row, name: String): Decoded[String] = cell(row, name) { case value: String => value }
  private def longCell(row: Row, name: String): Decoded[Long] = cell(row, name) { case value: java.lang.Long =>
    value.longValue
  }
  private def doubleCell(row: Row, name: String): Decoded[Double] = cell(row, name) { case value: java.lang.Double =>
    value.doubleValue
  }
  private def dayCell(row: Row, name: String): Decoded[Instant] = cell(row, name) {
    case value: LocalDate     => dayStart(value)
    case value: java.sql.Date => dayStart(value.toLocalDate)
  }

  private def decodeFunnel(row: Row): Decoded[AnalyticsFunnelDayOutput] =
    (
      dayCell(row, Columns.Day),
      longCell(row, Columns.Created),
      longCell(row, Columns.Accepted),
      longCell(row, Columns.Declined),
      longCell(row, Columns.Interview),
      longCell(row, Columns.Hired),
      longCell(row, Columns.Rejected)
    ).mapN(AnalyticsFunnelDayOutput.apply)

  private def decodeTimeToHire(row: Row): Decoded[AnalyticsTimeToHireOutput] =
    (
      doubleCell(row, Columns.P50Hours),
      doubleCell(row, Columns.P75Hours),
      doubleCell(row, Columns.P90Hours),
      doubleCell(row, Columns.P95Hours),
      longCell(row, Columns.EligibleCount),
      longCell(row, Columns.ExcludedCount)
    ).mapN(AnalyticsTimeToHireOutput.apply)

  private def decodeSkill(row: Row): Decoded[AnalyticsSkillPostingDayOutput] =
    (dayCell(row, Columns.Day), stringCell(row, Columns.Skill), longCell(row, Columns.Postings))
      .mapN(AnalyticsSkillPostingDayOutput.apply)

  def extract[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[AnalyticsReportOutput] =
    for {
      funnelRows <- rows(spark, paths.funnelGold, AnalyticsTableSchemas.funnel, sparkExecution)
      funnel <- Async[F].fromEither(funnelRows.traverse(decodeFunnel))
      timeRows <- rows(spark, paths.timeToHireGold, AnalyticsTableSchemas.timeToHire, sparkExecution)
      _ <-
        if (timeRows.size > 1)
          Async[F].raiseError[Unit](AnalyticsError.ReportNotSingular)
        else Async[F].unit
      timeToHire <- Async[F].fromEither(timeRows.headOption.traverse(decodeTimeToHire))
      skillRows <- rows(spark, paths.skillsGold, AnalyticsTableSchemas.skills, sparkExecution)
      skills <- Async[F].fromEither(skillRows.traverse(decodeSkill))
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, skills)

  private def rows[F[_]: Async](
      spark: SparkSession,
      path: String,
      expected: Vector[(String, DataType)],
      sparkExecution: SparkExecution[F]
  ): F[Vector[Row]] =
    new LakehouseOperation[F](sparkExecution)
      .either {
        DeltaTables.readIfExists(spark, path).fold[Either[AnalyticsError, Vector[Row]]](Right(Vector.empty)) { frame =>
          validateOutputSchema(frame.schema, expected).map(_ => frame.limit(MaximumReportRows + 1).collect().toVector)
        }
      }
      .flatMap { result =>
        if (result.size > MaximumReportRows)
          Async[F].raiseError(AnalyticsError.ReportRowLimitExceeded(MaximumReportRows))
        else Async[F].pure(result)
      }

  /** Creates or validates the declared target, then replaces only the data: a drifting frame fails, never the schema.
    */
  private def write[F[_]](
      frame: DataFrame,
      path: String,
      spec: DeltaTableSpec,
      lakehouse: LakehouseOperation[F]
  ): F[Unit] =
    lakehouse.either {
      AnalyticsTableSchemas.createOrValidate(frame.sparkSession, path, spec)
      Either
        .cond(AnalyticsTableSchemas.matches(frame.schema, spec.shape), (), AnalyticsError.DeltaSchemaMismatch(path))
        .map { _ =>
          frame.write
            .format("delta")
            .mode("overwrite")
            .save(SparkPhysicalLocation.resolve(path))
        }
    }
}
