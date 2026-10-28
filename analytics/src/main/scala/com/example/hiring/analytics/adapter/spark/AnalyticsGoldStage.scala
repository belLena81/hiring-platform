package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsFunnelDayOutput
import com.example.hiring.analytics.domain.AnalyticsReportOutput
import com.example.hiring.analytics.domain.AnalyticsHiringStageMetric
import com.example.hiring.analytics.domain.AnalyticsRetention
import com.example.hiring.analytics.domain.AnalyticsSkillPostingDayOutput
import com.example.hiring.analytics.domain.AnalyticsStageDurationOutput
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

  /** Rebuilds the event-derived Gold (funnel and skills) from Silver. */
  private def rebuildEventGold[F[_]: Async](
      paths: AnalyticsLakehousePaths,
      silver: DataFrame,
      lakehouse: LakehouseOperation[F]
  ): F[Unit] =
    for {
      funnel <- lakehouse.either(HiringGoldTransforms.wideFunnelDay(silver))
      _ <- write(funnel, paths.funnelGold, AnalyticsDeltaCatalog.FunnelGold, lakehouse)
      skills <- lakehouse.either(HiringGoldTransforms.skillPostingActivity(silver))
      _ <- write(skills, paths.skillsGold, AnalyticsDeltaCatalog.SkillsGold, lakehouse)
    } yield ()

  /** Rebuilds time to hire and the hiring stage durations from the stage times that have not expired at `asOf`. */
  private def rebuildStageGold[F[_]: Async](
      paths: AnalyticsLakehousePaths,
      stageTimes: DataFrame,
      asOf: Instant,
      sparkExecution: SparkExecution[F],
      lakehouse: LakehouseOperation[F]
  ): F[Unit] =
    for {
      current <- lakehouse.either(HiringGoldTransforms.currentStageTimes(stageTimes, asOf))
      timeToHire <- lakehouse.effect(HiringGoldTransforms.timeToHireAction[F](current, sparkExecution))
      _ <- write(timeToHire, paths.timeToHireGold, AnalyticsDeltaCatalog.TimeToHireGold, lakehouse)
      durations <- lakehouse.either(HiringGoldTransforms.stageDurations(current))
      _ <- write(durations, paths.hiringStageDurationsGold, AnalyticsDeltaCatalog.HiringStageDurationsGold, lakehouse)
    } yield ()

  /** Rebuilds each Gold family from the tables given; a family whose source is absent is cleared. */
  def rebuild[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      silver: Option[DataFrame],
      stageTimes: Option[DataFrame],
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[Unit] = {
    val lakehouse = new LakehouseOperation[F](sparkExecution)
    silver.fold(clear(spark, paths.funnelGold, paths.skillsGold, sparkExecution))(
      rebuildEventGold(paths, _, lakehouse)
    ) *>
      stageTimes.fold(clear(spark, paths.timeToHireGold, paths.hiringStageDurationsGold, sparkExecution))(
        rebuildStageGold(paths, _, asOf, sparkExecution, lakehouse)
      )
  }

  private def clear[F[_]: Async](
      spark: SparkSession,
      first: String,
      second: String,
      sparkExecution: SparkExecution[F]
  ): F[Unit] =
    new LakehouseOperation[F](sparkExecution)(
      Vector(first, second).foreach { path =>
        if (DeltaTables.exists(spark, path)) DeltaTables.forPath(spark, path).delete()
      }
    )

  /** Rebuilds Gold from the stored Silver and stage times, then extracts the bounded report. */
  def rebuildAndExtract[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[AnalyticsReportOutput] =
    rebuildFromStoredTables(spark, paths, asOf, sparkExecution) *> extract(spark, paths, asOf, sparkExecution)

  /** Event Gold rebuilds when Silver exists, stage Gold when the stage times exist, even without Silver. */
  def rebuildFromStoredTables[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant,
      sparkExecution: SparkExecution[F]
  ): F[Unit] = {
    val lakehouse = new LakehouseOperation[F](sparkExecution)
    for {
      silver <- lakehouse(DeltaTables.readIfExists(spark, paths.silver))
      stageTimes <- lakehouse(DeltaTables.readIfExists(spark, paths.applicationStageTimes))
      _ <- rebuild(spark, paths, silver, stageTimes, asOf, sparkExecution)
    } yield ()
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

  private def decodeStageDuration(row: Row): Decoded[AnalyticsStageDurationOutput] =
    (
      stringCell(row, Columns.Metric).flatMap(
        AnalyticsHiringStageMetric.fromWire(_).toRight(AnalyticsError.InvalidGoldSchema)
      ),
      doubleCell(row, Columns.P50Hours),
      doubleCell(row, Columns.P75Hours),
      doubleCell(row, Columns.P90Hours),
      doubleCell(row, Columns.P95Hours),
      longCell(row, Columns.EligibleCount).filterOrElse(
        _ >= AnalyticsRetention.MinimumContributors,
        AnalyticsError.InvalidGoldSchema
      )
    ).mapN(AnalyticsStageDurationOutput.apply)

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
      durationRows <- rows(spark, paths.hiringStageDurationsGold, AnalyticsTableSchemas.stageDurations, sparkExecution)
      stageDurations <- Async[F].fromEither(durationRows.traverse(decodeStageDuration))
      skillRows <- rows(spark, paths.skillsGold, AnalyticsTableSchemas.skills, sparkExecution)
      skills <- Async[F].fromEither(skillRows.traverse(decodeSkill))
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, stageDurations.sortBy(_.metric.ordinal), skills)

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
