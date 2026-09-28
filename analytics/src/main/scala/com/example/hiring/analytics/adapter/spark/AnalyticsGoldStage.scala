package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.*

import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

import java.sql.Timestamp
import java.time.Instant

/** Gold table rebuilds and bounded report extraction, kept outside the batch coordinator. */
private[spark] object AnalyticsGoldStage {
  private val MaximumReportRows = 10000
  private val FunnelSchema = Vector(
    "day" -> DataTypes.TimestampType,
    "created" -> DataTypes.LongType,
    "accepted" -> DataTypes.LongType,
    "declined" -> DataTypes.LongType,
    "interview" -> DataTypes.LongType,
    "hired" -> DataTypes.LongType,
    "rejected" -> DataTypes.LongType
  )
  private val TimeToHireSchema = Vector(
    "p50Hours" -> DataTypes.DoubleType,
    "p75Hours" -> DataTypes.DoubleType,
    "p90Hours" -> DataTypes.DoubleType,
    "p95Hours" -> DataTypes.DoubleType,
    "eligibleCount" -> DataTypes.LongType,
    "excludedCount" -> DataTypes.LongType
  )
  private val SkillsSchema = Vector(
    "day" -> DataTypes.TimestampType,
    "skill" -> DataTypes.StringType,
    "postings" -> DataTypes.LongType
  )

  private[spark] def validateOutputSchema(
      actual: StructType,
      expected: Vector[(String, DataType)]
  ): Either[AnalyticsError.InvalidGoldSchema.type, Unit] = {
    val fields = actual.fields.toVector.map(field => field.name -> field.dataType)
    Either.cond(fields == expected, (), AnalyticsError.InvalidGoldSchema)
  }

  def rebuild[F[_]: Async](paths: AnalyticsLakehousePaths, silver: DataFrame): F[Unit] =
    for {
      funnel <- lakehouse[F, DataFrame](HiringGoldTransforms.wideFunnelDay(silver))
      _ <- write(funnel, paths.funnelGold)
      timeToHire <- lakehouseIO[F, DataFrame](HiringGoldTransforms.timeToHireAction[F](silver))
      _ <- write(timeToHire, paths.timeToHireGold)
      skills <- lakehouse[F, DataFrame](HiringGoldTransforms.skillPostingActivity(silver))
      _ <- write(skills, paths.skillsGold)
    } yield ()

  def clear[F[_]: Async](spark: SparkSession, paths: AnalyticsLakehousePaths): F[Unit] = lakehouse[F, Unit] {
    Vector(paths.funnelGold, paths.timeToHireGold, paths.skillsGold).foreach { path =>
      if (DeltaTable.isDeltaTable(spark, path)) DeltaTable.forPath(spark, path).delete()
    }
  }

  def extract[F[_]: Async](
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      asOf: Instant
  ): F[AnalyticsReportOutput] =
    for {
      funnelRows <- rows(spark, paths.funnelGold, FunnelSchema)
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
      timeRows <- rows(spark, paths.timeToHireGold, TimeToHireSchema)
      _ <-
        if (timeRows.size > 1)
          Async[F].raiseError[Unit](
            AnalyticsError.LakehouseFailure(new IllegalStateException("time-to-hire report is not singular"))
          )
        else Async[F].unit
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
      skillRows <- rows(spark, paths.skillsGold, SkillsSchema)
      skills = skillRows.map(row =>
        AnalyticsSkillPostingDayOutput(
          row.getAs[Timestamp]("day").toInstant,
          row.getAs[String]("skill"),
          row.getAs[Long]("postings")
        )
      )
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, skills)

  private def rows[F[_]: Async](
      spark: SparkSession,
      path: String,
      expected: Vector[(String, DataType)]
  ): F[Vector[Row]] =
    lakehouseEither[F, Vector[Row]] {
      if (!DeltaTable.isDeltaTable(spark, path)) Right(Vector.empty)
      else {
        val frame = spark.read.format("delta").load(path)
        validateOutputSchema(frame.schema, expected).map(_ => frame.limit(MaximumReportRows + 1).collect().toVector)
      }
    }.flatMap { result =>
      if (result.size > MaximumReportRows)
        Async[F].raiseError(
          AnalyticsError.LakehouseFailure(new IllegalStateException(s"report output exceeds $MaximumReportRows rows"))
        )
      else if (result.exists(_.anyNull)) Async[F].raiseError(AnalyticsError.InvalidGoldSchema)
      else Async[F].pure(result)
    }

  private def write[F[_]: Async](frame: DataFrame, path: String): F[Unit] =
    lakehouse[F, Unit](frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))

  private def lakehouse[F[_]: Async, A](work: => A): F[A] =
    adapt(Async[F].blocking(work))

  private def lakehouseIO[F[_]: Async, A](work: F[A]): F[A] = adapt(work)

  private def lakehouseEither[F[_]: Async, A](work: => Either[AnalyticsError, A]): F[A] =
    lakehouse[F, Either[AnalyticsError, A]](work).flatMap(Async[F].fromEither)

  private def adapt[F[_]: Async, A](work: F[A]): F[A] = work.handleErrorWith {
    case error: AnalyticsError              => Async[F].raiseError(error)
    case scala.util.control.NonFatal(error) => Async[F].raiseError(AnalyticsError.LakehouseFailure(error))
    case error                              => Async[F].raiseError(error)
  }
}
