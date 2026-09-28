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

import cats.effect.IO
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

import java.sql.Timestamp
import java.time.Instant

/** Gold table rebuilds and bounded report extraction, kept outside the batch coordinator. */
private[spark] object AnalyticsGoldStage extends LakehouseOperation {
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

  def rebuild(paths: AnalyticsLakehousePaths, silver: DataFrame): IO[Unit] =
    for {
      funnel <- lakehouse(HiringGoldTransforms.wideFunnelDay(silver))
      _ <- write(funnel, paths.funnelGold)
      timeToHire <- lakehouseIO(HiringGoldTransforms.timeToHireAction(silver))
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
      skillRows <- rows(spark, paths.skillsGold, SkillsSchema)
      skills = skillRows.map(row =>
        AnalyticsSkillPostingDayOutput(
          row.getAs[Timestamp]("day").toInstant,
          row.getAs[String]("skill"),
          row.getAs[Long]("postings")
        )
      )
    } yield AnalyticsReportOutput(asOf, funnel, timeToHire, skills)

  private def rows(spark: SparkSession, path: String, expected: Vector[(String, DataType)]): IO[Vector[Row]] =
    lakehouseEither {
      if (!DeltaTable.isDeltaTable(spark, path)) Right(Vector.empty)
      else {
        val frame = spark.read.format("delta").load(path)
        validateOutputSchema(frame.schema, expected).map(_ => frame.limit(MaximumReportRows + 1).collect().toVector)
      }
    }.flatMap { result =>
      if (result.size > MaximumReportRows)
        IO.raiseError(
          AnalyticsError.LakehouseFailure(new IllegalStateException(s"report output exceeds $MaximumReportRows rows"))
        )
      else if (result.exists(_.anyNull)) IO.raiseError(AnalyticsError.InvalidGoldSchema)
      else IO.pure(result)
    }

  private def write(frame: DataFrame, path: String): IO[Unit] =
    lakehouse(frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path))
}
