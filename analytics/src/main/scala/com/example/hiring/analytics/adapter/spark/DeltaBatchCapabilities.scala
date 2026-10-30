package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.errors.AnalyticsError

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, count, lit, min, sha2, struct, to_json, when}
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant

/** Owns idempotent Delta writes used by the bounded batch pipeline. */
private[analytics] final class DeltaBatchWriter[F[_]](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F]
) extends DeltaWriter[F] {
  override def merge(source: DataFrame, path: String, condition: String): F[Unit] =
    mergeWithFreshness(source, path, condition, None)

  override def mergeWhenFresh(source: DataFrame, path: String, condition: String, at: () => Instant): F[Unit] =
    mergeWithFreshness(source, path, condition, Some(at))

  private def mergeWithFreshness(
      source: DataFrame,
      path: String,
      condition: String,
      freshness: Option[() => Instant]
  ): F[Unit] = execution.either {
    val target = AnalyticsTableSchemas.targetOf(paths, path)
    AnalyticsTableSchemas.createOrValidate(source.sparkSession, path, target)
    if (!AnalyticsTableSchemas.matches(source.schema, target.shape)) throw AnalyticsError.DeltaSchemaMismatch(path)
    val mutation = DeltaTables
      .forPath(source.sparkSession, path)
      .as("target")
      .merge(source.as("source"), MergeKeyBounds.bounded(source, condition))
      .whenNotMatched()
      .insertAll()
    freshnessVerdict(source, freshness).map(_ => mutation.execute()).map(_ => ())
  }

  private def freshnessVerdict(
      facts: DataFrame,
      freshness: Option[() => Instant]
  ): Either[AnalyticsError, Unit] = freshness match {
    case None     => Right(())
    case Some(at) =>
      val expiry = facts.agg(min(col(Columns.ExpiresAt)), count(when(col(Columns.ExpiresAt).isNull, lit(1)))).head()
      val minimumExpiry = Option(expiry.getAs[Timestamp](0)).map(_.toInstant)
      // Read the clock after Spark normalization and Delta preparation, on the same serialized executor.
      Either.cond(
        expiry.getLong(1) == 0L && minimumExpiry.exists(_.isAfter(at())),
        (),
        AnalyticsError.LateFactReplayRejected
      )
  }

  override def mergeStageTimes(
      facts: DataFrame,
      retention: AnalyticsRetentionSettings,
      freshness: Option[() => Instant]
  ): F[Unit] = execution.either {
    freshnessVerdict(facts, freshness).flatMap(_ =>
      ApplicationStageTimes.mergeFacts(facts.sparkSession, paths, facts, retention)
    )
  }

  override def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
    frame
      .withColumn(Columns.IngestedAt, lit(Timestamp.from(now)))
      .withColumn(Columns.ExpiresAt, AnalyticsTableSchemas.expiryAt(now, days.toLong))
}

/** Reads an existing Delta table or creates an empty frame with the caller's expected schema. */
private[analytics] final class DeltaBatchReader[F[_]](execution: SparkExecution[F]) {
  def readOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame] = execution {
    DeltaTables.readIfExists(spark, path).getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema))
  }
}

/** Builds stable quarantine keys for malformed and conflicting operational events. */
private[analytics] object QuarantineIdentifier {
  def apply(): Column =
    when(
      col(Columns.RawValue).isNull,
      concat(
        lit("tombstone:"),
        to_json(
          struct(
            col(Columns.Topic).as(Columns.Topic),
            col(Columns.Partition).as(Columns.Partition),
            col(Columns.Offset).as(Columns.Offset)
          )
        )
      )
    ).otherwise(sha2(col(Columns.RawValue), 256))
}
