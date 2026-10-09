package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import scala.util.control.NonFatal

/** Test adapter over an in-memory Kafka-shaped frame. Its frame must have Kafka's topic, partition, offset, timestamp
  * and value columns.
  */
private[analytics] final case class DataFrameBatchSource[F[_]: Async](
    records: DataFrame,
    sparkExecution: SparkExecution[F]
) extends BoundedOperationalEventSource[F] {
  override def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): F[Unit] =
    AnalyticsOffsetRanges.verify(frame, manifest, sparkExecution)

  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): F[DataFrame] =
    AnalyticsOffsetRanges.requireNonEmpty(manifest) *>
      sparkExecution(records.schema)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
        .flatMap(KafkaRecordColumns.validate) *>
      sparkExecution {
        val inManifest = manifest.offsetRanges.foldLeft(lit(false): Column) { (condition, range) =>
          condition || (
            col(Columns.Topic) === lit(AnalyticsTopic.unwrap(range.topic)) &&
              col(Columns.Partition) === lit(range.partition) &&
              col(Columns.Offset) >= lit(range.startOffset) &&
              col(Columns.Offset) < lit(range.endOffsetExclusive)
          )
        }
        records.filter(inManifest)
      }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
}

private[analytics] object DataFrameBatchSource {
  def apply[F[_]: Async](records: DataFrame): DataFrameBatchSource[F] =
    new DataFrameBatchSource[F](
      records,
      SparkBlockingExecution.forTests[F](scala.concurrent.ExecutionContext.parasitic)
    )
}
