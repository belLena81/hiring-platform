package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import org.apache.spark.sql.types.StructType

/** Validates the Kafka columns required by bounded analytics ingestion. */
private[analytics] object KafkaRecordColumns {
  def validate[F[_]: Async](schema: StructType): F[Unit] = {
    val required = Set("topic", "partition", "offset", "timestamp", "value")
    val missing = required.diff(schema.fieldNames.toSet).toVector.sorted
    if (missing.isEmpty) Async[F].unit else Async[F].raiseError(AnalyticsError.InvalidSourceSchema(missing))
  }
}
