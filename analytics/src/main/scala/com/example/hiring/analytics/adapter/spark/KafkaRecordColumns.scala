package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import org.apache.spark.sql.types.{ArrayType, BinaryType, StringType, StructField, StructType}

/** Validates the Kafka columns required by bounded analytics ingestion. `headers` (from `includeHeaders=true`) is
  * optional because records written before headers existed carry none; when present it must have the Kafka shape.
  */
private[analytics] object KafkaRecordColumns {
  val HeadersType: ArrayType = ArrayType(
    StructType(Seq(StructField("key", StringType), StructField("value", BinaryType))),
    containsNull = true
  )

  def validate[F[_]: Async](schema: StructType): F[Unit] = {
    val required = Set(Columns.Topic, Columns.Partition, Columns.Offset, Columns.Timestamp, Columns.Value)
    val badHeaders =
      schema.fields.find(_.name == Columns.Headers).filterNot(f => f.dataType == HeadersType).map(_ => Columns.Headers)
    val missing = required.diff(schema.fieldNames.toSet).toVector.sorted ++ badHeaders
    if (missing.isEmpty) Async[F].unit else Async[F].raiseError(AnalyticsError.InvalidSourceSchema(missing))
  }
}
