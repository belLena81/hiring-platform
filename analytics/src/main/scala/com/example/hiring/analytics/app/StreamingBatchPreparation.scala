package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.spark.{Columns, LakehouseOperation}
import com.example.hiring.analytics.domain.{
  RangeFingerprint,
  StreamingBatchId,
  StreamingBatchIdentity,
  StreamingLineage,
  StreamingPartitionEndOffset,
  StreamingPartitionSummary
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.streaming.{StreamingBatchJournal, StreamingInputPreparation}

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sha2}

import java.time.Instant

/** Captures the immutable identity, input fingerprint and offset evidence of one Spark micro-batch. */
private[app] object StreamingBatchPreparation {
  def apply[F[_]: Async](
      execution: LakehouseOperation[F],
      parsed: DataFrame,
      lineage: StreamingLineage,
      batchId: StreamingBatchId,
      observedAt: Instant,
      sourceEndOffsets: Map[(String, Int), Long],
      journal: StreamingBatchJournal[F]
  ): F[StreamingInputPreparation] =
    execution
      .either {
        val rows = parsed
          .withColumn("_fingerprint", sha2(col(Columns.RawValue), 256))
          .select(col(Columns.Topic), col(Columns.Partition), col(Columns.Offset), col("_fingerprint"))
          .collect()
          .toVector
        val fingerprint = RangeFingerprint.ofSha256(
          rows
            .map(row => s"${row.getString(0)}:${row.getInt(1)}:${row.getLong(2)}:${row.getString(3)}")
            .sorted
            .mkString("\n")
        )
        val deliveredOffsets = rows
          .groupBy(row => (row.getString(0), row.getInt(1)))
          .toVector
          .sortBy(_._1)
          .traverse { case ((topic, partition), partitionRows) =>
            val offsets = partitionRows.map(_.getLong(2))
            StreamingPartitionSummary
              .from(topic, partition, offsets.min, offsets.max, offsets.size.toLong)
              .toEither
              .leftMap(AnalyticsError.InvalidInput.apply)
          }
        val endOffsets = sourceEndOffsets.toVector.sortBy(_._1).traverse { case ((topic, partition), offset) =>
          StreamingPartitionEndOffset.from(topic, partition, offset).toEither.leftMap(AnalyticsError.InvalidInput.apply)
        }
        (deliveredOffsets, endOffsets).mapN((delivered, ends) => (fingerprint, delivered, ends))
      }
      .flatMap { case (fingerprint, delivered, endOffsets) =>
        journal.latestWatermark(lineage).map { watermark =>
          StreamingInputPreparation(
            StreamingBatchIdentity(lineage, batchId),
            observedAt,
            watermark,
            fingerprint,
            endOffsets,
            delivered
          )
        }
      }
}
