package com.example.hiring.analytics.batch

import com.example.hiring.analytics.*

import cats.effect.IO
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, max, min}

import scala.util.control.NonFatal

/** Checks the requested coordinates before a run can write its first manifest row. */
private[analytics] object AnalyticsOffsetRanges {
  final case class Observed(count: Long, first: Long, last: Long)

  def requireNonEmpty(manifest: AnalyticsRunManifest): IO[Unit] =
    manifest.offsetRanges.find(range => range.startOffset == range.endOffsetExclusive) match {
      case Some(range) =>
        IO.raiseError(AnalyticsError.EmptyRequestedRange(range.topic, range.partition, range.startOffset))
      case None => IO.unit
    }

  def available(
      range: PartitionOffsetRange,
      earliestAvailable: Long,
      latestExclusive: Long
  ): Either[AnalyticsError, Unit] =
    if (range.startOffset < earliestAvailable)
      Left(
        AnalyticsError.ExpiredOffsetRange(
          range.topic,
          range.partition,
          range.startOffset,
          earliestAvailable
        )
      )
    else if (range.endOffsetExclusive > latestExclusive)
      Left(
        AnalyticsError.MissingOffsetRange(
          range.topic,
          range.partition,
          range.endOffsetExclusive - range.startOffset,
          (latestExclusive - range.startOffset).max(0L)
        )
      )
    else Right(())

  def complete(range: PartitionOffsetRange, observed: Option[Observed]): Either[AnalyticsError, Unit] = {
    val requested = range.endOffsetExclusive - range.startOffset
    observed match {
      case Some(actual)
          if actual.count == requested && actual.first == range.startOffset &&
            actual.last == range.endOffsetExclusive - 1L =>
        Right(())
      case other =>
        Left(AnalyticsError.MissingOffsetRange(range.topic, range.partition, requested, other.fold(0L)(_.count)))
    }
  }

  def verify(frame: DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
    observedOffsets(frame).flatMap(found => verifyObservedOffsets(manifest, found, allowKafkaGaps = false))

  /** Kafka offset coordinates are not dense record counts: control records consume offsets and `read_committed` omits
    * aborted records. The Kafka source validates low/high broker bounds before reading and enables failOnDataLoss; here
    * we check that every returned data record lies within its assigned range without treating legitimate transactional
    * gaps as missing records.
    */
  def verifyCommittedKafkaRange(frame: DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
    observedOffsets(frame).flatMap(found => verifyObservedOffsets(manifest, found, allowKafkaGaps = true))

  private def observedOffsets(frame: DataFrame): IO[Map[(String, Int), Observed]] =
    IO.blocking {
      frame
        .groupBy(col("topic"), col("partition"))
        .agg(
          countDistinct(col("offset")).as("observed"),
          min(col("offset")).as("first"),
          max(col("offset")).as("last")
        )
        .collect()
        .iterator
        .map(row => (row.getString(0), row.getInt(1)) -> Observed(row.getLong(2), row.getLong(3), row.getLong(4)))
        .toMap
    }.adaptError { case NonFatal(cause) => AnalyticsError.SourceReadFailure(cause) }

  private def verifyObservedOffsets(
      manifest: AnalyticsRunManifest,
      found: Map[(String, Int), Observed],
      allowKafkaGaps: Boolean
  ): IO[Unit] = {
    val requested: Set[(String, Int)] = manifest.offsetRanges
      .map(range => (range.topic, PartitionOffsetRange.partitionNumber(range.partition)))
      .toSet
    found.keySet.diff(requested).headOption match {
      case Some((topic, partition)) =>
        IO.raiseError(AnalyticsError.UnexpectedOffsetPartition(topic, partition))
      case None =>
        IO.fromEither(manifest.offsetRanges.foldLeft[Either[AnalyticsError, Unit]](Right(())) { (result, range) =>
          result.flatMap { _ =>
            val observation = found.get((range.topic, range.partition))
            if (allowKafkaGaps) verifyKafkaCoordinates(range, observation)
            else complete(range, observation)
          }
        })
    }
  }

  private def verifyKafkaCoordinates(
      range: PartitionOffsetRange,
      observed: Option[Observed]
  ): Either[AnalyticsError, Unit] = {
    val requested = range.endOffsetExclusive - range.startOffset
    observed match {
      case None => Right(())
      case Some(actual)
          if actual.count <= requested && actual.first >= range.startOffset && actual.last < range.endOffsetExclusive =>
        Right(())
      case other =>
        Left(AnalyticsError.MissingOffsetRange(range.topic, range.partition, requested, other.fold(0L)(_.count)))
    }
  }
}
