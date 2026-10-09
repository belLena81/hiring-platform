package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsLakehouseIdentity,
  AnalyticsOffset,
  AnalyticsPartition,
  StreamingActivationIdentity
}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.{Validated, ValidatedNec}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Interval
import _root_.pureconfig.*

import scala.concurrent.duration.*
import java.net.URI
import java.nio.file.Paths

type MaximumStreamingReplayRecords = Int :| Interval.Closed[1, 1000]
type MaximumOffsetsPerTrigger = Int :| Interval.Closed[1, 100000]

object MaximumOffsetsPerTrigger {
  extension (value: MaximumOffsetsPerTrigger) def value: Int = value
}

final case class KafkaStartingOffset private (partition: AnalyticsPartition, offset: AnalyticsOffset)

object KafkaStartingOffset {
  def from(partition: Int, offset: Long): ValidatedNec[String, KafkaStartingOffset] =
    (AnalyticsPartition.from(partition).toValidatedNec, AnalyticsOffset.from(offset).toValidatedNec)
      .mapN(KafkaStartingOffset.apply)
}

final case class AnalyticsStreamingSettings private (
    streamId: AnalyticsNonBlank,
    activationGrantId: AnalyticsNonBlank,
    checkpointLocation: AnalyticsNonBlank,
    triggerInterval: FiniteDuration,
    maxOffsetsPerTrigger: MaximumOffsetsPerTrigger,
    maximumReplayRecords: MaximumStreamingReplayRecords,
    initialOffsets: Vector[KafkaStartingOffset],
    maintenanceInterval: FiniteDuration,
    progressRetention: FiniteDuration
) {
  def activationIdentity(
      sourceClusterId: String,
      sourceTopicId: String,
      topic: String,
      lakehouseRoot: String
  ): Either[AnalyticsError, StreamingActivationIdentity] = {
    val offsets = initialOffsets
      .sortBy(offset => AnalyticsPartition.unwrap(offset.partition))
      .map(offset => s"${AnalyticsPartition.unwrap(offset.partition)}:${AnalyticsOffset.unwrap(offset.offset)}")
      .mkString(",")
    val settings = Vector(
      streamId,
      checkpointLocation,
      triggerInterval.toString,
      maxOffsetsPerTrigger.toString,
      maximumReplayRecords.toString,
      AnalyticsStreamingSettings.ConsumerGroupId,
      offsets,
      maintenanceInterval.toString,
      progressRetention.toString
    ).mkString("\n")
    for {
      _ <- Either.cond(
        sourceClusterId.trim.nonEmpty && sourceTopicId.trim.nonEmpty,
        (),
        AnalyticsError.InvalidConfiguration("Kafka cluster and topic IDs are required for streaming identity")
      )
      lakehouseId <- AnalyticsLakehouseIdentity
        .from(lakehouseRoot)
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics lakehouse root is invalid"))
    } yield StreamingActivationIdentity(
      streamId,
      AnalyticsDigest.sha256Hex(s"$sourceClusterId\n$topic\n$sourceTopicId"),
      lakehouseId,
      AnalyticsDigest.sha256Hex("hiring-operational-event-envelope:seven-field:v1"),
      AnalyticsDigest.sha256Hex(settings)
    )
  }
}

object AnalyticsStreamingSettings {
  import AnalyticsConfigReaders.{decode, given}
  import io.github.iltotore.iron.pureconfig.given

  val ConsumerGroupId: String = "hiring-analytics-streaming-v1"

  private final case class KafkaStartingOffsetInput(partition: Int, offset: Long) derives ConfigReader
  private final case class StreamingSettingsInput(
      streamId: AnalyticsNonBlank,
      activationGrantId: AnalyticsNonBlank,
      checkpointLocation: AnalyticsNonBlank,
      triggerInterval: StreamingTriggerInterval,
      maxOffsetsPerTrigger: MaximumOffsetsPerTrigger,
      maximumReplayRecords: MaximumStreamingReplayRecords,
      initialOffsets: Vector[KafkaStartingOffsetInput],
      maintenanceInterval: AnalyticsPositiveDuration,
      progressRetention: AnalyticsPositiveDuration
  ) derives ConfigReader

  /** Check before checkpoint creation that the explicit offsets cover exactly the broker's current partition set. */
  def validatePartitionCoverage(
      settings: AnalyticsStreamingSettings,
      sourcePartitions: Set[Int]
  ): Either[AnalyticsError, Unit] = {
    val configuredPartitions = settings.initialOffsets.map(offset => AnalyticsPartition.unwrap(offset.partition)).toSet
    Either.cond(
      configuredPartitions == sourcePartitions,
      (),
      AnalyticsError.InvalidConfiguration(
        "analytics.streaming.initial-offsets must match the Kafka source partition set"
      )
    )
  }

  /** Decodes `analytics.streaming` from `source`; field bounds come from the readers, offset rules are checked here. */
  private[analytics] def read(source: ConfigSource): ValidatedNec[String, AnalyticsStreamingSettings] =
    decode[StreamingSettingsInput](source, "analytics.streaming").andThen(validate)

  private def validate(raw: StreamingSettingsInput): ValidatedNec[String, AnalyticsStreamingSettings] = {
    val offsets = raw.initialOffsets.traverse(offset => KafkaStartingOffset.from(offset.partition, offset.offset))
    val uniquePartitions = Validated.condNec(
      raw.initialOffsets.map(_.partition).distinct.size == raw.initialOffsets.size,
      (),
      "analytics.streaming.initial-offsets must contain each partition once"
    )
    val nonEmptyOffsets = Validated.condNec(
      raw.initialOffsets.nonEmpty,
      (),
      "analytics.streaming.initial-offsets must explicitly name every source partition"
    )
    (
      validateCheckpointLocation(raw.checkpointLocation),
      (offsets, uniquePartitions, nonEmptyOffsets).mapN((validOffsets, _, _) => validOffsets)
    ).mapN((checkpointLocation, validOffsets) =>
      AnalyticsStreamingSettings(
        raw.streamId,
        raw.activationGrantId,
        checkpointLocation,
        raw.triggerInterval,
        raw.maxOffsetsPerTrigger,
        raw.maximumReplayRecords,
        validOffsets,
        raw.maintenanceInterval,
        raw.progressRetention
      )
    )
  }

  private def validateCheckpointLocation(value: AnalyticsNonBlank): ValidatedNec[String, AnalyticsNonBlank] = {
    val location = Either
      .catchNonFatal {
        val uri = new URI(value)
        val path = Paths.get(uri).normalize()
        val owned =
          AnalyticsRuntimeDirectory.owns(path, AnalyticsRuntimeDirectory.Checkpoints, allowCategoryRoot = false)
        uri.getScheme == "file" && uri.getAuthority == null && uri.getQuery == null && uri.getFragment == null &&
        owned && AnalyticsLakehouseIdentity.from(value).isRight &&
        !Option(uri.getPath).toVector.flatMap(_.split("/")).contains("..")
      }
      .leftMap(_ => "checkpoint-location must be inside the owned analytics runtime checkpoint directory")
      .flatMap(isAllowed =>
        Either.cond(
          isAllowed,
          value,
          "checkpoint-location must be inside the owned analytics runtime checkpoint directory"
        )
      )
    location.toValidatedNec
  }
}
