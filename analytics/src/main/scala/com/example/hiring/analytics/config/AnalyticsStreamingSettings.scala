package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{
  AnalyticsDigest,
  AnalyticsLakehouseIdentity,
  AnalyticsEventTimePolicy,
  AnalyticsOffset,
  AnalyticsPartition,
  StreamingActivationIdentity
}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.ValidatedNec
import cats.effect.Async
import cats.syntax.all.*
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Interval
import pureconfig.*
import pureconfig.generic.derivation.{
  ConfigReaderDerivation,
  CoproductConfigReaderDerivation,
  ProductConfigReaderDerivation
}

import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*
import java.net.URI
import java.nio.file.Paths

type MaximumStreamingReplayRecords = Int :| Interval.Closed[1, 1000]
type MaximumOffsetsPerTrigger = Int :| Interval.Closed[1, 100000]

object MaximumOffsetsPerTrigger {
  extension (value: MaximumOffsetsPerTrigger) def value: Int = value.asInstanceOf[Int]
}

final case class KafkaStartingOffset private (partition: AnalyticsPartition, offset: AnalyticsOffset)

object KafkaStartingOffset {
  def from(partition: Int, offset: Long): ValidatedNec[String, KafkaStartingOffset] =
    (AnalyticsPartition.from(partition).toValidatedNec, AnalyticsOffset.from(offset).toValidatedNec)
      .mapN(KafkaStartingOffset.apply)
}

final case class AnalyticsStreamingSettings private (
    streamId: AnalyticsNonBlank,
    checkpointLocation: AnalyticsNonBlank,
    triggerInterval: FiniteDuration,
    maxOffsetsPerTrigger: MaximumOffsetsPerTrigger,
    maximumReplayRecords: MaximumStreamingReplayRecords,
    initialOffsets: Vector[KafkaStartingOffset]
) {
  def allowedFutureSkew: FiniteDuration = AnalyticsEventTimePolicy.AllowedFutureSkew
  def watermarkLag: FiniteDuration = AnalyticsEventTimePolicy.WatermarkLag
  def lateFactRetentionDays: Int = AnalyticsEventTimePolicy.LateFactRetentionDays

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
      maxOffsetsPerTrigger.asInstanceOf[Int].toString,
      maximumReplayRecords.toString,
      AnalyticsStreamingSettings.ConsumerGroupId,
      offsets
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
      AnalyticsDigest.sha256Hex(
        s"$sourceClusterId\n$topic\n$sourceTopicId".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      ),
      lakehouseId,
      AnalyticsDigest.sha256Hex(
        "hiring-operational-event-envelope:seven-field:v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      ),
      AnalyticsDigest.sha256Hex(settings.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    )
  }
}

private final case class StreamingSettingsInput(
    streamId: AnalyticsNonBlank,
    checkpointLocation: AnalyticsNonBlank,
    triggerInterval: FiniteDuration,
    maxOffsetsPerTrigger: Int,
    maximumReplayRecords: Int,
    initialOffsets: Vector[KafkaStartingOffsetInput]
)

private final case class KafkaStartingOffsetInput(partition: Int, offset: Long)

object AnalyticsStreamingSettings {
  import AnalyticsConfigReaders.given

  private val InvalidStreamingConfiguration =
    AnalyticsError.InvalidConfiguration("analytics.streaming configuration is invalid")

  val TriggerInterval: FiniteDuration = 60.seconds
  val ConsumerGroupId: String = "hiring-analytics-streaming-v1"
  val MaximumOffsetsPerTrigger: Int = 100000

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

  private def validateCheckpointLocation(value: AnalyticsNonBlank): ValidatedNec[String, AnalyticsNonBlank] = {
    val location = Either
      .catchNonFatal {
        val uri = new URI(value)
        val path = Paths.get(uri).normalize()
        val segments = path.iterator().asScala.map(_.toString).toVector
        val dockerRoot = Paths.get("/var/lib/hiring-analytics/checkpoints")
        val localRootPresent = segments.sliding(4).zipWithIndex.exists { case (parts, index) =>
          parts == Vector(".local", "data", "analytics", "checkpoints") && segments.size > index + 4
        }
        val permitted = (path.startsWith(dockerRoot) && path != dockerRoot) || localRootPresent
        uri.getScheme == "file" && uri.getAuthority == null && uri.getQuery == null && uri.getFragment == null &&
        path.isAbsolute && permitted && !Option(uri.getPath).toVector.flatMap(_.split("/")).contains("..")
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

  @nowarn("cat=deprecation")
  private object KebabCaseConfigReader
      extends ConfigReaderDerivation
      with CoproductConfigReaderDerivation(ConfigFieldMapping(PascalCase, KebabCase), "type")
      with ProductConfigReaderDerivation(ConfigFieldMapping(CamelCase, KebabCase)) {
    inline def derive[A](using Mirror.Of[A]): ConfigReader[A] = deriveConfigReader[A]
  }

  private given ConfigReader[KafkaStartingOffsetInput] = KebabCaseConfigReader.derive[KafkaStartingOffsetInput]
  private given ConfigReader[StreamingSettingsInput] = KebabCaseConfigReader.derive[StreamingSettingsInput]
  def load[F[_]: Async]: F[AnalyticsStreamingSettings] =
    Async[F].blocking(ConfigSource.default.at("analytics.streaming").load[StreamingSettingsInput]).flatMap {
      case Right(raw) => Async[F].fromEither(validate(raw))
      case Left(_)    => Async[F].raiseError(InvalidStreamingConfiguration)
    }

  def fromHocon(
      value: String,
      environment: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsStreamingSettings] =
    for {
      parsed <- Either
        .catchNonFatal(ConfigFactory.parseString(value, ConfigParseOptions.defaults().setAllowMissing(false)))
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics streaming HOCON is malformed"))
      resolved <- Either
        .catchNonFatal(
          parsed.withFallback(ConfigFactory.parseMap(environment.asJava)).resolve(ConfigResolveOptions.noSystem())
        )
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics streaming substitutions are invalid"))
      raw <- ConfigSource
        .fromConfig(resolved)
        .at("analytics.streaming")
        .load[StreamingSettingsInput]
        .left
        .map(_ => InvalidStreamingConfiguration)
      settings <- validate(raw)
    } yield settings

  private def validate(raw: StreamingSettingsInput): Either[AnalyticsError, AnalyticsStreamingSettings] = {
    val offsets = raw.initialOffsets.traverse(offset => KafkaStartingOffset.from(offset.partition, offset.offset))
    val uniquePartitions = Either
      .cond(
        raw.initialOffsets.map(_.partition).distinct.size == raw.initialOffsets.size,
        (),
        "analytics.streaming.initial-offsets must contain each partition once"
      )
      .toValidatedNec
    val nonEmptyOffsets = Either
      .cond(
        raw.initialOffsets.nonEmpty,
        (),
        "analytics.streaming.initial-offsets must explicitly name every source partition"
      )
      .toValidatedNec
    val settings = (
      raw.streamId.validNec[String],
      validateCheckpointLocation(raw.checkpointLocation),
      Either
        .cond(
          raw.triggerInterval == TriggerInterval,
          TriggerInterval,
          "analytics.streaming.trigger-interval must be 60 seconds"
        )
        .toValidatedNec,
      raw.maxOffsetsPerTrigger
        .refineEither[Interval.Closed[1, 100000]]
        .leftMap(_ => "max-offsets-per-trigger must be between 1 and 100000")
        .toValidatedNec,
      raw.maximumReplayRecords
        .refineEither[Interval.Closed[1, 1000]]
        .leftMap(_ => "maximum-replay-records must be between 1 and 1000")
        .toValidatedNec,
      (offsets, uniquePartitions, nonEmptyOffsets).mapN((validOffsets, _, _) => validOffsets)
    ).mapN(AnalyticsStreamingSettings.apply)
    settings.toEither.leftMap(errors =>
      AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; "))
    )
  }
}
