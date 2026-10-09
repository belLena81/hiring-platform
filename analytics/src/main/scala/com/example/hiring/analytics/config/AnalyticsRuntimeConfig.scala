package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{
  AnalyticsLateFactReplayRequest,
  AnalyticsRunManifest,
  AnalyticsTopic,
  PartitionOffsetRange,
  RunId,
  SubjectPseudonymizer
}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.ValidatedNec
import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.ConnectionString
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import _root_.pureconfig.error.UserValidationFailed
import java.time.Instant
import java.nio.file.Paths

type AnalyticsNonBlank = String :| Not[Blank]

final case class AnalyticsCommonSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    sparkLocalDirectory: AnalyticsNonBlank,
    kafka: KafkaConnection,
    lakehouseRoot: AnalyticsNonBlank,
    hmac: AnalyticsHmacSettings,
    operational: AnalyticsOperationalSettings
) {
  override def toString: String = "AnalyticsCommonSettings([REDACTED])"
}

final case class AnalyticsHmacSettings(
    secretBase64: AnalyticsNonBlank,
    keyId: AnalyticsNonBlank,
    previousKeyId: Option[AnalyticsNonBlank],
    previousSecretBase64: Option[AnalyticsNonBlank]
) {
  override def toString: String = "AnalyticsHmacSettings([REDACTED])"
}

final case class AnalyticsBatchSettings(common: AnalyticsCommonSettings, manifest: AnalyticsRunManifest) {
  override def toString: String = "AnalyticsBatchSettings([REDACTED])"
}

final case class AnalyticsStreamingRuntimeSettings(
    common: AnalyticsCommonSettings,
    streaming: AnalyticsStreamingSettings,
    topic: AnalyticsTopic
) {
  override def toString: String = "AnalyticsStreamingRuntimeSettings([REDACTED])"
}

final case class AnalyticsLateFactReplaySettings(
    common: AnalyticsCommonSettings,
    request: AnalyticsLateFactReplayRequest
) {
  override def toString: String = "AnalyticsLateFactReplaySettings([REDACTED])"
}

final case class AnalyticsWorkerSettings(
    common: AnalyticsCommonSettings,
    topic: AnalyticsTopic,
    fencerKafka: KafkaConnection
) {
  override def toString: String = "AnalyticsWorkerSettings([REDACTED])"
}

private[analytics] enum AnalyticsAuditWriterDisposition {
  case Stopped, AccessRevoked, Active, Unknown
}

private[analytics] object AnalyticsAuditWriterDisposition {
  given ConfigReader[AnalyticsAuditWriterDisposition] = ConfigReader[String].emap {
    case "stopped"        => Right(Stopped)
    case "access-revoked" => Right(AccessRevoked)
    case "active"         => Right(Active)
    case "unknown"        => Right(Unknown)
    case _                => Left(UserValidationFailed("must be stopped, access-revoked, active, or unknown"))
  }
}

private[analytics] final case class AnalyticsAuditWriter(
    identity: Option[AnalyticsNonBlank],
    disposition: Option[AnalyticsAuditWriterDisposition],
    evidenceReference: Option[AnalyticsNonBlank]
)

private[analytics] final case class AnalyticsAuditHorizon(
    retainedUntil: Option[Instant],
    evidenceReference: Option[AnalyticsNonBlank]
)

private[analytics] final case class AnalyticsAuditWriterInventory(
    observedAt: Option[Instant],
    coverageReference: Option[AnalyticsNonBlank],
    managed: Vector[AnalyticsAuditWriter],
    unmanaged: Vector[AnalyticsAuditWriter]
)

private[analytics] final case class AnalyticsKeyRetirementAuditSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    lakehouseRoot: AnalyticsNonBlank,
    operational: AnalyticsOperationalSettings,
    retiringKeyId: AnalyticsNonBlank,
    kafkaBarrierOffset: Option[Long],
    kafkaEarliestAvailableOffset: Option[Long],
    kafkaEvidenceReference: Option[AnalyticsNonBlank],
    deltaData: AnalyticsAuditHorizon,
    deltaLogs: AnalyticsAuditHorizon,
    reports: AnalyticsAuditHorizon,
    writers: AnalyticsAuditWriterInventory
) {
  override def toString: String = "AnalyticsKeyRetirementAuditSettings([REDACTED])"
}

/** Loads the runtime configuration once, validates it without effects, and only then starts Spark/Mongo resources. */
object AnalyticsRuntimeConfig {
  import AnalyticsConfigReaders.{complete, decode, given}

  private given ConfigReader[AnalyticsAuditHorizon] = ConfigReader.derived
  private given ConfigReader[AnalyticsAuditWriter] = ConfigReader.derived
  private given ConfigReader[AnalyticsAuditWriterInventory] = ConfigReader.derived

  private final case class MongoInput(uri: AnalyticsNonBlank, database: AnalyticsNonBlank) derives ConfigReader
  private final case class SparkInput(master: AnalyticsNonBlank, localDirectory: AnalyticsNonBlank) derives ConfigReader
  private final case class LakehouseInput(root: AnalyticsNonBlank) derives ConfigReader
  private final case class KafkaFencerInput(username: Option[AnalyticsNonBlank], password: Option[AnalyticsNonBlank])
      derives ConfigReader
  private final case class KafkaInput(
      bootstrapServers: AnalyticsNonBlank,
      username: AnalyticsNonBlank,
      password: AnalyticsNonBlank,
      topic: AnalyticsTopic,
      fencer: KafkaFencerInput,
      securityProtocol: Option[KafkaSecurityProtocol],
      allowPlaintext: Option[Boolean]
  ) derives ConfigReader
  private final case class BatchInput(
      runId: Option[AnalyticsNonBlank],
      partition: Option[Int],
      startOffset: Option[Long],
      endOffsetExclusive: Option[Long]
  ) derives ConfigReader
  private final case class ReplayCoordinateInput(topic: String, partition: Int, offset: Long) derives ConfigReader
  private final case class ReplayInput(
      requestId: Option[String],
      maximumRecords: Option[Int],
      coordinates: Option[List[ReplayCoordinateInput]]
  ) derives ConfigReader
  private final case class KafkaRetentionEvidence(
      barrierOffset: Option[Long],
      earliestAvailableOffset: Option[Long],
      evidenceReference: Option[AnalyticsNonBlank]
  ) derives ConfigReader
  private final case class KeyRetirementAuditInput(
      retiringKeyId: Option[AnalyticsNonBlank],
      kafka: KafkaRetentionEvidence,
      deltaData: AnalyticsAuditHorizon,
      deltaLogs: AnalyticsAuditHorizon,
      reports: AnalyticsAuditHorizon,
      writers: AnalyticsAuditWriterInventory
  ) derives ConfigReader
  private final case class AnalyticsConfigValues(
      mongo: MongoInput,
      spark: SparkInput,
      kafka: KafkaInput,
      lakehouse: LakehouseInput,
      hmac: AnalyticsHmacSettings,
      batch: Option[BatchInput],
      operational: AnalyticsOperationalSettings,
      keyRetirementAudit: Option[KeyRetirementAuditInput],
      replay: Option[ReplayInput]
  ) derives ConfigReader

  def loadBatch[F[_]: Async]: F[AnalyticsBatchSettings] =
    load(readBatch).flatTap(settings => KafkaConnection.preflight(settings.common.kafka))

  def loadWorker[F[_]: Async]: F[AnalyticsWorkerSettings] =
    load(readWorker).flatTap(settings =>
      KafkaConnection.preflight(settings.common.kafka) *> KafkaConnection.preflight(settings.fencerKafka)
    )

  def loadStreaming[F[_]: Async]: F[AnalyticsStreamingRuntimeSettings] =
    load(readStreaming).flatTap(settings => KafkaConnection.preflight(settings.common.kafka))

  def loadLateFactReplay[F[_]: Async]: F[AnalyticsLateFactReplaySettings] =
    load(readLateFactReplay).flatTap(settings => KafkaConnection.preflight(settings.common.kafka))

  def loadKeyRetirementAudit[F[_]: Async]: F[AnalyticsKeyRetirementAuditSettings] =
    load(readKeyRetirementAudit)

  /** The packaged `application.conf` resolved by Typesafe Config, including its `${?NAME}` substitutions. */
  private def load[F[_]: Async, A](read: ConfigSource => Either[AnalyticsError, A]): F[A] =
    Async[F].blocking(read(ConfigSource.default)).flatMap(Async[F].fromEither)

  private[analytics] def readBatch(source: ConfigSource): Either[AnalyticsError, AnalyticsBatchSettings] =
    complete(values(source).andThen(batch))

  private[analytics] def readWorker(source: ConfigSource): Either[AnalyticsError, AnalyticsWorkerSettings] =
    complete(values(source).andThen(worker))

  private[analytics] def readStreaming(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsStreamingRuntimeSettings] =
    complete(
      (values(source).andThen(raw => common(raw).tupleRight(raw.kafka.topic)), AnalyticsStreamingSettings.read(source))
        .mapN { case ((settings, topic), streaming) => AnalyticsStreamingRuntimeSettings(settings, streaming, topic) }
    )

  private[analytics] def readLateFactReplay(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsLateFactReplaySettings] =
    complete(values(source).andThen(lateFactReplay))

  private[analytics] def readKeyRetirementAudit(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] =
    complete(values(source).andThen(keyRetirementAudit))

  private def values(source: ConfigSource): ValidatedNec[String, AnalyticsConfigValues] =
    decode[AnalyticsConfigValues](source, "analytics")

  private def batch(raw: AnalyticsConfigValues): ValidatedNec[String, AnalyticsBatchSettings] = {
    val manifest = raw.batch.toValidNec("analytics.batch configuration is required").andThen { input =>
      val inputs = (
        required(input.runId, "analytics.batch.run-id"),
        input.partition.toValidNec("analytics.batch.partition is required"),
        input.startOffset.toValidNec("analytics.batch.start-offset is required"),
        input.endOffsetExclusive.toValidNec("analytics.batch.end-offset-exclusive is required")
      ).tupled
      inputs.andThen { case (runIdValue, partition, startOffset, endOffsetExclusive) =>
        val runId = RunId.from(runIdValue).leftMap(_ => "analytics.batch.run-id must be non-empty").toValidatedNec
        val ranges = PartitionOffsetRange
          .fromTopic(raw.kafka.topic, partition, startOffset, endOffsetExclusive)
          .map(Vector(_))
        AnalyticsRunManifest.fromValidated(runId, ranges)
      }
    }
    (common(raw), manifest).mapN(AnalyticsBatchSettings.apply)
  }

  private def worker(raw: AnalyticsConfigValues): ValidatedNec[String, AnalyticsWorkerSettings] =
    (
      common(raw),
      required(raw.kafka.fencer.username, "analytics.kafka.fencer.username"),
      required(raw.kafka.fencer.password, "analytics.kafka.fencer.password")
    ).tupled.andThen { case (settings, username, password) =>
      KafkaConnection
        .validate(
          KafkaConnection(
            settings.kafka.bootstrapServers,
            Some(username),
            Some(password),
            settings.kafka.securityProtocol,
            settings.kafka.allowPlaintext
          )
        )
        .map(AnalyticsWorkerSettings(settings, raw.kafka.topic, _))
    }

  private def lateFactReplay(raw: AnalyticsConfigValues): ValidatedNec[String, AnalyticsLateFactReplaySettings] = {
    val validatedRequest = raw.replay.toValidNec("analytics.replay configuration is required").andThen { value =>
      val requestId = value.requestId.toValidNec("analytics.replay.request-id is required")
      val coordinates = value.coordinates.toValidNec("analytics.replay.coordinates is required")
      val builtRequest = (requestId, coordinates).tupled.andThen { case (id, selected) =>
        val maximum = value.maximumRecords.getOrElse(AnalyticsLateFactReplayRequest.MaximumCoordinates)
        AnalyticsLateFactReplayRequest
          .from(id, selected.map(row => (row.topic, row.partition, row.offset)).toVector, maximum)
          .leftMap(_.toNonEmptyList.toList.mkString("; "))
          .toValidatedNec
      }
      val configuredTopic = AnalyticsTopic.unwrap(raw.kafka.topic)
      val topicBound = coordinates.andThen { selected =>
        Either
          .cond(
            selected.forall(_.topic == configuredTopic),
            (),
            "analytics.replay coordinates must use analytics.kafka.topic"
          )
          .toValidatedNec
      }
      (builtRequest, topicBound).mapN((request, _) => request)
    }
    (common(raw), validatedRequest).mapN(AnalyticsLateFactReplaySettings.apply)
  }

  private def keyRetirementAudit(
      raw: AnalyticsConfigValues
  ): ValidatedNec[String, AnalyticsKeyRetirementAuditSettings] =
    raw.keyRetirementAudit.toValidNec("analytics.key-retirement-audit configuration is required").andThen { audit =>
      (
        mongoUri(raw.mongo.uri),
        required(audit.retiringKeyId, "analytics.key-retirement-audit.retiring-key-id")
      ).mapN { (uri, retiringKeyId) =>
        AnalyticsKeyRetirementAuditSettings(
          uri,
          raw.mongo.database,
          raw.spark.master,
          raw.lakehouse.root,
          raw.operational,
          retiringKeyId,
          audit.kafka.barrierOffset,
          audit.kafka.earliestAvailableOffset,
          audit.kafka.evidenceReference,
          audit.deltaData,
          audit.deltaLogs,
          audit.reports,
          audit.writers
        )
      }
    }

  /** Cross-field rules that no single reader can express: Mongo URI syntax, Kafka transport, directory ownership and
    * the HMAC key ring. Every bound on an individual field is already enforced by its reader.
    */
  private def common(raw: AnalyticsConfigValues): ValidatedNec[String, AnalyticsCommonSettings] = {
    val kafka = KafkaConnection.validate(
      KafkaConnection(
        raw.kafka.bootstrapServers,
        Some(raw.kafka.username),
        Some(raw.kafka.password),
        raw.kafka.securityProtocol.getOrElse(KafkaSecurityProtocol.SaslSsl),
        raw.kafka.allowPlaintext.getOrElse(false)
      )
    )
    (
      mongoUri(raw.mongo.uri),
      validateSparkLocalDirectory(raw.spark.localDirectory),
      kafka,
      validateHmac(raw.hmac)
    ).mapN((uri, localDirectory, connection, hmac) =>
      AnalyticsCommonSettings(
        uri,
        raw.mongo.database,
        raw.spark.master,
        localDirectory,
        connection,
        raw.lakehouse.root,
        hmac,
        raw.operational
      )
    )
  }

  private def mongoUri(uri: AnalyticsNonBlank): ValidatedNec[String, AnalyticsNonBlank] =
    Either
      .catchNonFatal(new ConnectionString(uri))
      .leftMap(_ => "analytics.mongo.uri is invalid")
      .toValidatedNec
      .as(uri)

  private def validateSparkLocalDirectory(value: AnalyticsNonBlank): ValidatedNec[String, AnalyticsNonBlank] =
    Either
      .catchNonFatal(Paths.get(value).normalize())
      .leftMap(_ => "analytics.spark.local-directory must be an owned absolute directory")
      .flatMap { path =>
        Either.cond(
          AnalyticsRuntimeDirectory.owns(path, AnalyticsRuntimeDirectory.SparkTemp, allowCategoryRoot = true) &&
            !value.contains(".."),
          value,
          "analytics.spark.local-directory must be inside the owned analytics runtime directory"
        )
      }
      .toValidatedNec

  private def required(value: Option[AnalyticsNonBlank], field: String): ValidatedNec[String, AnalyticsNonBlank] =
    value.toValidNec(s"$field is required")

  private def validateHmac(raw: AnalyticsHmacSettings): ValidatedNec[String, AnalyticsHmacSettings] =
    SubjectPseudonymizer
      .validateFromBase64(Some(raw.secretBase64), raw.keyId, raw.previousKeyId, raw.previousSecretBase64)
      .as(raw)
}
