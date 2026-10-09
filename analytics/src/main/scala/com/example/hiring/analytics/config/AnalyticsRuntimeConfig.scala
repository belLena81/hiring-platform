package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{
  AnalyticsLateFactReplayRequest,
  AnalyticsReplayRequestId,
  AnalyticsRunManifest,
  AnalyticsTopic,
  KafkaRetentionEvidence,
  PartitionOffsetRange,
  RetentionEvidence,
  RetentionHorizon,
  RunId,
  SubjectPseudonymizer,
  WriterDisposition,
  WriterInventory,
  WriterRecord
}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.{Validated, ValidatedNec}
import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.ConnectionString
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import java.nio.file.Paths
import java.time.Instant

type AnalyticsNonBlank = String :| Not[Blank]

final case class AnalyticsCommonSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    sparkLocalDirectory: AnalyticsNonBlank,
    kafka: KafkaConnection,
    lakehouseRoot: AnalyticsNonBlank,
    pseudonymizer: SubjectPseudonymizer,
    operational: AnalyticsOperationalSettings
) {
  override def toString: String = "AnalyticsCommonSettings([REDACTED])"
}

/** HMAC key-ring inputs; validated once into [[SubjectPseudonymizer]] and never kept in settings. */
private[config] final case class AnalyticsHmacSettings(
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

private[analytics] final case class AnalyticsKeyRetirementAuditSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    lakehouseRoot: AnalyticsNonBlank,
    operational: AnalyticsOperationalSettings,
    retiringKeyId: AnalyticsNonBlank,
    retention: RetentionEvidence,
    writers: WriterInventory
) {
  override def toString: String = "AnalyticsKeyRetirementAuditSettings([REDACTED])"
}

/** Loads the runtime configuration once, validates it without effects, and only then starts Spark/Mongo resources. */
object AnalyticsRuntimeConfig {
  import AnalyticsConfigReaders.{complete, decode, given}

  // Optional evidence text defaults to the empty "not provided" value the audit treats as missing.
  private given ConfigReader[RetentionHorizon] =
    ConfigReader.forProduct2("retained-until", "evidence-reference")(
      (until: Option[Instant], reference: Option[String]) => RetentionHorizon(until, reference.getOrElse(""))
    )
  private given ConfigReader[WriterRecord] =
    ConfigReader.forProduct3("identity", "disposition", "evidence-reference")(
      (identity: Option[String], disposition: Option[WriterDisposition], reference: Option[String]) =>
        WriterRecord(identity.getOrElse(""), disposition.getOrElse(WriterDisposition.Unknown), reference.getOrElse(""))
    )
  private given ConfigReader[WriterInventory] =
    ConfigReader.forProduct4("observed-at", "coverage-reference", "managed", "unmanaged")(
      (
          observedAt: Option[Instant],
          coverage: Option[String],
          managed: Vector[WriterRecord],
          unmanaged: Vector[WriterRecord]
      ) => WriterInventory(observedAt, coverage.getOrElse(""), managed, unmanaged)
    )
  private given ConfigReader[KafkaRetentionEvidence] =
    ConfigReader.forProduct3("barrier-offset", "earliest-available-offset", "evidence-reference")(
      (barrier: Option[Long], earliest: Option[Long], reference: Option[String]) =>
        KafkaRetentionEvidence(barrier, earliest, reference.getOrElse(""))
    )

  private final case class MongoInput(uri: AnalyticsNonBlank, database: AnalyticsNonBlank) derives ConfigReader
  private final case class SparkInput(master: AnalyticsNonBlank, localDirectory: AnalyticsNonBlank) derives ConfigReader
  private final case class LakehouseInput(root: AnalyticsNonBlank) derives ConfigReader
  private final case class KafkaInput(
      bootstrapServers: AnalyticsNonBlank,
      username: AnalyticsNonBlank,
      password: AnalyticsNonBlank,
      topic: AnalyticsTopic,
      securityProtocol: Option[KafkaSecurityProtocol],
      allowPlaintext: Option[Boolean]
  ) derives ConfigReader
  private final case class CommonInput(
      mongo: MongoInput,
      spark: SparkInput,
      kafka: KafkaInput,
      lakehouse: LakehouseInput,
      hmac: AnalyticsHmacSettings,
      operational: AnalyticsOperationalSettings
  ) derives ConfigReader

  // Command sections are decoded only by their own command, so each field is required where the section is read.
  private final case class FencerInput(username: AnalyticsNonBlank, password: AnalyticsNonBlank) derives ConfigReader
  private final case class BatchInput(runId: RunId, partition: Int, startOffset: Long, endOffsetExclusive: Long)
      derives ConfigReader
  private final case class ReplayCoordinateInput(topic: String, partition: Int, offset: Long) derives ConfigReader
  private final case class ReplayInput(
      requestId: AnalyticsReplayRequestId,
      maximumRecords: Option[Int],
      coordinates: List[ReplayCoordinateInput]
  ) derives ConfigReader
  private final case class KeyRetirementAuditInput(
      retiringKeyId: AnalyticsNonBlank,
      kafka: KafkaRetentionEvidence,
      deltaData: RetentionHorizon,
      deltaLogs: RetentionHorizon,
      reports: RetentionHorizon,
      writers: WriterInventory
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
    complete(
      (decodeCommon(source), decode[BatchInput](source, "analytics.batch")).tupled.andThen { case (raw, input) =>
        val ranges = PartitionOffsetRange
          .fromTopic(raw.kafka.topic, input.partition, input.startOffset, input.endOffsetExclusive)
          .map(Vector(_))
        (common(raw), AnalyticsRunManifest.from(input.runId, ranges))
          .mapN(AnalyticsBatchSettings.apply)
      }
    )

  private[analytics] def readWorker(source: ConfigSource): Either[AnalyticsError, AnalyticsWorkerSettings] =
    complete(
      (
        decodeCommon(source).andThen(raw => common(raw).tupleRight(raw.kafka.topic)),
        decode[FencerInput](source, "analytics.kafka.fencer")
      ).tupled.andThen { case ((settings, topic), fencer) =>
        KafkaConnection
          .validate(settings.kafka.copy(saslUsername = Some(fencer.username), saslPassword = Some(fencer.password)))
          .map(AnalyticsWorkerSettings(settings, topic, _))
      }
    )

  private[analytics] def readStreaming(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsStreamingRuntimeSettings] =
    complete(
      (
        decodeCommon(source).andThen(raw => common(raw).tupleRight(raw.kafka.topic)),
        AnalyticsStreamingSettings.read(source)
      )
        .mapN { case ((settings, topic), streaming) => AnalyticsStreamingRuntimeSettings(settings, streaming, topic) }
    )

  private[analytics] def readLateFactReplay(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsLateFactReplaySettings] =
    complete(
      (decodeCommon(source), decode[ReplayInput](source, "analytics.replay")).tupled.andThen { case (raw, input) =>
        val request = AnalyticsLateFactReplayRequest
          .from(
            input.requestId,
            input.coordinates.map(row => (row.topic, row.partition, row.offset)).toVector,
            input.maximumRecords.getOrElse(AnalyticsLateFactReplayRequest.MaximumCoordinates)
          )
        val topicBound = Validated.condNec(
          input.coordinates.forall(_.topic == AnalyticsTopic.unwrap(raw.kafka.topic)),
          (),
          "analytics.replay coordinates must use analytics.kafka.topic"
        )
        (common(raw), request, topicBound).mapN((settings, validRequest, _) =>
          AnalyticsLateFactReplaySettings(settings, validRequest)
        )
      }
    )

  private[analytics] def readKeyRetirementAudit(
      source: ConfigSource
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] =
    complete(
      (
        decodeCommon(source).andThen(raw => mongoUri(raw.mongo.uri).as(raw)),
        decode[KeyRetirementAuditInput](source, "analytics.key-retirement-audit")
      ).mapN { (raw, audit) =>
        AnalyticsKeyRetirementAuditSettings(
          raw.mongo.uri,
          raw.mongo.database,
          raw.spark.master,
          raw.lakehouse.root,
          raw.operational,
          audit.retiringKeyId,
          RetentionEvidence(audit.kafka, audit.deltaData, audit.deltaLogs, audit.reports),
          audit.writers
        )
      }
    )

  private def decodeCommon(source: ConfigSource): ValidatedNec[String, CommonInput] =
    decode[CommonInput](source, "analytics")

  /** Cross-field rules that no single reader can express: Mongo URI syntax, Kafka transport, directory ownership and
    * the HMAC key ring. Every bound on an individual field is already enforced by its reader.
    */
  private def common(raw: CommonInput): ValidatedNec[String, AnalyticsCommonSettings] = {
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
      SubjectPseudonymizer.validateFromBase64(
        raw.hmac.secretBase64,
        raw.hmac.keyId,
        raw.hmac.previousKeyId,
        raw.hmac.previousSecretBase64
      )
    ).mapN((uri, localDirectory, connection, pseudonymizer) =>
      AnalyticsCommonSettings(
        uri,
        raw.mongo.database,
        raw.spark.master,
        localDirectory,
        connection,
        raw.lakehouse.root,
        pseudonymizer,
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
}
