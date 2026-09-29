package com.example.hiring.analytics.config

import AnalyticsPositiveInt.*

import com.example.hiring.analytics.domain.{AnalyticsRunManifest, PartitionOffsetRange, RunId, SubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError

import cats.data.ValidatedNec
import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.ConnectionString
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.numeric.Positive
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import pureconfig.error.UserValidationFailed
import pureconfig.generic.derivation.{
  ConfigReaderDerivation,
  CoproductConfigReaderDerivation,
  ProductConfigReaderDerivation
}
import scala.annotation.nowarn
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*
import java.time.Instant

type AnalyticsNonBlank = String :| Not[Blank]

final case class AnalyticsCommonSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    kafka: KafkaConnection,
    lakehouseRoot: AnalyticsNonBlank,
    pseudonymizer: SubjectPseudonymizer,
    operational: AnalyticsOperationalSettings
) {
  override def toString: String = "AnalyticsCommonSettings([REDACTED])"
}

final case class AnalyticsBatchSettings(common: AnalyticsCommonSettings, manifest: AnalyticsRunManifest) {
  override def toString: String = "AnalyticsBatchSettings([REDACTED])"
}

final case class AnalyticsWorkerSettings(
    common: AnalyticsCommonSettings,
    topic: AnalyticsNonBlank,
    fencerKafka: KafkaConnection
) {
  override def toString: String = "AnalyticsWorkerSettings([REDACTED])"
}

private[analytics] enum AnalyticsAuditWriterDisposition {
  case Stopped, AccessRevoked, Active, Unknown
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
  import AnalyticsRawConfig.given
  import AnalyticsRawConfig.{Lakehouse as RawLakehouse, Mongo as RawMongo, Spark as RawSpark}

  private final case class RawFencer(username: Option[AnalyticsNonBlank], password: Option[AnalyticsNonBlank])
  private final case class RawKafka(
      bootstrapServers: Option[AnalyticsNonBlank],
      username: Option[AnalyticsNonBlank],
      password: Option[AnalyticsNonBlank],
      topic: Option[AnalyticsNonBlank],
      fencer: RawFencer,
      securityProtocol: Option[KafkaSecurityProtocol],
      allowPlaintext: Option[Boolean]
  )
  private final case class RawHmac(
      secretBase64: Option[AnalyticsNonBlank],
      keyId: Option[AnalyticsNonBlank],
      previousKeyId: Option[AnalyticsNonBlank],
      previousSecretBase64: Option[AnalyticsNonBlank]
  )
  private final case class RawBatch(
      runId: Option[AnalyticsNonBlank],
      partition: Option[Int],
      startOffset: Option[Long],
      endOffsetExclusive: Option[Long]
  )
  private final case class RawRetention(
      bronzeDays: Int,
      quarantineDays: Int,
      silverDays: Int,
      publishedSnapshotDays: Int,
      deletionMarkerDays: Int,
      deltaVacuumSafety: FiniteDuration,
      deltaLogRetention: FiniteDuration
  )
  private final case class RawOperational(
      retention: RawRetention,
      reportReservationTtlDays: Int,
      mongoTransactionWindowSeconds: Int,
      maximumErasureEvidenceFiles: Int,
      mongoPublisherBufferSize: Int
  )
  private final case class RawAuditKafka(
      barrierOffset: Option[Long],
      earliestAvailableOffset: Option[Long],
      evidenceReference: Option[AnalyticsNonBlank]
  )
  private final case class RawAuditHorizon(
      retainedUntil: Option[Instant],
      evidenceReference: Option[AnalyticsNonBlank]
  )
  private final case class RawAuditWriter(
      identity: Option[AnalyticsNonBlank],
      disposition: Option[AnalyticsAuditWriterDisposition],
      evidenceReference: Option[AnalyticsNonBlank]
  )
  private final case class RawAuditWriters(
      observedAt: Option[Instant],
      coverageReference: Option[AnalyticsNonBlank],
      managed: Vector[RawAuditWriter],
      unmanaged: Vector[RawAuditWriter]
  )
  private final case class RawKeyRetirementAudit(
      retiringKeyId: Option[AnalyticsNonBlank],
      kafka: RawAuditKafka,
      deltaData: RawAuditHorizon,
      deltaLogs: RawAuditHorizon,
      reports: RawAuditHorizon,
      writers: RawAuditWriters
  )
  private final case class RawAnalytics(
      mongo: RawMongo,
      spark: RawSpark,
      kafka: RawKafka,
      lakehouse: RawLakehouse,
      hmac: RawHmac,
      batch: RawBatch,
      operational: RawOperational,
      keyRetirementAudit: Option[RawKeyRetirementAudit]
  )

  @nowarn("cat=deprecation")
  private object KebabCaseConfigReader
      extends ConfigReaderDerivation
      with CoproductConfigReaderDerivation(ConfigFieldMapping(PascalCase, KebabCase), "type")
      with ProductConfigReaderDerivation(
        ConfigFieldMapping(CamelCase, KebabCase)
          .withOverrides("secretBase64" -> "secret-base64", "previousSecretBase64" -> "previous-secret-base64")
      ) {
    import AnalyticsRawConfig.given

    inline def derive[A](using Mirror.Of[A]): ConfigReader[A] = deriveConfigReader[A]
  }

  private given ConfigReader[RawFencer] = KebabCaseConfigReader.derive[RawFencer]
  private given ConfigReader[RawKafka] = KebabCaseConfigReader.derive[RawKafka]
  private given ConfigReader[RawHmac] = KebabCaseConfigReader.derive[RawHmac]
  private given ConfigReader[RawBatch] = KebabCaseConfigReader.derive[RawBatch]
  private given ConfigReader[RawRetention] = KebabCaseConfigReader.derive[RawRetention]
  private given ConfigReader[RawOperational] = KebabCaseConfigReader.derive[RawOperational]
  private given ConfigReader[Instant] = ConfigReader[String].emap { value =>
    Either
      .catchNonFatal(Instant.parse(value))
      .leftMap(_ => UserValidationFailed("must be an ISO-8601 UTC timestamp"))
  }
  private given ConfigReader[AnalyticsAuditWriterDisposition] = ConfigReader[String].emap {
    case "stopped"        => Right(AnalyticsAuditWriterDisposition.Stopped)
    case "access-revoked" => Right(AnalyticsAuditWriterDisposition.AccessRevoked)
    case "active"         => Right(AnalyticsAuditWriterDisposition.Active)
    case "unknown"        => Right(AnalyticsAuditWriterDisposition.Unknown)
    case _                => Left(UserValidationFailed("must be stopped, access-revoked, active, or unknown"))
  }
  private given ConfigReader[RawAuditKafka] = KebabCaseConfigReader.derive[RawAuditKafka]
  private given ConfigReader[RawAuditHorizon] = KebabCaseConfigReader.derive[RawAuditHorizon]
  private given ConfigReader[RawAuditWriter] = KebabCaseConfigReader.derive[RawAuditWriter]
  private given ConfigReader[RawAuditWriters] = KebabCaseConfigReader.derive[RawAuditWriters]
  private given ConfigReader[RawKeyRetirementAudit] = KebabCaseConfigReader.derive[RawKeyRetirementAudit]
  private given ConfigReader[RawAnalytics] = KebabCaseConfigReader.derive[RawAnalytics]

  def loadBatch[F[_]: Async]: F[AnalyticsBatchSettings] =
    load[F].flatMap(raw => Async[F].fromEither(batch(raw)))

  def loadWorker[F[_]: Async]: F[AnalyticsWorkerSettings] =
    load[F].flatMap(raw => Async[F].fromEither(worker(raw)))

  def loadKeyRetirementAudit[F[_]: Async]: F[AnalyticsKeyRetirementAuditSettings] =
    load[F].flatMap(raw => Async[F].fromEither(keyRetirementAudit(raw)))

  def loadOperational[F[_]: Async]: F[AnalyticsOperationalSettings] =
    Async[F].blocking(ConfigSource.default.at("analytics.operational").load[RawOperational]).flatMap {
      case Right(raw) => Async[F].fromEither(complete(operational(raw)))
      case Left(_)    =>
        Async[F].raiseError(
          AnalyticsError.InvalidConfiguration("analytics operational configuration is missing or malformed")
        )
    }

  def batchFromHocon(
      value: String,
      environment: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsBatchSettings] =
    resolve(value, environment).flatMap(batch)

  def workerFromHocon(
      value: String,
      environment: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsWorkerSettings] =
    resolve(value, environment).flatMap(worker)

  private[analytics] def keyRetirementAuditFromHocon(
      value: String,
      environment: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] =
    resolve(value, environment).flatMap(keyRetirementAudit)

  def operationalFromHocon(
      value: String,
      environment: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsOperationalSettings] =
    for {
      config <- Either
        .catchNonFatal(ConfigFactory.parseString(value, ConfigParseOptions.defaults().setAllowMissing(false)))
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics HOCON configuration is malformed"))
      resolved <- Either
        .catchNonFatal(
          config.withFallback(ConfigFactory.parseMap(environment.asJava)).resolve(ConfigResolveOptions.noSystem())
        )
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics configuration substitutions are invalid"))
      raw <- ConfigSource
        .fromConfig(resolved)
        .at("analytics.operational")
        .load[RawOperational]
        .left
        .map(_ => AnalyticsError.InvalidConfiguration("analytics operational configuration is missing or malformed"))
      settings <- complete(operational(raw))
    } yield settings

  private def load[F[_]: Async]: F[RawAnalytics] =
    Async[F].blocking(ConfigSource.default.at("analytics").load[RawAnalytics]).flatMap {
      case Right(raw) => Async[F].pure(raw)
      case Left(_)    =>
        Async[F].raiseError(AnalyticsError.InvalidConfiguration("analytics configuration is missing or malformed"))
    }

  private def resolve(value: String, environment: Map[String, String]): Either[AnalyticsError, RawAnalytics] =
    for {
      parsed <- Either
        .catchNonFatal(ConfigFactory.parseString(value, ConfigParseOptions.defaults().setAllowMissing(false)))
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics HOCON configuration is malformed"))
      resolved <- Either
        .catchNonFatal(
          parsed.withFallback(ConfigFactory.parseMap(environment.asJava)).resolve(ConfigResolveOptions.noSystem())
        )
        .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics configuration substitutions are invalid"))
      raw <- rawFrom(resolved)
    } yield raw

  private def rawFrom(config: Config): Either[AnalyticsError, RawAnalytics] =
    ConfigSource
      .fromConfig(config)
      .at("analytics")
      .load[RawAnalytics]
      .left
      .map(_ => AnalyticsError.InvalidConfiguration("analytics configuration is missing or malformed"))

  private def batch(raw: RawAnalytics): Either[AnalyticsError, AnalyticsBatchSettings] =
    val runId = RunId
      .from(raw.batch.runId.getOrElse(""))
      .leftMap(_ => "analytics.batch.run-id must be non-empty")
      .toValidatedNec
    val ranges = (
      present(raw.kafka.topic, "analytics.kafka.topic"),
      integer(raw.batch.partition, "analytics.batch.partition"),
      long(raw.batch.startOffset, "analytics.batch.start-offset"),
      long(raw.batch.endOffsetExclusive, "analytics.batch.end-offset-exclusive")
    )
      .mapN((topic, partition, startOffset, endOffset) => (topic, partition, startOffset, endOffset))
      .andThen { case (topic, partition, startOffset, endOffset) =>
        PartitionOffsetRange.from(topic, partition, startOffset, endOffset).map(Vector(_))
      }
    val manifest = AnalyticsRunManifest.fromValidated(runId, ranges)
    complete((common(raw), manifest).mapN(AnalyticsBatchSettings.apply))

  private def worker(raw: RawAnalytics): Either[AnalyticsError, AnalyticsWorkerSettings] =
    complete(
      (
        common(raw),
        required(raw.kafka.topic, "analytics.kafka.topic"),
        required(raw.kafka.fencer.username, "analytics.kafka.fencer.username"),
        required(raw.kafka.fencer.password, "analytics.kafka.fencer.password")
      ).mapN((settings, topic, username, password) => (settings, topic, username, password))
        .andThen { case (settings, topic, username, password) =>
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
            .map(AnalyticsWorkerSettings(settings, topic, _))
        }
    )

  private def keyRetirementAudit(
      raw: RawAnalytics
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] = {
    val audit = raw.keyRetirementAudit.toRight(
      AnalyticsError.InvalidConfiguration("analytics key-retirement-audit configuration is missing or malformed")
    )
    for {
      settings <- audit
      mongoUri <- required(raw.mongo.uri, "analytics.mongo.uri")
        .andThen { uri =>
          Either
            .catchNonFatal(new ConnectionString(uri))
            .leftMap(_ => "analytics.mongo.uri is invalid")
            .toValidatedNec
            .as(uri)
        }
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
      mongoDatabase <- required(raw.mongo.database, "analytics.mongo.database").toEither.leftMap(errors =>
        AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; "))
      )
      sparkMaster <- required(raw.spark.master, "analytics.spark.master").toEither.leftMap(errors =>
        AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; "))
      )
      root <- required(raw.lakehouse.root, "analytics.lakehouse.root").toEither.leftMap(errors =>
        AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; "))
      )
      retiringKeyId <- required(settings.retiringKeyId, "analytics.key-retirement-audit.retiring-key-id").toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
      operationalSettings <- complete(operational(raw.operational))
    } yield AnalyticsKeyRetirementAuditSettings(
      mongoUri,
      mongoDatabase,
      sparkMaster,
      root,
      operationalSettings,
      retiringKeyId,
      settings.kafka.barrierOffset,
      settings.kafka.earliestAvailableOffset,
      settings.kafka.evidenceReference,
      AnalyticsAuditHorizon(settings.deltaData.retainedUntil, settings.deltaData.evidenceReference),
      AnalyticsAuditHorizon(settings.deltaLogs.retainedUntil, settings.deltaLogs.evidenceReference),
      AnalyticsAuditHorizon(settings.reports.retainedUntil, settings.reports.evidenceReference),
      AnalyticsAuditWriterInventory(
        settings.writers.observedAt,
        settings.writers.coverageReference,
        settings.writers.managed.map(auditWriter),
        settings.writers.unmanaged.map(auditWriter)
      )
    )
  }

  private def auditWriter(raw: RawAuditWriter): AnalyticsAuditWriter =
    AnalyticsAuditWriter(raw.identity, raw.disposition, raw.evidenceReference)

  private def common(raw: RawAnalytics): ValidatedNec[String, AnalyticsCommonSettings] = {
    val mongoUri = required(raw.mongo.uri, "analytics.mongo.uri").andThen { uri =>
      Either
        .catchNonFatal(new ConnectionString(uri))
        .leftMap(_ => "analytics.mongo.uri is invalid")
        .toValidatedNec
        .as(uri)
    }
    val kafka = (
      required(raw.kafka.bootstrapServers, "analytics.kafka.bootstrap-servers"),
      required(raw.kafka.username, "analytics.kafka.username"),
      required(raw.kafka.password, "analytics.kafka.password"),
      kafkaSecurity(raw)
    ).mapN((brokers, username, password, security) =>
      KafkaConnection(
        brokers,
        Some(username),
        Some(password),
        security._1,
        security._2
      )
    ).andThen(KafkaConnection.validate)
    val lakehouseRoot = required(raw.lakehouse.root, "analytics.lakehouse.root")
    val pseudonymizer = SubjectPseudonymizer.validateFromBase64(
      raw.hmac.secretBase64,
      raw.hmac.keyId.getOrElse("hmac-v1"),
      raw.hmac.previousKeyId,
      raw.hmac.previousSecretBase64
    )
    val operationalSettings = operational(raw.operational)

    (
      mongoUri,
      required(raw.mongo.database, "analytics.mongo.database"),
      required(raw.spark.master, "analytics.spark.master"),
      kafka,
      lakehouseRoot,
      pseudonymizer,
      operationalSettings
    ).mapN(AnalyticsCommonSettings.apply)
  }

  private def operational(raw: RawOperational): ValidatedNec[String, AnalyticsOperationalSettings] = {
    val retention = (
      positive(raw.retention.bronzeDays, "analytics.operational.retention.bronze-days"),
      positive(raw.retention.quarantineDays, "analytics.operational.retention.quarantine-days"),
      positive(raw.retention.silverDays, "analytics.operational.retention.silver-days"),
      positive(raw.retention.publishedSnapshotDays, "analytics.operational.retention.published-snapshot-days"),
      positive(raw.retention.deletionMarkerDays, "analytics.operational.retention.deletion-marker-days"),
      positiveDuration(raw.retention.deltaVacuumSafety, "analytics.operational.retention.delta-vacuum-safety"),
      positiveDuration(raw.retention.deltaLogRetention, "analytics.operational.retention.delta-log-retention")
    ).mapN(AnalyticsRetentionSettings.apply)
    val reportReservationTtl =
      positive(raw.reportReservationTtlDays, "analytics.operational.report-reservation-ttl-days")
        .map(value => value.value.toLong.days)
    val mongoTransactionWindow = positive(
      raw.mongoTransactionWindowSeconds,
      "analytics.operational.mongo-transaction-window-seconds"
    ).map(value => value.value.toLong.seconds)
    (
      retention,
      reportReservationTtl,
      mongoTransactionWindow,
      positiveAtMost(
        raw.maximumErasureEvidenceFiles,
        Int.MaxValue - 1,
        "analytics.operational.maximum-erasure-evidence-files"
      ),
      positiveAtMost(
        raw.mongoPublisherBufferSize,
        AnalyticsOperationalSettings.MaximumMongoPublisherBufferSize,
        "analytics.operational.mongo-publisher-buffer-size"
      )
    ).mapN(AnalyticsOperationalSettings.apply)
  }

  private def kafkaSecurity(raw: RawAnalytics): ValidatedNec[String, (KafkaSecurityProtocol, Boolean)] = {
    (raw.kafka.securityProtocol.getOrElse(KafkaSecurityProtocol.SaslSsl), raw.kafka.allowPlaintext.getOrElse(false))
      .validNec[String]
  }

  private def required(value: Option[AnalyticsNonBlank], field: String): ValidatedNec[String, AnalyticsNonBlank] =
    value match {
      case None            => s"$field is required".invalidNec
      case Some(candidate) => candidate.validNec
    }

  private def present(value: Option[AnalyticsNonBlank], field: String): ValidatedNec[String, String] =
    value.toValidNec(s"$field is required")

  private def integer(value: Option[Int], field: String): ValidatedNec[String, Int] =
    value.toValidNec(s"$field is required")

  private def positive(value: Int, field: String): ValidatedNec[String, AnalyticsPositiveInt] =
    value.refineEither[Positive].leftMap(_ => s"$field must be greater than zero").toValidatedNec

  private def positiveDuration(value: FiniteDuration, field: String): ValidatedNec[String, FiniteDuration] =
    Either.cond(value.length > 0L, value, s"$field must be greater than zero").toValidatedNec

  private def positiveAtMost(value: Int, maximum: Int, field: String): ValidatedNec[String, Int] =
    Either.cond(value > 0 && value <= maximum, value, s"$field must be between one and $maximum").toValidatedNec

  private def long(value: Option[Long], field: String): ValidatedNec[String, Long] =
    value.toValidNec(s"$field is required")

  private def complete[A](value: ValidatedNec[String, A]): Either[AnalyticsError, A] =
    value.toEither.leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))
}
