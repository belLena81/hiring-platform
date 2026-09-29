package com.example.hiring.analytics.config

import AnalyticsPositiveInt.*

import com.example.hiring.analytics.domain.{
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
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.numeric.Positive
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import pureconfig.error.{ConfigReaderFailures, UserValidationFailed}
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
  import AnalyticsConfigReaders.given
  import AnalyticsConfigReaders.{
    Lakehouse as AnalyticsLakehouseSettings,
    Mongo as AnalyticsMongoSettings,
    Spark as AnalyticsSparkSettings
  }

  private final case class KafkaFencerSettings(username: Option[AnalyticsNonBlank], password: Option[AnalyticsNonBlank])
  private final case class KafkaRuntimeSettings(
      bootstrapServers: AnalyticsNonBlank,
      username: AnalyticsNonBlank,
      password: AnalyticsNonBlank,
      topic: AnalyticsTopic,
      fencer: KafkaFencerSettings,
      securityProtocol: Option[KafkaSecurityProtocol],
      allowPlaintext: Option[Boolean]
  )
  private final case class BatchInvocationSettings(
      runId: Option[AnalyticsNonBlank],
      partition: Option[Int],
      startOffset: Option[Long],
      endOffsetExclusive: Option[Long]
  )
  private final case class KafkaRetentionEvidence(
      barrierOffset: Option[Long],
      earliestAvailableOffset: Option[Long],
      evidenceReference: Option[AnalyticsNonBlank]
  )
  private final case class KeyRetirementAuditConfiguration(
      retiringKeyId: Option[AnalyticsNonBlank],
      kafka: KafkaRetentionEvidence,
      deltaData: AnalyticsAuditHorizon,
      deltaLogs: AnalyticsAuditHorizon,
      reports: AnalyticsAuditHorizon,
      writers: AnalyticsAuditWriterInventory
  )
  private final case class AnalyticsConfigValues(
      mongo: AnalyticsMongoSettings,
      spark: AnalyticsSparkSettings,
      kafka: KafkaRuntimeSettings,
      lakehouse: AnalyticsLakehouseSettings,
      hmac: AnalyticsHmacSettings,
      batch: Option[BatchInvocationSettings],
      operational: AnalyticsOperationalSettings,
      keyRetirementAudit: Option[KeyRetirementAuditConfiguration]
  )

  @nowarn("cat=deprecation")
  private object KebabCaseConfigReader
      extends ConfigReaderDerivation
      with CoproductConfigReaderDerivation(ConfigFieldMapping(PascalCase, KebabCase), "type")
      with ProductConfigReaderDerivation(
        ConfigFieldMapping(CamelCase, KebabCase)
          .withOverrides(
            "secretBase64" -> "secret-base64",
            "previousSecretBase64" -> "previous-secret-base64",
            "erasureWorkerTimings" -> "erasure-worker"
          )
      ) {
    import AnalyticsConfigReaders.given

    inline def derive[A](using Mirror.Of[A]): ConfigReader[A] = deriveConfigReader[A]
  }

  private given ConfigReader[KafkaFencerSettings] = KebabCaseConfigReader.derive[KafkaFencerSettings]
  private given ConfigReader[KafkaRuntimeSettings] = KebabCaseConfigReader.derive[KafkaRuntimeSettings]
  private given ConfigReader[AnalyticsHmacSettings] = KebabCaseConfigReader.derive[AnalyticsHmacSettings]
  private given ConfigReader[BatchInvocationSettings] = KebabCaseConfigReader.derive[BatchInvocationSettings]
  private given ConfigReader[AnalyticsPositiveInt] = ConfigReader[Int].emap { value =>
    value.refineEither[Positive].leftMap(_ => UserValidationFailed("must be greater than zero"))
  }
  private given ConfigReader[AnalyticsRetentionSettings] = KebabCaseConfigReader.derive[AnalyticsRetentionSettings]
  private given ConfigReader[AnalyticsErasureWorkerTimings] =
    KebabCaseConfigReader.derive[AnalyticsErasureWorkerTimings]
  private given ConfigReader[AnalyticsOperationalSettings] = KebabCaseConfigReader.derive[AnalyticsOperationalSettings]
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
  private given ConfigReader[KafkaRetentionEvidence] = KebabCaseConfigReader.derive[KafkaRetentionEvidence]
  private given ConfigReader[AnalyticsAuditHorizon] = KebabCaseConfigReader.derive[AnalyticsAuditHorizon]
  private given ConfigReader[AnalyticsAuditWriter] = KebabCaseConfigReader.derive[AnalyticsAuditWriter]
  private given ConfigReader[AnalyticsAuditWriterInventory] =
    KebabCaseConfigReader.derive[AnalyticsAuditWriterInventory]
  private given ConfigReader[KeyRetirementAuditConfiguration] =
    KebabCaseConfigReader.derive[KeyRetirementAuditConfiguration]
  private given ConfigReader[AnalyticsConfigValues] = KebabCaseConfigReader.derive[AnalyticsConfigValues]

  def loadBatch[F[_]: Async]: F[AnalyticsBatchSettings] =
    load[F].flatMap(raw => Async[F].fromEither(batch(raw)))

  def loadWorker[F[_]: Async]: F[AnalyticsWorkerSettings] =
    load[F].flatMap(raw => Async[F].fromEither(worker(raw)))

  def loadKeyRetirementAudit[F[_]: Async]: F[AnalyticsKeyRetirementAuditSettings] =
    load[F].flatMap(raw => Async[F].fromEither(keyRetirementAudit(raw)))

  def loadOperational[F[_]: Async]: F[AnalyticsOperationalSettings] =
    Async[F].blocking(ConfigSource.default.at("analytics.operational").load[AnalyticsOperationalSettings]).flatMap {
      case Right(settings) => Async[F].fromEither(complete(operational(settings)))
      case Left(failures)  =>
        Async[F].raiseError(
          configFailure("analytics.operational", failures)
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
        .load[AnalyticsOperationalSettings]
        .left
        .map(failures => configFailure("analytics.operational", failures))
      settings <- complete(operational(raw))
    } yield settings

  private def load[F[_]: Async]: F[AnalyticsConfigValues] =
    Async[F].blocking(ConfigSource.default.at("analytics").load[AnalyticsConfigValues]).flatMap {
      case Right(raw)     => Async[F].pure(raw)
      case Left(failures) => Async[F].raiseError(configFailure("analytics", failures))
    }

  private def resolve(value: String, environment: Map[String, String]): Either[AnalyticsError, AnalyticsConfigValues] =
    for {
      parsed <- Either
        .catchNonFatal(ConfigFactory.parseString(value, ConfigParseOptions.defaults().setAllowMissing(false)))
        .leftMap(error =>
          AnalyticsError.InvalidConfiguration(
            s"analytics HOCON configuration is malformed: ${redactConfigDiagnostic(error.getMessage)}"
          )
        )
      resolved <- Either
        .catchNonFatal(
          parsed.withFallback(ConfigFactory.parseMap(environment.asJava)).resolve(ConfigResolveOptions.noSystem())
        )
        .leftMap(error =>
          AnalyticsError.InvalidConfiguration(
            s"analytics configuration substitutions are invalid: ${redactConfigDiagnostic(error.getMessage)}"
          )
        )
      raw <- rawFrom(resolved)
    } yield raw

  private def rawFrom(config: Config): Either[AnalyticsError, AnalyticsConfigValues] =
    ConfigSource
      .fromConfig(config)
      .at("analytics")
      .load[AnalyticsConfigValues]
      .left
      .map(failures => configFailure("analytics", failures))

  private def batch(raw: AnalyticsConfigValues): Either[AnalyticsError, AnalyticsBatchSettings] = {
    val batchInputs = raw.batch.toValidNec("analytics.batch configuration is required")
    val manifest = batchInputs.andThen { input =>
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
    complete((common(raw), manifest).mapN(AnalyticsBatchSettings.apply))
  }

  private def worker(raw: AnalyticsConfigValues): Either[AnalyticsError, AnalyticsWorkerSettings] =
    complete(
      (
        common(raw),
        raw.kafka.topic.validNec[String],
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
      raw: AnalyticsConfigValues
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] = {
    val audit = raw.keyRetirementAudit.toRight(
      AnalyticsError.InvalidConfiguration("analytics.key-retirement-audit configuration is required")
    )
    for {
      settings <- audit
      mongoUri <- raw.mongo.uri
        .validNec[String]
        .andThen { uri =>
          Either
            .catchNonFatal(new ConnectionString(uri))
            .leftMap(_ => "analytics.mongo.uri is invalid")
            .toValidatedNec
            .as(uri)
        }
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
      mongoDatabase = raw.mongo.database
      sparkMaster = raw.spark.master
      root = raw.lakehouse.root
      retiringKeyId <- settings.retiringKeyId.toRight(
        AnalyticsError.InvalidConfiguration("analytics.key-retirement-audit.retiring-key-id is required")
      )
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
      settings.deltaData,
      settings.deltaLogs,
      settings.reports,
      settings.writers
    )
  }

  private def common(raw: AnalyticsConfigValues): ValidatedNec[String, AnalyticsCommonSettings] = {
    val mongoUri = raw.mongo.uri.validNec[String].andThen { uri =>
      Either
        .catchNonFatal(new ConnectionString(uri))
        .leftMap(_ => "analytics.mongo.uri is invalid")
        .toValidatedNec
        .as(uri)
    }
    val kafka = (
      raw.kafka.bootstrapServers.validNec[String],
      raw.kafka.username.validNec[String],
      raw.kafka.password.validNec[String],
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
    val lakehouseRoot = raw.lakehouse.root.validNec[String]
    val hmac = validateHmac(raw.hmac)
    val operationalSettings = operational(raw.operational)

    (
      mongoUri,
      raw.mongo.database.validNec[String],
      raw.spark.master.validNec[String],
      kafka,
      lakehouseRoot,
      hmac,
      operationalSettings
    ).mapN(AnalyticsCommonSettings.apply)
  }

  private def operational(
      settings: AnalyticsOperationalSettings
  ): ValidatedNec[String, AnalyticsOperationalSettings] = {
    val retention = (
      positiveDuration(settings.retention.deltaVacuumSafety, "analytics.operational.retention.delta-vacuum-safety"),
      positiveDuration(settings.retention.deltaLogRetention, "analytics.operational.retention.delta-log-retention")
    ).mapN((_, _) => settings.retention)
    val reportReservationTtl = positiveDuration(
      settings.reportReservationTtl,
      "analytics.operational.report-reservation-ttl"
    )
    val mongoTransactionWindow = positiveDuration(
      settings.mongoTransactionWindow,
      "analytics.operational.mongo-transaction-window"
    )
    val workerTimings = (
      positiveMillisecondDuration(
        settings.erasureWorkerTimings.leaseDuration,
        "analytics.operational.erasure-worker.lease-duration"
      ),
      positiveMillisecondDuration(
        settings.erasureWorkerTimings.deliveryTimeout,
        "analytics.operational.erasure-worker.delivery-timeout"
      ),
      positiveMillisecondDuration(
        settings.erasureWorkerTimings.pollInterval,
        "analytics.operational.erasure-worker.poll-interval"
      )
    ).mapN((_, _, _) => settings.erasureWorkerTimings)
    (
      retention,
      reportReservationTtl,
      mongoTransactionWindow,
      workerTimings
    ).mapN((_, _, _, _) => settings)
  }

  private def kafkaSecurity(raw: AnalyticsConfigValues): ValidatedNec[String, (KafkaSecurityProtocol, Boolean)] = {
    (raw.kafka.securityProtocol.getOrElse(KafkaSecurityProtocol.SaslSsl), raw.kafka.allowPlaintext.getOrElse(false))
      .validNec[String]
  }

  private def required(value: Option[AnalyticsNonBlank], field: String): ValidatedNec[String, AnalyticsNonBlank] =
    value match {
      case None            => s"$field is required".invalidNec
      case Some(candidate) => candidate.validNec
    }

  private def positiveDuration(value: FiniteDuration, field: String): ValidatedNec[String, FiniteDuration] =
    Either.cond(value.length > 0L, value, s"$field must be greater than zero").toValidatedNec

  private def positiveMillisecondDuration(
      value: FiniteDuration,
      field: String
  ): ValidatedNec[String, FiniteDuration] =
    Either.cond(value.toMillis > 0L, value, s"$field must be at least one millisecond").toValidatedNec

  private def validateHmac(raw: AnalyticsHmacSettings): ValidatedNec[String, AnalyticsHmacSettings] =
    (
      raw.secretBase64.validNec[String],
      raw.keyId.validNec[String],
      SubjectPseudonymizer.validateFromBase64(
        Some(raw.secretBase64),
        raw.keyId,
        raw.previousKeyId,
        raw.previousSecretBase64
      )
    ).mapN((secret, keyId, _) => raw.copy(secretBase64 = secret, keyId = keyId))

  private def complete[A](value: ValidatedNec[String, A]): Either[AnalyticsError, A] =
    value.toEither.leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))

  private def configFailure(path: String, failures: ConfigReaderFailures): AnalyticsError.InvalidConfiguration =
    AnalyticsError.InvalidConfiguration(
      s"$path configuration is invalid:\n${redactConfigDiagnostic(failures.prettyPrint())}"
    )

  private val SensitiveAssignment =
    "(?i)((?:previous-)?secret-base64|(?:sasl-)?(?:username|password)|mongo(?:db)?-uri)\\s*[:=]\\s*(\"(?:\\\\.|[^\"])*\"|[^,\\s}]+)".r
  private val MongoCredentials = "(?i)(mongodb(?:\\+srv)?://)[^/@\\s]+@".r
  private val LongEncodedSecret = "(?<![A-Za-z0-9])[A-Za-z0-9+/]{32,}={0,2}(?![A-Za-z0-9])".r

  private def redactConfigDiagnostic(message: String): String =
    LongEncodedSecret.replaceAllIn(
      MongoCredentials.replaceAllIn(
        SensitiveAssignment.replaceAllIn(message, matched => s"${matched.group(1)}=[REDACTED]"),
        matched => s"${matched.group(1)}[REDACTED]@"
      ),
      "[REDACTED_SECRET]"
    )
}
