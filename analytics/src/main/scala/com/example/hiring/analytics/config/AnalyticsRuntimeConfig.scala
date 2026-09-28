package com.example.hiring.analytics.config

import com.example.hiring.analytics.domain.{AnalyticsRunManifest, PartitionOffsetRange, RunId, SubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.data.ValidatedNec
import cats.effect.Async
import cats.syntax.all.*
import com.mongodb.ConnectionString
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import pureconfig.generic.derivation.{
  ConfigReaderDerivation,
  CoproductConfigReaderDerivation,
  ProductConfigReaderDerivation
}
import scala.annotation.nowarn
import scala.deriving.Mirror
import scala.jdk.CollectionConverters.*

type AnalyticsNonBlank = String :| Not[Blank]

final case class AnalyticsCommonSettings(
    mongoUri: AnalyticsNonBlank,
    mongoDatabase: AnalyticsNonBlank,
    sparkMaster: AnalyticsNonBlank,
    kafka: KafkaConnection,
    lakehousePaths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer
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
      securityProtocol: Option[AnalyticsNonBlank],
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
  private final case class RawAnalytics(
      mongo: RawMongo,
      spark: RawSpark,
      kafka: RawKafka,
      lakehouse: RawLakehouse,
      hmac: RawHmac,
      batch: RawBatch
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
  private given ConfigReader[RawAnalytics] = KebabCaseConfigReader.derive[RawAnalytics]

  def loadBatch[F[_]: Async]: F[AnalyticsBatchSettings] =
    load[F].flatMap(raw => Async[F].fromEither(batch(raw)))

  def loadWorker[F[_]: Async]: F[AnalyticsWorkerSettings] =
    load[F].flatMap(raw => Async[F].fromEither(worker(raw)))

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
    val ranges = PartitionOffsetRange
      .fromValidated(
        present(raw.kafka.topic, "analytics.kafka.topic"),
        integer(raw.batch.partition, "analytics.batch.partition"),
        long(raw.batch.startOffset, "analytics.batch.start-offset"),
        long(raw.batch.endOffsetExclusive, "analytics.batch.end-offset-exclusive")
      )
      .map(Vector(_))
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
    val paths = raw.lakehouse.root match {
      case Some(root) => AnalyticsLakehousePaths.from(root)
      case None       => "analytics.lakehouse.root is required".invalidNec
    }
    val pseudonymizer = SubjectPseudonymizer.validateFromBase64(
      raw.hmac.secretBase64,
      raw.hmac.keyId.getOrElse("hmac-v1"),
      raw.hmac.previousKeyId,
      raw.hmac.previousSecretBase64
    )

    (
      mongoUri,
      required(raw.mongo.database, "analytics.mongo.database"),
      required(raw.spark.master, "analytics.spark.master"),
      kafka,
      paths,
      pseudonymizer
    ).mapN(AnalyticsCommonSettings.apply)
  }

  private def kafkaSecurity(raw: RawAnalytics): ValidatedNec[String, (String, Boolean)] = {
    (raw.kafka.securityProtocol.getOrElse("SASL_SSL"), raw.kafka.allowPlaintext.getOrElse(false)).validNec[String]
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

  private def long(value: Option[Long], field: String): ValidatedNec[String, Long] =
    value.toValidNec(s"$field is required")

  private def complete[A](value: ValidatedNec[String, A]): Either[AnalyticsError, A] =
    value.toEither.leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))
}
