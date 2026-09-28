package com.example.hiring.analytics.config
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.{AnalyticsLakehousePaths, KafkaConnection}

import cats.data.ValidatedNec
import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.ConnectionString
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Interval
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank
import _root_.pureconfig.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

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
  import AnalyticsRawConfig.{Lakehouse as RawLakehouse, Mongo as RawMongo, Spark as RawSpark}

  private final case class RawFencer(username: Option[String], password: Option[String]) derives ConfigReader
  private final case class RawKafka(
      bootstrapServers: Option[String],
      username: Option[String],
      password: Option[String],
      topic: Option[String],
      fencer: RawFencer,
      securityProtocol: Option[String],
      allowPlaintext: Option[String]
  )
  private final case class RawHmac(
      secretBase64: Option[String],
      keyId: Option[String],
      previousKeyId: Option[String],
      previousSecretBase64: Option[String]
  )
  private final case class RawBatch(
      runId: Option[String],
      partition: Option[String],
      startOffset: Option[String],
      endOffsetExclusive: Option[String]
  )
  private final case class RawAnalytics(
      mongo: RawMongo,
      spark: RawSpark,
      kafka: RawKafka,
      lakehouse: RawLakehouse,
      hmac: RawHmac,
      batch: RawBatch
  ) derives ConfigReader

  // Scala 3's built-in PureConfig derivation ignores ProductHint and does not map camelCase fields to kebab-case.
  private given ConfigReader[RawKafka] =
    ConfigReader.forProduct7(
      "bootstrap-servers",
      "username",
      "password",
      "topic",
      "fencer",
      "security-protocol",
      "allow-plaintext"
    )(RawKafka.apply)
  private given ConfigReader[RawHmac] =
    ConfigReader.forProduct4("secret-base64", "key-id", "previous-key-id", "previous-secret-base64")(RawHmac.apply)
  private given ConfigReader[RawBatch] =
    ConfigReader.forProduct4("run-id", "partition", "start-offset", "end-offset-exclusive")(RawBatch.apply)

  def loadBatch: IO[AnalyticsBatchSettings] = load.flatMap(raw => IO.fromEither(batch(raw)))

  def loadWorker: IO[AnalyticsWorkerSettings] = load.flatMap(raw => IO.fromEither(worker(raw)))

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

  private def load: IO[RawAnalytics] =
    IO.blocking(ConfigSource.default.at("analytics").load[RawAnalytics]).flatMap {
      case Right(raw) => IO.pure(raw)
      case Left(_)    =>
        IO.raiseError(AnalyticsError.InvalidConfiguration("analytics configuration is missing or malformed"))
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
    complete(
      (
        common(raw),
        required(raw.kafka.topic, "analytics.kafka.topic"),
        required(raw.batch.runId, "analytics.batch.run-id"),
        nonNegativeInt(raw.batch.partition, "analytics.batch.partition"),
        nonNegativeLong(raw.batch.startOffset, "analytics.batch.start-offset"),
        nonNegativeLong(raw.batch.endOffsetExclusive, "analytics.batch.end-offset-exclusive"),
        offsetOrder(raw.batch.startOffset, raw.batch.endOffsetExclusive)
      ).mapN { (settings, topic, runId, partition, start, end, _) =>
        AnalyticsRunManifest
          .validated(runId, Vector(PartitionOffsetRange.refined(topic, partition, start, end)))
          .map(AnalyticsBatchSettings(settings, _))
      }.andThen(identity)
    )

  private def worker(raw: RawAnalytics): Either[AnalyticsError, AnalyticsWorkerSettings] =
    complete(
      (
        common(raw),
        required(raw.kafka.topic, "analytics.kafka.topic"),
        required(raw.kafka.fencer.username, "analytics.kafka.fencer.username"),
        required(raw.kafka.fencer.password, "analytics.kafka.fencer.password")
      ).mapN { (settings, topic, username, password) =>
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
      }.andThen(identity)
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
    val paths = required(raw.lakehouse.root, "analytics.lakehouse.root")
      .andThen(root => AnalyticsLakehousePaths.validate(AnalyticsLakehousePaths(root)))
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
    val protocol = raw.kafka.securityProtocol.getOrElse("SASL_SSL")
    val allowPlaintext = raw.kafka.allowPlaintext match {
      case None | Some("false") => false.validNec[String]
      case Some("true")         => true.validNec[String]
      case Some(_)              => "analytics.kafka.allow-plaintext must be true or false".invalidNec
    }
    allowPlaintext.map(protocol -> _)
  }

  private def required(value: Option[String], field: String): ValidatedNec[String, AnalyticsNonBlank] =
    value match {
      case None            => s"$field is required".invalidNec
      case Some(candidate) =>
        candidate.refineEither[Not[Blank]].leftMap(_ => s"$field must be non-empty").toValidatedNec
    }

  private def nonNegativeInt(value: Option[String], field: String): ValidatedNec[String, AnalyticsPartition] =
    required(value, field)
      .andThen { candidate =>
        Try(candidate.toInt).toEither.leftMap(_ => s"$field must be an integer").toValidatedNec
      }
      .andThen { candidate =>
        candidate
          .refineEither[Interval.Closed[0, 2147483647]]
          .leftMap(_ => s"$field must be non-negative")
          .toValidatedNec
      }

  private def nonNegativeLong(value: Option[String], field: String): ValidatedNec[String, AnalyticsOffset] =
    required(value, field)
      .andThen { candidate =>
        Try(candidate.toLong).toEither.leftMap(_ => s"$field must be an integer").toValidatedNec
      }
      .andThen { candidate =>
        candidate
          .refineEither[Interval.Closed[0L, 9223372036854775807L]]
          .leftMap(_ => s"$field must be non-negative")
          .toValidatedNec
      }

  private def offsetOrder(start: Option[String], end: Option[String]): ValidatedNec[String, Unit] =
    (
      nonNegativeLong(start, "analytics.batch.start-offset"),
      nonNegativeLong(end, "analytics.batch.end-offset-exclusive")
    ).mapN { (first, last) =>
      if (last < first) "end offset must not precede start offset".invalidNec[Unit]
      else ().validNec[String]
    }.andThen(identity)

  private def complete[A](value: ValidatedNec[String, A]): Either[AnalyticsError, A] =
    value.toEither.leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))
}
