package com.example.graphQL.cats.config

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all.*
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import _root_.pureconfig.*
import _root_.pureconfig.error.{ConfigReaderFailures, ConvertFailure, KeyNotFound}

private[config] object AppConfigValidation {
  def parse(raw: String): Either[NonEmptyList[ConfigError], Config] =
    Either.catchNonFatal(ConfigFactory.parseString(raw, parseOptions)).leftMap(_ => parseError)

  def fromParsed(config: Config): Either[NonEmptyList[ConfigError], AppConfig] =
    Either
      .catchNonFatal(config.resolve(ConfigResolveOptions.noSystem()))
      .leftMap(_ => parseError)
      .map(ConfigSource.fromConfig)
      .flatMap(read)

  /** Each section is validated as soon as it decodes, so a decode failure in one section never hides another's. */
  def read(source: ConfigSource): Either[NonEmptyList[ConfigError], AppConfig] =
    (
      (
        loadSection[RawHttpConfig](source, "http"),
        loadSection[RawMongoConfig](source, "mongo")
          .andThen(mongo => HttpMongoConfigValidation.validMongoDatabase(mongo.database).as(mongo))
      ).tupled.andThen(HttpMongoConfigValidation.read.tupled),
      loadSection[RawLoggingConfig](source, "logging"),
      loadSection[RawAuthConfig](source, "auth").andThen(AuthConfigValidation.read),
      loadSection[RawKafkaConfig](source, "kafka").andThen(KafkaConfigValidation.read),
      loadSection[RawVectorSearchConfig](source, "vector-search").andThen(VectorSearchConfigValidation.read)
    ).mapN { (transport, logging, authSettings, kafkaSettings, vectorSettings) =>
      AppConfig(
        transport.host,
        transport.port,
        transport.admissionPermits,
        transport.requestTimeout,
        transport.trustedProxy,
        transport.mongoUri,
        transport.mongoDatabase,
        logging.maskSensitive,
        authSettings.jwt,
        authSettings.passwordHash,
        authSettings.rateLimit,
        vectorSettings,
        kafkaSettings,
        transport.resetOnStart,
        transport.discovery,
        authSettings.adminSeed,
        authSettings.interviewActionRateLimit
      )
    }.toEither

  private def loadSection[A: ConfigReader](source: ConfigSource, section: String): ValidatedNel[ConfigError, A] =
    source.at(section).load[A].leftMap(readError(_, section)).toValidated

  /** Every decoding failure names a path; the owning `ConfigError` is declared on the enum case itself. */
  private def readError(failures: ConfigReaderFailures, section: String): NonEmptyList[ConfigError] =
    failures.toList
      .collect {
        case ConvertFailure(KeyNotFound(key, _), _, path) => qualifiedPath(section, fullPath(path, key))
        case ConvertFailure(_, _, path)                   => qualifiedPath(section, path)
      }
      .distinct
      .map(path => ConfigError.forPath(path).getOrElse(ConfigError.InvalidConfigFile))
      .toNel
      .getOrElse(parseError)

  private def qualifiedPath(section: String, path: String): String =
    if (path == section || path.startsWith(s"$section.")) path else s"$section.$path"

  private def fullPath(path: String, key: String): String =
    Option(path).filter(_.nonEmpty).fold(key)(parent => s"$parent.$key")

  private val parseError: NonEmptyList[ConfigError] = NonEmptyList.one(ConfigError.InvalidConfigFile)

  private val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)
}
