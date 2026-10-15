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

  def readWithMask(source: ConfigSource): (Boolean, Either[NonEmptyList[ConfigError], AppConfig]) =
    (source.at("logging").load[RawLoggingConfig].toOption.forall(_.maskSensitive), read(source))

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
      loadSection[RawVectorSearchConfig](source, "vector-search").andThen(VectorSearchConfigValidation.read),
      resilienceSection(source),
      graphQLSection(source),
      recruiterJobClosureSection(source),
      readinessSection(source)
    ).mapN {
      (transport, logging, authSettings, kafkaSettings, vectorSettings, resilience, graphql, closure, readiness) =>
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
          authSettings.interviewActionRateLimit,
          resilience,
          graphql,
          closure,
          readiness
        )
    }.toEither

  /** An absent section keeps the conservative defaults; a present one must be complete and valid. */
  private def resilienceSection(source: ConfigSource): ValidatedNel[ConfigError, ResilienceConfig] =
    source
      .at("resilience")
      .cursor()
      .fold(
        _ => ResilienceConfig().validNel,
        _ => loadSection[RawResilienceConfig](source, "resilience").andThen(ResilienceConfigValidation.read)
      )

  /** An absent section keeps the defaults; a present one is validated, accumulating every violation. */
  private def graphQLSection(source: ConfigSource): ValidatedNel[ConfigError, GraphQLConfig] =
    source
      .at("graphql")
      .cursor()
      .fold(
        _ => GraphQLConfig().validNel,
        _ => loadSection[RawGraphQLConfig](source, "graphql").andThen(GraphQLConfigValidation.read)
      )

  /** An absent section keeps the defaults; a present one is validated, accumulating every violation. */
  private def recruiterJobClosureSection(source: ConfigSource): ValidatedNel[ConfigError, RecruiterJobClosureConfig] =
    source
      .at("recruiter-job-closure")
      .cursor()
      .fold(
        _ => RecruiterJobClosureConfig().validNel,
        _ =>
          loadSection[RawRecruiterJobClosureConfig](source, "recruiter-job-closure")
            .andThen(RecruiterJobClosureConfigValidation.read)
      )

  /** An absent section keeps the defaults; a present one is validated, accumulating every violation. */
  private def readinessSection(source: ConfigSource): ValidatedNel[ConfigError, ReadinessConfig] =
    source
      .at("readiness")
      .cursor()
      .fold(
        _ => ReadinessConfig().validNel,
        _ => loadSection[RawReadinessConfig](source, "readiness").andThen(ReadinessConfigValidation.read)
      )

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
