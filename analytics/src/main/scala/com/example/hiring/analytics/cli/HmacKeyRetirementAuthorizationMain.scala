package com.example.hiring.analytics.cli
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
import com.example.hiring.analytics.app.AppModule

import cats.effect.{Clock, ExitCode, IO, IOApp}
import cats.effect.Clock
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.{ConfigReader, ConfigSource}

import scala.util.control.NonFatal

/** Host-only local retirement operator command. The diagnostic entry point never writes an authorization. */
object HmacKeyRetirementAuthorizationMain extends IOApp {
  private final case class RawMongo(uri: Option[String], database: Option[String])
  private final case class RawSpark(master: Option[String])
  private final case class RawLakehouse(root: Option[String])
  private final case class RawKafka(
      bootstrapServers: Option[String],
      username: Option[String],
      password: Option[String],
      securityProtocol: Option[KafkaSecurityProtocol],
      allowPlaintext: Option[Boolean],
      topic: Option[String]
  )
  private final case class RawDocker(
      volumeName: Option[String],
      oldImage: Option[String],
      oldImageId: Option[String],
      oldUid: Option[Int],
      newImage: Option[String],
      newImageId: Option[String],
      newUid: Option[Int]
  )
  private final case class RawSettings(
      mongo: RawMongo,
      spark: RawSpark,
      lakehouse: RawLakehouse,
      kafka: RawKafka,
      retiringKeyId: Option[String],
      docker: RawDocker
  )
  private final case class Settings(
      mongoUri: String,
      database: String,
      sparkMaster: String,
      paths: AnalyticsLakehousePaths,
      kafka: KafkaConnection,
      topic: String,
      keyId: String,
      docker: LocalHmacKeyWriterExclusion.Settings,
      operational: AnalyticsOperationalSettings
  )

  private given ConfigReader[RawMongo] = ConfigReader.forProduct2("uri", "database")(RawMongo.apply)
  private given ConfigReader[RawSpark] = ConfigReader.forProduct1("master")(RawSpark.apply)
  private given ConfigReader[RawLakehouse] = ConfigReader.forProduct1("root")(RawLakehouse.apply)
  private given ConfigReader[RawKafka] = ConfigReader.forProduct6(
    "bootstrap-servers",
    "username",
    "password",
    "security-protocol",
    "allow-plaintext",
    "topic"
  )(RawKafka.apply)
  private given ConfigReader[RawDocker] = ConfigReader.forProduct7(
    "volume-name",
    "old-image",
    "old-image-id",
    "old-uid",
    "new-image",
    "new-image-id",
    "new-uid"
  )(RawDocker.apply)
  private given ConfigReader[RawSettings] = ConfigReader.forProduct6(
    "mongo",
    "spark",
    "lakehouse",
    "kafka",
    "retiring-key-id",
    "docker"
  )(RawSettings.apply)

  private val logger = Slf4jLogger.getLogger[IO]

  private def required(name: String, value: Option[String]): Either[AnalyticsError, String] =
    value.filter(_.trim.nonEmpty).toRight(AnalyticsError.InvalidConfiguration(s"$name is required"))

  private def decode(
      raw: RawSettings,
      operational: AnalyticsOperationalSettings
  ): Either[AnalyticsError, Settings] =
    for {
      mongoUri <- required("retirement.mongo.uri", raw.mongo.uri)
      database <- required("retirement.mongo.database", raw.mongo.database)
      master <- required("retirement.spark.master", raw.spark.master)
      root <- required("retirement.lakehouse.root", raw.lakehouse.root)
      paths <- AnalyticsLakehousePaths
        .from(root)
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
      bootstrap <- required("retirement.kafka.bootstrap-servers", raw.kafka.bootstrapServers)
      topic <- required("retirement.kafka.topic", raw.kafka.topic)
      keyId <- required("retirement.retiring-key-id", raw.retiringKeyId)
      volume <- required("retirement.docker.volume-name", raw.docker.volumeName)
      oldImage <- required("retirement.docker.old-image", raw.docker.oldImage)
      oldImageId <- required("retirement.docker.old-image-id", raw.docker.oldImageId)
      oldUid <- raw.docker.oldUid.toRight(AnalyticsError.InvalidConfiguration("retirement.docker.old-uid is required"))
      newImage <- required("retirement.docker.new-image", raw.docker.newImage)
      newImageId <- required("retirement.docker.new-image-id", raw.docker.newImageId)
      newUid <- raw.docker.newUid.toRight(AnalyticsError.InvalidConfiguration("retirement.docker.new-uid is required"))
      connection <- KafkaConnection
        .validate(
          KafkaConnection(
            bootstrap,
            raw.kafka.username.filter(_.nonEmpty),
            raw.kafka.password.filter(_.nonEmpty),
            raw.kafka.securityProtocol.getOrElse(KafkaSecurityProtocol.SaslSsl),
            raw.kafka.allowPlaintext.getOrElse(false)
          )
        )
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))
    } yield Settings(
      mongoUri,
      database,
      master,
      paths,
      connection,
      topic,
      keyId,
      LocalHmacKeyWriterExclusion.Settings(volume, oldImage, oldImageId, oldUid, newImage, newImageId, newUid),
      operational
    )

  private def program(action: String): IO[Unit] =
    IO.blocking(ConfigSource.default.at("analytics.key-retirement-authorization").load[RawSettings])
      .flatMap {
        case Right(raw) =>
          AnalyticsRuntimeConfig.loadOperational[IO].flatMap(value => IO.fromEither(decode(raw, value)))
        case Left(_) =>
          IO.raiseError(
            AnalyticsError.InvalidConfiguration("key-retirement authorization HOCON is missing or malformed")
          )
      }
      .flatMap { settings =>
        AppModule
          .sparkMongo[IO](
            settings.mongoUri,
            settings.sparkMaster,
            appName = "hiring-hmac-key-retirement",
            sparkUiEnabled = Some(false)
          )
          .use { case (spark, mongo, sparkExecution) =>
            mongo.getDatabase(settings.database).flatMap { database =>
              val clock = Clock[IO]
              val streams = new MongoPublisherStream(settings.operational)
              val coordinator = new HmacKeyRetirementCoordinator[IO](
                spark,
                settings.paths,
                database,
                settings.kafka,
                settings.topic,
                settings.docker,
                settings.operational,
                streams,
                clock,
                new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](database, clock, streams),
                sparkExecution
              )
              action match {
                case "prepare"   => coordinator.prepare(settings.keyId).void
                case "authorize" => coordinator.authorize(settings.keyId).void
                case _           =>
                  IO.raiseError(AnalyticsError.InvalidConfiguration("retirement action must be prepare or authorize"))
              }
            }
          }
      }

  override def run(args: List[String]): IO[ExitCode] = args match {
    case List(action @ ("prepare" | "authorize")) =>
      program(action).attempt.flatMap {
        case Right(_)                    => logger.info(s"HMAC key retirement $action completed").as(ExitCode.Success)
        case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
        case Left(error)                 =>
          logger
            .error(s"HMAC key retirement failed (${error.getClass.getSimpleName})")
            .as(ExitCode.Error)
      }
    case _ => logger.error("retirement action must be prepare or authorize").as(ExitCode.Error)
  }
}
