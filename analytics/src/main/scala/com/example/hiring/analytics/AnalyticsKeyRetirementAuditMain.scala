package com.example.hiring.analytics

import com.example.hiring.analytics.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.erasure.*

import cats.effect.{Clock, ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.mongodb.client.MongoClient
import com.typesafe.config.ConfigFactory
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.{ConfigReader, ConfigSource}

import java.time.Instant
import scala.util.Try

/** Read-only operator entrypoint. A clear diagnostic is not an authorization to remove a key. */
object AnalyticsKeyRetirementAuditMain extends IOApp {
  import AnalyticsRawConfig.{Lakehouse as RawLakehouse, Mongo as RawMongo, Spark as RawSpark}

  private final case class RawKafka(
      barrierOffset: Option[Long],
      earliestAvailableOffset: Option[Long],
      evidenceReference: Option[String]
  )
  private final case class RawHorizon(retainedUntil: Option[String], evidenceReference: Option[String])
  private final case class RawWriter(
      identity: Option[String],
      disposition: Option[String],
      evidenceReference: Option[String]
  )
  private final case class RawWriters(
      observedAt: Option[String],
      coverageReference: Option[String],
      managed: Vector[RawWriter],
      unmanaged: Vector[RawWriter]
  )
  private final case class RawAudit(
      mongo: RawMongo,
      spark: RawSpark,
      lakehouse: RawLakehouse,
      retiringKeyId: Option[String],
      kafka: RawKafka,
      deltaData: RawHorizon,
      deltaLogs: RawHorizon,
      reports: RawHorizon,
      writers: RawWriters
  )
  private final case class AuditInputs(
      mongoUri: String,
      mongoDatabase: String,
      sparkMaster: String,
      paths: AnalyticsLakehousePaths,
      retiringKeyId: String,
      retention: AnalyticsKeyRetirement.RetentionEvidence,
      writers: AnalyticsKeyRetirement.WriterInventory
  )

  private given ConfigReader[RawKafka] =
    ConfigReader.forProduct3("barrier-offset", "earliest-available-offset", "evidence-reference")(RawKafka.apply)
  private given ConfigReader[RawHorizon] =
    ConfigReader.forProduct2("retained-until", "evidence-reference")(RawHorizon.apply)
  private given ConfigReader[RawWriter] =
    ConfigReader.forProduct3("identity", "disposition", "evidence-reference")(RawWriter.apply)
  private given ConfigReader[RawWriters] =
    ConfigReader.forProduct4("observed-at", "coverage-reference", "managed", "unmanaged")(RawWriters.apply)
  private given ConfigReader[RawAudit] = ConfigReader.forProduct9(
    "mongo",
    "spark",
    "lakehouse",
    "retiring-key-id",
    "kafka",
    "delta-data",
    "delta-logs",
    "reports",
    "writers"
  )(RawAudit.apply)

  private val logger = Slf4jLogger.getLogger[IO]

  private[analytics] def validateAuditHocon(value: String): Either[AnalyticsError, Unit] =
    Either
      .catchNonFatal(ConfigFactory.parseString(value).resolve())
      .leftMap(_ => AnalyticsError.InvalidConfiguration("key-retirement audit HOCON is malformed"))
      .flatMap { config =>
        ConfigSource
          .fromConfig(config)
          .at("analytics.key-retirement-audit")
          .load[RawAudit]
          .left
          .map(_ => AnalyticsError.InvalidConfiguration("key-retirement audit HOCON is missing or malformed"))
          .map(_ => ())
      }

  private def load: IO[RawAudit] =
    IO.blocking(ConfigSource.default.at("analytics.key-retirement-audit").load[RawAudit]).flatMap {
      case Right(value) => IO.pure(value)
      case Left(_)      =>
        IO.raiseError(AnalyticsError.InvalidConfiguration("key-retirement audit HOCON is missing or malformed"))
    }

  private def instant(name: String, value: Option[String]): Either[String, Option[Instant]] =
    value.traverse(text => Try(Instant.parse(text)).toEither.leftMap(_ => s"$name must be an ISO-8601 UTC timestamp"))

  private def writerRecords(
      values: Vector[RawWriter],
      group: String
  ): Either[String, Vector[AnalyticsKeyRetirement.WriterRecord]] =
    values.traverse { value =>
      val disposition = value.disposition.flatMap {
        case "stopped"        => Some(AnalyticsKeyRetirement.WriterDisposition.Stopped)
        case "access-revoked" => Some(AnalyticsKeyRetirement.WriterDisposition.AccessRevoked)
        case "active"         => Some(AnalyticsKeyRetirement.WriterDisposition.Active)
        case "unknown"        => Some(AnalyticsKeyRetirement.WriterDisposition.Unknown)
        case _                => None
      }
      disposition.toRight(s"$group writer disposition must be stopped, access-revoked, active, or unknown").map {
        status =>
          AnalyticsKeyRetirement.WriterRecord(
            value.identity.getOrElse(""),
            status,
            value.evidenceReference.getOrElse("")
          )
      }
    }

  private def decode(raw: RawAudit): Either[String, AuditInputs] =
    for {
      mongoUri <- raw.mongo.uri.filter(_.trim.nonEmpty).toRight("analytics.key-retirement-audit.mongo.uri is required")
      mongoDatabase <- raw.mongo.database
        .filter(_.trim.nonEmpty)
        .toRight("analytics.key-retirement-audit.mongo.database is required")
      sparkMaster <- raw.spark.master
        .filter(_.trim.nonEmpty)
        .toRight("analytics.key-retirement-audit.spark.master is required")
      root <- raw.lakehouse.root
        .filter(_.trim.nonEmpty)
        .toRight("analytics.key-retirement-audit.lakehouse.root is required")
      keyId <- raw.retiringKeyId
        .filter(_.trim.nonEmpty)
        .toRight("analytics.key-retirement-audit.retiring-key-id is required")
      deltaDataUntil <- instant("delta-data.retained-until", raw.deltaData.retainedUntil)
      deltaLogsUntil <- instant("delta-logs.retained-until", raw.deltaLogs.retainedUntil)
      reportsUntil <- instant("reports.retained-until", raw.reports.retainedUntil)
      writersObservedAt <- instant("writers.observed-at", raw.writers.observedAt)
      managed <- writerRecords(raw.writers.managed, "managed")
      unmanaged <- writerRecords(raw.writers.unmanaged, "unmanaged")
    } yield AuditInputs(
      mongoUri,
      mongoDatabase,
      sparkMaster,
      AnalyticsLakehousePaths(root),
      keyId,
      AnalyticsKeyRetirement.RetentionEvidence(
        AnalyticsKeyRetirement.KafkaRetentionEvidence(
          raw.kafka.barrierOffset,
          raw.kafka.earliestAvailableOffset,
          raw.kafka.evidenceReference.getOrElse("")
        ),
        AnalyticsKeyRetirement.RetentionHorizon(deltaDataUntil, raw.deltaData.evidenceReference.getOrElse("")),
        AnalyticsKeyRetirement.RetentionHorizon(deltaLogsUntil, raw.deltaLogs.evidenceReference.getOrElse("")),
        AnalyticsKeyRetirement.RetentionHorizon(reportsUntil, raw.reports.evidenceReference.getOrElse(""))
      ),
      AnalyticsKeyRetirement.WriterInventory(
        writersObservedAt.orNull,
        raw.writers.coverageReference.getOrElse(""),
        managed,
        unmanaged
      )
    )

  private def resources(inputs: AuditInputs) =
    com.example.hiring.analytics.batch.HiringAnalyticsBatchMain.managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .appName("hiring-analytics-key-retirement-audit")
          .master(inputs.sparkMaster)
          .config("spark.ui.enabled", "false")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      ),
      IO.blocking(com.mongodb.client.MongoClients.create(inputs.mongoUri))
    )

  private def program: IO[ExitCode] =
    load.flatMap(raw => IO.fromEither(decode(raw).leftMap(AnalyticsError.InvalidConfiguration.apply))).flatMap {
      inputs =>
        resources(inputs).use { case (spark, client: MongoClient) =>
          for {
            now <- Clock[IO].realTimeInstant
            database = client.getDatabase(inputs.mongoDatabase)
            result <- AnalyticsKeyRetirement.audit(
              spark,
              inputs.paths,
              database,
              inputs.retiringKeyId,
              inputs.retention,
              inputs.writers,
              now,
              new com.example.hiring.analytics.mongo.MongoAnalyticsLakehouseLock(database)
            )
            code <- result match {
              case Right(summary) =>
                logger.warn(summary.operatorEvidence + "; exit status is not approval to remove a key") *>
                  logger
                    .info(
                      s"key-retirement diagnostic scanned ${summary.deltaFilesScanned} Delta files and ${summary.mongoDocumentsScanned} Mongo documents"
                    )
                    .as(ExitCode.Success)
              case Left(blockers) =>
                logger
                  .warn("key-retirement diagnostic blocked: " + blockers.toNonEmptyList.toList.mkString("; "))
                  .as(ExitCode.Error)
            }
          } yield code
        }
    }

  override def run(args: List[String]): IO[ExitCode] =
    if (args.nonEmpty)
      IO.raiseError(
        AnalyticsError.InvalidConfiguration(
          "audit settings are loaded from HOCON; command-line arguments are not accepted"
        )
      )
    else
      program.handleErrorWith {
        case error: AnalyticsError => logger.error(error.getMessage).as(ExitCode.Error)
        case error                 =>
          logger.error("key-retirement diagnostic failed (" + error.getClass.getSimpleName + ")").as(ExitCode.Error)
      }
}
