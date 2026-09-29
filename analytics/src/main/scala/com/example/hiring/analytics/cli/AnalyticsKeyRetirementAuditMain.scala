package com.example.hiring.analytics.cli
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.service.erasure.*
import com.example.hiring.analytics.app.AppModule

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.{Clock, ExitCode, IO, IOApp}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Read-only operator entrypoint. A clear diagnostic is not an authorization to remove a key. */
object AnalyticsKeyRetirementAuditMain extends IOApp {
  private final case class AuditInputs(
      mongoUri: String,
      mongoDatabase: String,
      sparkMaster: String,
      paths: AnalyticsLakehousePaths,
      operational: AnalyticsOperationalSettings,
      retiringKeyId: String,
      retention: AnalyticsKeyRetirement.RetentionEvidence,
      writers: AnalyticsKeyRetirement.WriterInventory
  )

  private val logger = Slf4jLogger.getLogger[IO]

  private def auditInputs(settings: AnalyticsKeyRetirementAuditSettings): Either[AnalyticsError, AuditInputs] =
    AppModule.resolveLakehousePaths(settings.lakehouseRoot).map { paths =>
      def writerRecord(value: AnalyticsAuditWriter): AnalyticsKeyRetirement.WriterRecord =
        AnalyticsKeyRetirement.WriterRecord(
          value.identity.fold("")(identity),
          value.disposition.fold(AnalyticsKeyRetirement.WriterDisposition.Unknown) {
            case AnalyticsAuditWriterDisposition.Stopped       => AnalyticsKeyRetirement.WriterDisposition.Stopped
            case AnalyticsAuditWriterDisposition.AccessRevoked => AnalyticsKeyRetirement.WriterDisposition.AccessRevoked
            case AnalyticsAuditWriterDisposition.Active        => AnalyticsKeyRetirement.WriterDisposition.Active
            case AnalyticsAuditWriterDisposition.Unknown       => AnalyticsKeyRetirement.WriterDisposition.Unknown
          },
          value.evidenceReference.fold("")(identity)
        )

      AuditInputs(
        settings.mongoUri,
        settings.mongoDatabase,
        settings.sparkMaster,
        paths,
        settings.operational,
        settings.retiringKeyId,
        AnalyticsKeyRetirement.RetentionEvidence(
          AnalyticsKeyRetirement.KafkaRetentionEvidence(
            settings.kafkaBarrierOffset,
            settings.kafkaEarliestAvailableOffset,
            settings.kafkaEvidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.deltaData.retainedUntil,
            settings.deltaData.evidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.deltaLogs.retainedUntil,
            settings.deltaLogs.evidenceReference.fold("")(identity)
          ),
          AnalyticsKeyRetirement.RetentionHorizon(
            settings.reports.retainedUntil,
            settings.reports.evidenceReference.fold("")(identity)
          )
        ),
        AnalyticsKeyRetirement.WriterInventory(
          settings.writers.observedAt,
          settings.writers.coverageReference.fold("")(identity),
          settings.writers.managed.map(writerRecord),
          settings.writers.unmanaged.map(writerRecord)
        )
      )
    }

  private def program: IO[ExitCode] =
    AnalyticsRuntimeConfig
      .loadKeyRetirementAudit[IO]
      .flatMap(settings => IO.fromEither(auditInputs(settings)))
      .flatMap { inputs =>
        AppModule
          .sparkMongo[IO](
            inputs.mongoUri,
            inputs.sparkMaster,
            appName = "hiring-analytics-key-retirement-audit",
            sparkUiEnabled = Some(false)
          )
          .use { case (spark, client, sparkExecution) =>
            for {
              now <- Clock[IO].realTimeInstant
              database <- client.getDatabase(inputs.mongoDatabase)
              streams = new MongoPublisherStream(inputs.operational)
              result <- AnalyticsKeyRetirement.audit(
                spark,
                inputs.paths,
                database.underlying,
                inputs.retiringKeyId,
                inputs.retention,
                inputs.writers,
                now,
                new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
                  database,
                  Clock[IO],
                  streams
                ),
                streams,
                sparkExecution
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
