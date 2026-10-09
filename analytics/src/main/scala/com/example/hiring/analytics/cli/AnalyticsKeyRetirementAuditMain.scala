package com.example.hiring.analytics.cli

import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig

import cats.effect.{ExitCode, IO}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Read-only operator entrypoint. A clear diagnostic is not an authorization to remove a key. */
object AnalyticsKeyRetirementAuditMain
    extends HoconConfiguredCommand(
      name = "analytics-key-retirement-audit",
      header = "Diagnose whether an HMAC key still has repository-visible references"
    ) {
  private val logger = Slf4jLogger.getLogger[IO]

  override protected def program: IO[ExitCode] =
    AnalyticsRuntimeConfig
      .loadKeyRetirementAudit[IO]
      .flatMap(settings => AppModule.keyRetirementAudit[IO](settings).use(identity))
      .flatMap {
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
}
