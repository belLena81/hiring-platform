package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.domain.AnalyticsTopic

/** Executes the production batch startup and recognizes only the precise retired-primary rejection. */
object RetiredHmacPrimaryStartupProofMain extends IOApp {
  // Only fixed categories and JVM class names are emitted. Configuration details, causes,
  // verifier material and exception messages remain private even when startup fails.
  private def diagnose(stage: String, error: Throwable): IO[Unit] = {
    val category = error match {
      case AnalyticsError.InvalidConfiguration(
            "isolated retirement calendar does not match its owned nonce fixture"
          ) =>
        "ISOLATION_MISMATCH"
      case AnalyticsError.InvalidConfiguration("isolated startup proof requires its nonce fixture") =>
        "ISOLATION_REQUIRED"
      case AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is invalid or its key is primary") =>
        "RETIRED_PRIMARY_REJECTION"
      case _: AnalyticsError.InvalidConfiguration => "INVALID_CONFIGURATION"
      case _: AnalyticsError                      => "ANALYTICS_ERROR"
      case _                                      => "UNEXPECTED_ERROR"
    }
    IO.println(
      s"ISOLATED_RETIRED_PRIMARY_DIAGNOSTIC stage=$stage category=$category class=${error.getClass.getSimpleName}"
    )
  }

  private def observed[A](stage: String)(action: IO[A]): IO[A] =
    action.onError { case error => diagnose(stage, error) }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      settings <- observed("CONFIGURATION")(AnalyticsRuntimeConfig.loadBatch[IO])
      common = settings.common
      shift <- observed("ISOLATION")(
        IO.fromEither(
          HmacRetirementProofCalendar.shift(
            common.lakehouseRoot,
            common.mongoDatabase,
            settings.manifest.offsetRanges.headOption.map(range => AnalyticsTopic.unwrap(range.topic)).getOrElse(""),
            common.hmac.keyId
          )
        )
      )
      _ <- observed("ISOLATION")(
        IO.raiseUnless(shift > 0L && args.isEmpty)(
          AnalyticsError.InvalidConfiguration("isolated startup proof requires its nonce fixture")
        )
      )
      result <- AppModule.batch[IO](settings).use(_.run).attempt
      _ <- result match {
        case Left(
              AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is invalid or its key is primary")
            ) =>
          IO.println("RETIRED_PRIMARY_STARTUP_REJECTED")
        case Left(error) =>
          diagnose("PRODUCTION_STARTUP", error) *> IO.raiseError(
            AnalyticsError.InvalidConfiguration("retired-primary startup did not produce the required rejection")
          )
        case Right(_) =>
          IO.println("ISOLATED_RETIRED_PRIMARY_DIAGNOSTIC stage=PRODUCTION_STARTUP category=UNEXPECTED_SUCCESS") *>
            IO.raiseError(
              AnalyticsError.InvalidConfiguration("retired-primary startup did not produce the required rejection")
            )
      }
    } yield ExitCode.Success).handleErrorWith(_ =>
      IO.println("ISOLATED_RETIRED_PRIMARY_STARTUP_PROOF_FAILED").as(ExitCode.Error)
    )
}
