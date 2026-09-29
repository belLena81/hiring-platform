package com.example.hiring.analytics.cli

import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsErasureQueue
import com.example.hiring.analytics.domain.AccountSubjectId
import com.example.hiring.analytics.service.erasure.ErasureUpdate

import cats.data.Validated
import cats.effect.{ExitCode, IO}
import cats.syntax.all.*
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp

/** Local operator utility. It exposes only fixed failure labels and requires an observed attempt count to requeue. */
object AnalyticsErasureRepairMain
    extends CommandIOApp(
      name = "analytics-erasure-repair",
      header = "Inspect and requeue analytics erasure repair requests",
      version = "0.1.0"
    ) {
  private[analytics] enum RepairAction {
    case Inspect(limit: Int)
    case Requeue(requestId: AccountSubjectId, observedAttemptCount: Int)
  }

  private def positiveInt(name: String): Opts[Int] =
    Opts.argument[Int](metavar = name).mapValidated { value =>
      if (value > 0) Validated.valid(value)
      else Validated.invalidNel(s"$name must be positive")
    }

  private[analytics] val repairOptions: Opts[RepairAction] =
    Opts.subcommand("inspect", "List repair requests") {
      positiveInt("positive-limit").map(RepairAction.Inspect.apply)
    } orElse Opts.subcommand("requeue", "Requeue a matching repair request") {
      (
        Opts.argument[String](metavar = "request-id").mapValidated { value =>
          AccountSubjectId.from(value) match {
            case Right(id) => Validated.valid(id)
            case Left(_)   => Validated.invalidNel("invalid repair request identity")
          }
        },
        positiveInt("observed-attempt-count")
      ).mapN(RepairAction.Requeue.apply)
    }

  private[analytics] def requeueExitCode(updated: ErasureUpdate): ExitCode = updated match {
    case ErasureUpdate.Applied   => ExitCode.Success
    case ErasureUpdate.LeaseLost => ExitCode.Error
  }

  override def main: Opts[IO[ExitCode]] = repairOptions.map {
    case RepairAction.Inspect(limit) =>
      program { store =>
        store
          .inspectRepairRequests(limit)
          .flatMap { requests =>
            requests.traverse_(request =>
              IO.println(
                s"request=${request.requestId.value} phase=${request.phase} attempts=${request.attemptCount} category=${request.failureCategory}"
              )
            )
          }
          .as(ExitCode.Success)
      }
    case RepairAction.Requeue(subjectId, attempt) =>
      program { store =>
        IO.realTimeInstant.flatMap(now => store.requeueRepair(subjectId, attempt, now)).flatMap {
          case ErasureUpdate.Applied =>
            IO.println("requeued the matching repair request").as(requeueExitCode(ErasureUpdate.Applied))
          case ErasureUpdate.LeaseLost =>
            IO.println("request state, attempt count, or lease changed; no update made")
              .as(requeueExitCode(ErasureUpdate.LeaseLost))
        }
      }
  }

  private def program(operation: MongoAnalyticsErasureQueue[IO] => IO[ExitCode]): IO[ExitCode] =
    AnalyticsRuntimeConfig.loadWorker[IO].flatMap(settings => AppModule.repair[IO](settings).use(operation))
}
