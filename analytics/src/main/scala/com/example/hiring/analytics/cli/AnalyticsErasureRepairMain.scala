package com.example.hiring.analytics.cli

import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsErasureWorkerStore
import com.example.hiring.analytics.domain.AccountSubjectId

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*

/** Local operator utility. It exposes only fixed failure labels and requires an observed attempt count to requeue. */
object AnalyticsErasureRepairMain extends IOApp {
  private[analytics] def requeueExitCode(updated: Boolean): ExitCode =
    if (updated) ExitCode.Success else ExitCode.Error

  override def run(args: List[String]): IO[ExitCode] = args match {
    case "inspect" :: limitText :: Nil =>
      scala.util.Try(limitText.toInt).toOption.filter(_ > 0) match {
        case None        => IO.println("usage: inspect <positive-limit>").as(ExitCode.Error)
        case Some(limit) =>
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
      }
    case "requeue" :: requestId :: attemptText :: Nil =>
      scala.util.Try(attemptText.toInt).toOption.filter(_ > 0) match {
        case None          => IO.println("usage: requeue <request-id> <observed-attempt-count>").as(ExitCode.Error)
        case Some(attempt) =>
          AccountSubjectId.from(requestId) match {
            case Left(_) => IO.println("invalid repair request identity").as(ExitCode.Error)
            case Right(subjectId) =>
              program { store =>
                IO.realTimeInstant.flatMap(now => store.requeueRepair(subjectId, attempt, now)).flatMap {
                  case true  => IO.println("requeued the matching repair request").as(requeueExitCode(updated = true))
                  case false =>
                    IO.println("request state, attempt count, or lease changed; no update made")
                      .as(requeueExitCode(updated = false))
                }
              }
          }
      }
    case _ =>
      IO.println("usage: inspect <positive-limit> | requeue <request-id> <observed-attempt-count>")
        .as(ExitCode.Error)
  }

  private def program(operation: MongoAnalyticsErasureWorkerStore[IO] => IO[ExitCode]): IO[ExitCode] =
    AnalyticsRuntimeConfig.loadWorker[IO].flatMap(settings => AppModule.repair[IO](settings).use(operation))
}
