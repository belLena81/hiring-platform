package com.example.hiring.analytics.erasure

import com.example.hiring.analytics.AnalyticsRuntimeConfig
import com.example.hiring.analytics.mongo.MongoAnalyticsErasureWorkerStore

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients}

/** Local operator utility. It exposes only fixed failure labels and requires an observed attempt count to requeue. */
object AnalyticsErasureRepairMain extends IOApp {
  private[analytics] def requeueExitCode(updated: Boolean): ExitCode =
    if (updated) ExitCode.Success else ExitCode.Error

  private def mongoClient(uri: String): Resource[IO, MongoClient] =
    Resource.make(IO.blocking(MongoClients.create(uri)))(client => IO.blocking(client.close()))

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
                    s"request=${request.requestId} phase=${request.phase} attempts=${request.attemptCount} category=${request.failureCategory}"
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
          program { store =>
            IO.realTimeInstant.flatMap(now => store.requeueRepair(requestId, attempt, now)).flatMap {
              case true  => IO.println("requeued the matching repair request").as(requeueExitCode(updated = true))
              case false =>
                IO.println("request state, attempt count, or lease changed; no update made")
                  .as(requeueExitCode(updated = false))
            }
          }
      }
    case _ =>
      IO.println("usage: inspect <positive-limit> | requeue <request-id> <observed-attempt-count>").as(ExitCode.Error)
  }

  private def program(operation: MongoAnalyticsErasureWorkerStore => IO[ExitCode]): IO[ExitCode] =
    AnalyticsRuntimeConfig.loadWorker.flatMap { settings =>
      mongoClient(settings.common.mongoUri).use { client =>
        IO.blocking(client.getDatabase(settings.common.mongoDatabase)).flatMap { database =>
          operation(new MongoAnalyticsErasureWorkerStore(client, database))
        }
      }
    }
}
