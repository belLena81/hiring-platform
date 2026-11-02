package com.example.graphQL.cats.acceptance

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.mongodb.client.model.{Filters, Projections, Sorts}
import mongo4cats.database.MongoDatabase
import scala.concurrent.duration.*

/** Test-harness diagnostics for the operational event outbox (not production code).
  *
  * When `HIRING_OUTBOX_DIAGNOSTICS` is set or the ignored file `.local/data/outbox-diagnostics` exists, a background
  * sampler prints the outbox rows that are not yet `Published` (state, attempts, last error, due time, lease fields,
  * partition key) and the count per state whenever that picture changes, with the elapsed time. It only reads the story
  * database, never writes, and is silent by default.
  */
object OutboxDiagnostics {
  // The marker file lets the wrapper-driven run enable it without exporting environment variables.
  private val enabled: Boolean =
    sys.env.contains("HIRING_OUTBOX_DIAGNOSTICS") ||
      java.nio.file.Files.exists(java.nio.file.Path.of(".local/data/outbox-diagnostics"))
  private val MaxRows = 12

  def resource(database: MongoDatabase[IO]): Resource[IO, Unit] =
    if (!enabled) Resource.unit[IO]
    else
      Resource
        .make(IO.monotonic.flatMap(start => loop(database, start, "").start))(_.cancel)
        .void

  private def loop(database: MongoDatabase[IO], start: FiniteDuration, previous: String): IO[Unit] =
    sample(database).attempt.flatMap { result =>
      val text = result.fold(error => s"sample failed: ${error.getClass.getSimpleName}", identity)
      val emit =
        if (text == previous) IO.unit
        else IO.monotonic.map(now => println(s"OUTBOX[t+${(now - start).toSeconds}s] $text"))
      emit *> IO.sleep(2.seconds) *> loop(database, start, text)
    }

  private val States = List("Retryable", "InFlight", "Published", "Failed")

  private def sample(database: MongoDatabase[IO]): IO[String] =
    for {
      collection <- database.getCollection("event_outbox")
      counts <- States.traverse(state => collection.count(Filters.eq("state", state)).map(state -> _))
      open <- collection
        .find(Filters.ne("state", "Published"))
        .projection(
          Projections.include(
            "_id",
            "eventType",
            "aggregateType",
            "state",
            "attempts",
            "availableAt",
            "occurredAt",
            "leaseUntil"
          )
        )
        .sort(Sorts.ascending("occurredAt", "_id"))
        .limit(MaxRows)
        .all
    } yield s"[${counts.map { case (state, n) => s"$state=$n" }.mkString(",")}] ${open.map(_.toJson).mkString(" ")}"
}
