package com.example.graphQL.cats.service.events

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import io.circe.Json
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class SearchSessionHandoffSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-22T08:00:00Z")
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000901"))
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000902")

  test("search-session handoff allows the configured final processing attempt") {
    for {
      retried <- Ref.of[IO, Vector[Int]](Vector.empty)
      failed <- Ref.of[IO, Vector[(Int, SearchSessionWorkFailure)]](Vector.empty)
      claimed <- Ref.of[IO, Int](0)
      terminal <- Deferred[IO, Unit]
      repository = failingRepository(claimed, retried, failed, terminal)
      result <- SearchSessionHandoff.resource(
        repository,
        SearchSessionHandoffConfig(parallelism = 1, retryDelay = 1.millis, pollInterval = 1.hour),
        Diagnostics.noop
      ).use(_ => terminal.get *> (retried.get, failed.get).tupled)
    } yield {
      assertEquals(result._1, Vector(1, 2))
      assertEquals(result._2, Vector(3 -> SearchSessionWorkFailure.RetryExhausted))
    }
  }

  private def failingRepository(
      claimed: Ref[IO, Int],
      retried: Ref[IO, Vector[Int]],
      failed: Ref[IO, Vector[(Int, SearchSessionWorkFailure)]],
      terminal: Deferred[IO, Unit]
  ): SearchSessionWorkRepository = {
    val session = SearchSession(
      searchId,
      actorId,
      "semanticJobSearch",
      Some("scala"),
      Json.obj("city" -> Json.fromString("Nicosia")),
      Some("test-model"),
      List(SearchSessionResult("job-1", 1, 0.9d)),
      now,
      now.plusSeconds(3600)
    )
    val work = PendingSearchSessionWork(
      session,
      OperationalEvents.searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000903"), session)
    )

    new SearchSessionWorkRepository {
      override def enqueue(work: PendingSearchSessionWork, createdAt: Instant): IO[Either[RepositoryError, Unit]] =
        IO.pure(Right(()))

      override def findForActor(actor: UserId, search: UUID): IO[Either[RepositoryError, Option[SearchSessionLookup]]] =
        IO.pure(Right(None))

      override def claim(workerId: String, currentTime: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedSearchSessionWork]]] =
        claimed.modify { attempt =>
          val next = attempt + 1
          if (next <= 3) (next, Right(Some(ClaimedSearchSessionWork(work, next, s"lease-$next"))))
          else (attempt, Right(None))
        }

      override def complete(claim: ClaimedSearchSessionWork, completedAt: Instant): IO[Either[RepositoryError, Unit]] =
        IO.pure(Left(RepositoryError.Unavailable))

      override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): IO[Either[RepositoryError, Unit]] =
        retried.update(_ :+ claim.attempts).as(Right(()))

      override def fail(
          claim: ClaimedSearchSessionWork,
          failure: SearchSessionWorkFailure,
          failedAt: Instant
      ): IO[Either[RepositoryError, Unit]] =
        failed.update(_ :+ (claim.attempts -> failure)) *> terminal.complete(()).as(Right(()))
    }
  }
}
