package com.example.graphQL.cats.service.events

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.port.*
import io.circe.Json
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** EPS-05.1: the handoff loop sleeps pollInterval after a recovery-deferred outcome and after an empty claim. */
final class SearchSessionHandoffTimingSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-22T08:00:00Z")
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000912")
  private val session = SearchSession(
    searchId,
    UserId(UUID.fromString("00000000-0000-0000-0000-000000000911")),
    SearchKind.SemanticJobSearch,
    None,
    Json.obj(),
    None,
    List(SearchSessionResult("00000000-0000-0000-0000-000000000914", 1, 0.9d)),
    now,
    now.plusSeconds(3600)
  )
  private val work = PendingSearchSessionWork(
    session,
    OperationalEvents
      .searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000913"), session)
      .fold(error => fail(error.toString), identity)
  )

  /** The first claim returns work whose completion and retry both fail (recovery deferred); later claims are empty. */
  private def repository(claimedAt: Ref[IO, Vector[FiniteDuration]]): SearchSessionWorkRepository[IO] =
    new SearchSessionWorkRepository[IO] {
      private def unavailable[A]: RepositoryF[IO, A] = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
      override def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryF[IO, Unit] = unavailable
      override def findForActor(actorId: UserId, searchId: UUID): RepositoryF[IO, Option[SearchSessionLookup]] =
        unavailable
      override def claim(
          workerId: String,
          now: Instant,
          leaseUntil: Instant
      ): RepositoryF[IO, Option[ClaimedSearchSessionWork]] =
        RepositoryF.lift(
          IO.monotonic.flatMap(at =>
            claimedAt.modify(times =>
              (times :+ at, if (times.isEmpty) Some(ClaimedSearchSessionWork(work, 0, "lease")) else None)
            )
          )
        )
      override def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryF[IO, Unit] = unavailable
      override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[IO, Unit] = unavailable
      override def fail(
          claim: ClaimedSearchSessionWork,
          failure: SearchSessionWorkFailure,
          now: Instant
      ): RepositoryF[IO, Unit] =
        unavailable
    }

  test("EPS-05.1 the worker polls again only after pollInterval, following a deferred recovery and an empty claim") {
    val program = for {
      claimedAt <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      _ <- SearchSessionHandoff
        .resource[IO](
          repository(claimedAt),
          SearchSessionHandoffConfig(parallelism = 1, pollInterval = 10.seconds),
          Diagnostics.noop[IO]
        )
        .use(_ => IO.sleep(35.seconds))
      times <- claimedAt.get
    } yield times

    TestControl
      .executeEmbed(program)
      .map(times => assertEquals(times, Vector(0.seconds, 10.seconds, 20.seconds, 30.seconds)))
  }
}
