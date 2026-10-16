package com.example.graphQL.cats.service.events

import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.{OperationalEvents, SearchSession, SearchSessionResult}
import java.time.Instant
import java.util.UUID
import cats.effect.testkit.TestControl
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
      result <- SearchSessionHandoff
        .resource[IO](
          repository,
          SearchSessionHandoffConfig(parallelism = 1, retryDelay = 1.millis, pollInterval = 1.hour),
          Diagnostics.noop
        )
        .use(_ => terminal.get *> (retried.get, failed.get).tupled)
    } yield {
      assertEquals(result._1, Vector(0, 1))
      assertEquals(result._2, Vector(2 -> SearchSessionWorkFailure.RetryExhausted))
    }
  }

  List(RepositoryError.Conflict, RepositoryError.Unavailable).foreach { error =>
    test(s"failed retry persistence reports $error without claiming success") {
      for {
        retried <- Ref.of[IO, Vector[Int]](Vector.empty)
        failed <- Ref.of[IO, Vector[(Int, SearchSessionWorkFailure)]](Vector.empty)
        claimed <- Ref.of[IO, Int](0)
        terminal <- Deferred[IO, Unit]
        repository = failingRepository(claimed, retried, failed, terminal, Some(error))
        claim <- repository.claim("worker", now, now.plusSeconds(30)).value.flatMap {
          case Right(Some(value)) => IO.pure(value)
          case other              => IO.raiseError(new AssertionError(other.toString))
        }
        outcome <- SearchSessionHandoff.processClaim(repository, SearchSessionHandoffConfig(), claim, now)
      } yield assertEquals(
        outcome,
        if (error == RepositoryError.Conflict) SearchSessionHandoff.WorkOutcome.OwnershipLost
        else SearchSessionHandoff.WorkOutcome.RecoveryDeferred(error)
      )
    }
    test(s"failed terminal persistence reports $error and preserves lease recovery") {
      for {
        retried <- Ref.of[IO, Vector[Int]](Vector.empty)
        failed <- Ref.of[IO, Vector[(Int, SearchSessionWorkFailure)]](Vector.empty)
        claimed <- Ref.of[IO, Int](0)
        terminal <- Deferred[IO, Unit]
        repository = failingRepository(claimed, retried, failed, terminal, Some(error))
        claim <- repository.claim("worker", now, now.plusSeconds(30)).value.flatMap {
          case Right(Some(value)) => IO.pure(value)
          case other              => IO.raiseError(new AssertionError(other.toString))
        }
        outcome <- SearchSessionHandoff.processClaim(
          repository,
          SearchSessionHandoffConfig(maxAttempts = 1),
          claim,
          now
        )
        retries <- retried.get
      } yield {
        assertEquals(retries, Vector.empty)
        assertEquals(
          outcome,
          if (error == RepositoryError.Conflict) SearchSessionHandoff.WorkOutcome.OwnershipLost
          else SearchSessionHandoff.WorkOutcome.RecoveryDeferred(error)
        )
      }
    }
  }

  test("search-session workers claim with the injected clock and the configured lease") {
    for {
      observed <- Deferred[IO, (Instant, Instant)]
      repository = new SearchSessionWorkRepository[IO] {
        override def enqueue(work: PendingSearchSessionWork, createdAt: Instant): RepositoryF[IO, Unit] =
          RepositoryF.at[IO].pure(())
        override def findForActor(actor: UserId, search: UUID): RepositoryF[IO, Option[SearchSessionLookup]] =
          RepositoryF.at[IO].pure(None)
        override def claim(
            workerId: String,
            currentTime: Instant,
            leaseUntil: Instant
        ): RepositoryF[IO, Option[ClaimedSearchSessionWork]] =
          RepositoryF.lift(observed.complete((currentTime, leaseUntil)).as(None))
        override def complete(claim: ClaimedSearchSessionWork, completedAt: Instant): RepositoryF[IO, Unit] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[IO, Unit] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        override def fail(
            claim: ClaimedSearchSessionWork,
            failure: SearchSessionWorkFailure,
            failedAt: Instant
        ): RepositoryF[IO, Unit] = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
      }
      result <- SearchSessionHandoff
        .resource[IO](
          repository,
          SearchSessionHandoffConfig(parallelism = 1, lease = 45.seconds, pollInterval = 1.hour),
          Diagnostics.noop,
          FixedTestClock.at(now)
        )
        .use(_ => observed.get)
    } yield assertEquals(result, (now, now.plusSeconds(45)))
  }

  private def beats(unavailable: Boolean): IO[Int] =
    for {
      retried <- Ref.of[IO, Vector[Int]](Vector.empty)
      failed <- Ref.of[IO, Vector[(Int, SearchSessionWorkFailure)]](Vector.empty)
      // Three claims already used, so the base repository reports an empty queue.
      claimed <- Ref.of[IO, Int](3)
      terminal <- Deferred[IO, Unit]
      base = failingRepository(claimed, retried, failed, terminal)
      repository = new SearchSessionWorkRepository[IO] {
        def enqueue(work: PendingSearchSessionWork, createdAt: Instant) = base.enqueue(work, createdAt)
        def findForActor(actor: UserId, search: UUID) = base.findForActor(actor, search)
        def claim(workerId: String, currentTime: Instant, leaseUntil: Instant) =
          if (unavailable) RepositoryF.raise[IO, Option[ClaimedSearchSessionWork]](RepositoryError.Unavailable)
          else base.claim(workerId, currentTime, leaseUntil)
        def complete(claim: ClaimedSearchSessionWork, completedAt: Instant) = base.complete(claim, completedAt)
        def retry(claim: ClaimedSearchSessionWork, availableAt: Instant) = base.retry(claim, availableAt)
        def fail(claim: ClaimedSearchSessionWork, failure: SearchSessionWorkFailure, failedAt: Instant) =
          base.fail(claim, failure, failedAt)
      }
      count <- Ref.of[IO, Int](0)
      _ <- SearchSessionHandoff
        .resource[IO](
          repository,
          SearchSessionHandoffConfig(parallelism = 1, pollInterval = 1.second),
          Diagnostics.noop,
          alive = count.update(_ + 1)
        )
        .surround(IO.sleep(10.seconds))
      total <- count.get
    } yield total

  test("ARR-07.1 the search-session handoff reports progress on every pass that reached the store") {
    TestControl.executeEmbed(beats(unavailable = false)).map(count => assert(count >= 9, clue(count)))
  }

  test("ARR-07.1 the search-session handoff reports no progress while its store is unavailable") {
    TestControl.executeEmbed(beats(unavailable = true)).map(count => assertEquals(count, 0))
  }

  private def failingRepository(
      claimed: Ref[IO, Int],
      retried: Ref[IO, Vector[Int]],
      failed: Ref[IO, Vector[(Int, SearchSessionWorkFailure)]],
      terminal: Deferred[IO, Unit],
      transitionError: Option[RepositoryError] = None
  ): SearchSessionWorkRepository[IO] = {
    val session = SearchSession(
      searchId,
      actorId,
      SearchKind.SemanticJobSearch,
      Some("scala"),
      SearchFilterSnapshot.JobCriteria(Some("Nicosia"), Nil, None),
      Some("test-model"),
      List(SearchSessionResult("00000000-0000-0000-0000-000000000904", 1, 0.9d)),
      now,
      now.plusSeconds(3600)
    )
    val work = PendingSearchSessionWork(
      session,
      OperationalEvents
        .searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000903"), session)
        .fold(error => fail(error.toString), identity)
    )

    new SearchSessionWorkRepository[IO] {
      override def enqueue(work: PendingSearchSessionWork, createdAt: Instant): RepositoryF[IO, Unit] =
        com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(())))

      override def findForActor(actor: UserId, search: UUID): RepositoryF[IO, Option[SearchSessionLookup]] =
        com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Right(None)))

      override def claim(
          workerId: String,
          currentTime: Instant,
          leaseUntil: Instant
      ): RepositoryF[IO, Option[ClaimedSearchSessionWork]] =
        com.example.graphQL.cats.service.port.RepositoryF.fromFEither(claimed.modify { attempt =>
          val next = attempt + 1
          if (next <= 3) (next, Right(Some(ClaimedSearchSessionWork(work, attempt, s"lease-$next"))))
          else (attempt, Right(None))
        })

      override def complete(claim: ClaimedSearchSessionWork, completedAt: Instant): RepositoryF[IO, Unit] =
        com.example.graphQL.cats.service.port.RepositoryF.fromFEither(IO.pure(Left(RepositoryError.Unavailable)))

      override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[IO, Unit] =
        com.example.graphQL.cats.service.port.RepositoryF
          .fromFEither(retried.update(_ :+ claim.attempts).as(transitionError.toLeft(())))

      override def fail(
          claim: ClaimedSearchSessionWork,
          failure: SearchSessionWorkFailure,
          failedAt: Instant
      ): RepositoryF[IO, Unit] = com.example.graphQL.cats.service.port.RepositoryF.fromFEither(
        failed.update(_ :+ (claim.attempts -> failure)) *> terminal.complete(()).as(transitionError.toLeft(()))
      )
    }
  }

  test("ARR-07.1 every repository error is classified, and an overloaded store reads like an unavailable one") {
    val runtime = "java.lang.RuntimeException"
    assertEquals(SearchSessionHandoff.errorType(RepositoryError.Overloaded), runtime)
    assertEquals(SearchSessionHandoff.errorType(RepositoryError.Unavailable), runtime)
    assertEquals(SearchSessionHandoff.errorType(RepositoryError.VectorSearchUnavailable), runtime)
    assertEquals(SearchSessionHandoff.errorType(RepositoryError.Conflict), "java.lang.IllegalStateException")
    assertEquals(
      RepositoryError.values
        .map(SearchSessionHandoff.errorType)
        .toSet
        .subsetOf(Set(runtime, "java.lang.IllegalStateException")),
      true
    )
  }
}
