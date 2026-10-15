package com.example.graphQL.cats.service.events

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, RepositoryError}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.port.*
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
    SearchFilterSnapshot.Unfiltered,
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

  /** ARR-06.1: the first claim raises in F; the loop logs, backs off for pollInterval and then completes the work. */
  test("ARR-06.1 a thrown claim failure is logged and the worker resumes after backoff to process the next item") {
    val program = for {
      claimedAt <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      completedAt <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      events <- Ref.of[IO, Vector[LogEvent]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]) =
          events.update(_ :+ event)
      }
      base = repository(claimedAt)
      throwing = new SearchSessionWorkRepository[IO] {
        override def enqueue(work: PendingSearchSessionWork, now: Instant) = base.enqueue(work, now)
        override def findForActor(actorId: UserId, searchId: UUID) = base.findForActor(actorId, searchId)
        override def claim(workerId: String, now: Instant, leaseUntil: Instant) =
          RepositoryF.lift(
            for {
              at <- IO.monotonic
              previous <- claimedAt.getAndUpdate(_ :+ at)
              done <- completedAt.get
              claimed <-
                if (previous.isEmpty) IO.raiseError[Option[ClaimedSearchSessionWork]](new IllegalStateException("boom"))
                else IO.pure(Option.when(done.isEmpty)(ClaimedSearchSessionWork(work, 0, "lease")))
            } yield claimed
          )
        override def complete(claim: ClaimedSearchSessionWork, now: Instant) =
          RepositoryF.lift(IO.monotonic.flatMap(at => completedAt.update(_ :+ at)))
        override def retry(claim: ClaimedSearchSessionWork, availableAt: Instant) = base.retry(claim, availableAt)
        override def fail(claim: ClaimedSearchSessionWork, failure: SearchSessionWorkFailure, now: Instant) =
          base.fail(claim, failure, now)
      }
      _ <- SearchSessionHandoff
        .resource[IO](throwing, SearchSessionHandoffConfig(parallelism = 1, pollInterval = 10.seconds), diagnostics)
        .use(_ => IO.sleep(15.seconds))
      done <- completedAt.get
      logged <- events.get
    } yield (done, logged)

    TestControl.executeEmbed(program).map { case (done, logged) =>
      assertEquals(done, Vector(10.seconds))
      assertEquals(logged, Vector(LogEvent.RuntimeFailed))
    }
  }

  test("ARR-06.1 releasing the handoff resource stops the worker loop") {
    val program = for {
      claimedAt <- Ref.of[IO, Vector[FiniteDuration]](Vector.empty)
      _ <- SearchSessionHandoff
        .resource[IO](
          repository(claimedAt),
          SearchSessionHandoffConfig(parallelism = 1, pollInterval = 10.seconds),
          Diagnostics.noop[IO]
        )
        .use(_ => IO.sleep(5.seconds))
      afterRelease <- claimedAt.get
      _ <- IO.sleep(120.seconds)
      later <- claimedAt.get
    } yield (afterRelease.size, later.size)

    TestControl.executeEmbed(program).map { case (afterRelease, later) => assertEquals(later, afterRelease) }
  }
}
