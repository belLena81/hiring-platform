package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.*
import io.circe.Json
import org.bson.Document
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

class MongoSearchSessionRetryIntegrationSpec extends MongoIntegrationSuite {
  test("worker exhausts three executions using Mongo prior-failure counters") {
    mongoResource.use { fixture =>
      val now = Instant.now()
      val session = SearchSession(
        UUID.randomUUID(),
        UserId(UUID.randomUUID()),
        "jobs",
        None,
        Json.obj(),
        None,
        Nil,
        now,
        now.plusSeconds(3600)
      )
      val event =
        OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
      val underlying =
        MongoSearchSessionWorkRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- underlying.enqueue(PendingSearchSessionWork(session, event), now).value.flatMap(result)
        executions <- Ref.of[IO, Vector[Int]](Vector.empty)
        terminal <- Deferred[IO, Unit]
        repository = new SearchSessionWorkRepository {
          def enqueue(work: PendingSearchSessionWork, at: Instant): RepositoryIO[Unit] = underlying.enqueue(work, at)
          def findForActor(actor: UserId, id: UUID): RepositoryIO[Option[SearchSessionLookup]] =
            underlying.findForActor(actor, id)
          def claim(worker: String, at: Instant, until: Instant): RepositoryIO[Option[ClaimedSearchSessionWork]] =
            underlying.claim(worker, at, until)
          def complete(claim: ClaimedSearchSessionWork, at: Instant): RepositoryIO[Unit] =
            RepositoryIO.fromIOEither(executions.update(_ :+ claim.attempts).as(Left(RepositoryError.Unavailable)))
          def retry(claim: ClaimedSearchSessionWork, at: Instant): RepositoryIO[Unit] = underlying.retry(claim, at)
          def fail(claim: ClaimedSearchSessionWork, reason: SearchSessionWorkFailure, at: Instant): RepositoryIO[Unit] =
            underlying.fail(claim, reason, at).semiflatTap(_ => terminal.complete(()).void)
        }
        _ <- SearchSessionHandoff
          .resource(
            repository,
            SearchSessionHandoffConfig(parallelism = 1, retryDelay = 1.millis, pollInterval = 5.millis),
            Diagnostics.noop
          )
          .use(_ => terminal.get.timeout(20.seconds))
        observed <- executions.get
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.SearchSessionWork,
          new Document("_id", session.id.toString)
        )
      } yield {
        assertEquals(observed, Vector(0, 1, 2))
        assertEquals(stored.map(_.getString("state")), Some("Failed"))
        assertEquals(stored.map(_.getInteger("attempts").intValue), Some(2))
      }
    }
  }

  test("expired claims recover with unchanged prior failures and reject stale transition writes") {
    mongoResource.use { fixture =>
      val now = Instant.now()
      val session = SearchSession(
        UUID.randomUUID(),
        UserId(UUID.randomUUID()),
        "jobs",
        None,
        Json.obj(),
        None,
        Nil,
        now,
        now.plusSeconds(3600)
      )
      val event =
        OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
      val repository =
        MongoSearchSessionWorkRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- repository.enqueue(PendingSearchSessionWork(session, event), now).value.flatMap(result)
        first <- repository
          .claim("first", now, now.plusSeconds(1))
          .value
          .flatMap(result)
          .map(_.getOrElse(fail("Missing initial claim")))
        recovered <- repository
          .claim("second", now.plusSeconds(2), now.plusSeconds(30))
          .value
          .flatMap(result)
          .map(_.getOrElse(fail("Missing recovered claim")))
        staleRetry <- repository.retry(first, now.plusSeconds(3)).value
        staleFailure <- repository.fail(first, SearchSessionWorkFailure.RetryExhausted, now.plusSeconds(3)).value
        _ <- repository.retry(recovered, now.plusSeconds(3)).value.flatMap(result)
        next <- repository
          .claim("second", now.plusSeconds(4), now.plusSeconds(40))
          .value
          .flatMap(result)
          .map(_.getOrElse(fail("Missing retry claim")))
      } yield {
        assertEquals(first.attempts, 0)
        assertEquals(recovered.attempts, 0)
        assertNotEquals(recovered.leaseToken, first.leaseToken)
        assertEquals(staleRetry, Left(RepositoryError.Conflict))
        assertEquals(staleFailure, Left(RepositoryError.Conflict))
        assertEquals(next.attempts, 1)
      }
    }
  }

  private def result[A](value: Either[RepositoryError, A]): IO[A] =
    value.fold(error => IO.raiseError(new AssertionError(error.toString)), IO.pure)
}
