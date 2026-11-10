package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.{EmbeddingPipeline, EmbeddingWork, EmbeddingWorkKeys}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** HDQ-09 restart: a worker process that dies with a claim held leaves a durable lease. A new process (new repository
  * and worker instances on the same database) must wait for the lease to end, then finish the work exactly once; it
  * must never restore data of an entity deleted in the meantime, and the displaced worker's writes stay fenced.
  */
final class MongoEmbeddingWorkRestartIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val model = "voyage-4-lite"

  /** One process worth of repositories over the shared database. */
  private final class Process(db: MongoDatabase[IO], transactions: MongoTransactionRunner) {
    val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop[IO])
    val users = new MongoUserRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop[IO])
    val jobs = new MongoJobRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop[IO])
  }

  /** The embedded entity of one kind and what a restart may or may not do to it. */
  private final case class Subject(
      key: EmbeddingWorkKey,
      sourceHash: String,
      storedEmbedding: IO[Option[EmbeddingMeta]],
      delete: IO[Unit],
      assertNothingRestored: IO[Unit]
  )

  private final class Provider(
      val calls: Ref[IO, Int],
      expired: Ref[IO, Boolean],
      val calledBeforeExpiry: Ref[IO, Boolean],
      val called: Deferred[IO, Unit],
      gate: Option[Deferred[IO, Unit]]
  ) extends EmbeddingService[IO] {
    def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      for {
        isExpired <- expired.get
        _ <- calledBeforeExpiry.set(true).whenA(!isExpired)
        _ <- calls.update(_ + 1)
        _ <- called.complete(())
        _ <- gate.traverse_(_.get)
      } yield Right(EmbeddingVector(List(0.1f), model, 1))
    def markExpired: IO[Unit] = expired.set(true)
  }

  private def provider(gate: Option[Deferred[IO, Unit]] = None): IO[Provider] =
    (Ref.of[IO, Int](0), Ref.of[IO, Boolean](false), Ref.of[IO, Boolean](false), Deferred[IO, Unit])
      .mapN(new Provider(_, _, _, _, gate))

  private def environment[A](body: (MongoDatabase[IO], () => Process) => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val transactions =
        MongoTransactionRunner.sessions(fixture.client, RepositoryError.Conflict, diagnostics = Diagnostics.noop[IO])
      body(fixture.database, () => new Process(fixture.database, transactions))
    }

  private def successful[A](result: RepositoryF[IO, A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def seed(db: MongoDatabase[IO], process: Process, candidateWork: Boolean): IO[Subject] =
    if (candidateWork) {
      val id = UserId(UUID.randomUUID())
      val profile = CandidateProfile(Set("Scala"), Some("Engineer"), None)
      val user = User(
        id,
        Some(email(s"$id@example.com")),
        "Candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(profile)),
        now
      )
      successful(process.users.insert(user)).as(
        Subject(
          EmbeddingWorkKeys.keyFor(EmbeddingWork.CandidateProfileChanged(id)),
          SourceHash.sha256(SearchableText.candidate(profile)),
          successful(process.users.findVersioned(id)).map(_.flatMap(_.value.embedding.map(_.meta))),
          successful(process.users.deleteAccount(id, now, "Deleted candidate", MutationWriteContext.directWrite)).void,
          for {
            raw <- MongoRepositoryTestSupport
              .findOne(db, MongoCollections.Users, Filters.eq(MongoFields.Id, id.value.toString))
              .flatMap(IO.fromOption(_)(new AssertionError("Missing tombstone")))
            deleted <- successful(process.users.find(id))
          } yield {
            assert(!raw.containsKey(MongoFields.Embedding))
            assert(!raw.containsKey(MongoFields.EmbeddingMeta))
            assertEquals(deleted.map(_.accountStatus), Some(AccountStatus.Deleted))
          }
        )
      )
    } else {
      val id = JobId(UUID.randomUUID())
      val job = Job(
        id,
        UserId(UUID.randomUUID()),
        "Scala Engineer",
        "Hiring infrastructure",
        List("Cats Effect"),
        Set("Scala"),
        Location("Cyprus", Some("Nicosia"), remote = true),
        JobState.Open,
        now,
        now
      )
      successful(process.jobs.createWithEvents(job, now, Nil, MutationWriteContext.directWrite)).as(
        Subject(
          EmbeddingWorkKeys.keyFor(EmbeddingWork.JobChanged(id)),
          SourceHash.sha256(SearchableText.job(job)),
          successful(process.jobs.findVersioned(id)).map(_.flatMap(_.value.embedding.map(_.meta))),
          MongoRepositoryTestSupport
            .collection(db, MongoCollections.Jobs)
            .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, id.value.toString)))
            .void,
          MongoRepositoryTestSupport
            .count(db, MongoCollections.Jobs)
            .map(count => assertEquals(count, 0L, "a restart must not recreate a deleted job"))
        )
      )
    }

  private def pipeline(process: Process, embeddings: EmbeddingService[IO]) =
    EmbeddingPipeline.resource[IO](
      process.work,
      process.users,
      process.jobs,
      embeddings,
      model,
      8,
      1,
      1,
      10.millis,
      30.seconds,
      diagnostics = Diagnostics.noop[IO]
    )

  private def storedWork(db: MongoDatabase[IO], key: EmbeddingWorkKey): IO[Option[Document]] =
    MongoRepositoryTestSupport.findOne(db, MongoCollections.EmbeddingWork, Filters.eq(MongoFields.Id, key.value))

  /** Bounded wait for the durable work row to be removed by completion. */
  private def awaitCompleted(db: MongoDatabase[IO], key: EmbeddingWorkKey, remaining: Int = 500): IO[Unit] =
    storedWork(db, key).flatMap {
      case None                     => IO.unit
      case Some(_) if remaining > 0 => IO.sleep(20.millis) *> awaitCompleted(db, key, remaining - 1)
      case Some(record)             => IO.raiseError(new AssertionError(s"Work did not complete: $record"))
    }

  /** The first process claims through the real pipeline, blocks in the provider and is cancelled: a crash. */
  private def crashInFlight(db: MongoDatabase[IO], process: Process, subject: Subject): IO[Unit] =
    for {
      forever <- Deferred[IO, Unit]
      started <- provider(Some(forever))
      worker <- pipeline(process, started).use(publisher => publisher.wake *> IO.never[Unit]).start
      _ <- successful(process.work.enqueue(subject.key, now))
      _ <- started.called.get.timeout(30.seconds)
      _ <- worker.cancel
      held <- storedWork(db, subject.key)
      embedding <- subject.storedEmbedding
    } yield {
      // The crash left the claim in place and persisted nothing.
      assertEquals(held.map(_.getString(MongoFields.State)), Some("Processing"))
      assert(held.exists(_.containsKey(MongoFields.LeaseToken)), clue(held))
      assertEquals(embedding, None)
    }

  List(false, true).foreach { candidateWork =>
    val kind = if (candidateWork) "candidate profile" else "job"

    test(
      s"HDQ-09 $kind embedding work interrupted by a crash is not stolen while the lease is live and completes exactly once after expiry"
    ) {
      environment { (db, newProcess) =>
        for {
          first <- IO(newProcess())
          subject <- seed(db, first, candidateWork)
          _ <- crashInFlight(db, first, subject)
          restarted <- IO(newProcess())
          live <- provider()
          _ <- pipeline(restarted, live).use { publisher =>
            for {
              // The restarted process polls while the dead process's lease is still live.
              _ <- List.fill(3)(publisher.wake).sequence_
              stolen <- successful(newProcess().work.claim("other", Instant.now(), 30.seconds))
              _ = assertEquals(stolen, None)
              _ <- live.markExpired
              _ <- LeaseTestSupport.expire(db, MongoCollections.EmbeddingWork, subject.key.value)
              _ <- publisher.wake
              _ <- live.called.get.timeout(30.seconds)
              _ <- awaitCompleted(db, subject.key)
            } yield ()
          }
          calls <- live.calls.get
          early <- live.calledBeforeExpiry.get
          embedding <- subject.storedEmbedding
        } yield {
          assertEquals(calls, 1)
          assert(!early, "the provider was called before the dead worker's lease expired")
          assertEquals(embedding.map(_.sourceHash), Some(subject.sourceHash))
        }
      }
    }

    test(s"HDQ-09 $kind embedding work of an entity deleted before the restart completes without restoring any data") {
      environment { (db, newProcess) =>
        for {
          first <- IO(newProcess())
          subject <- seed(db, first, candidateWork)
          _ <- crashInFlight(db, first, subject)
          _ <- subject.delete
          _ <- LeaseTestSupport.expire(db, MongoCollections.EmbeddingWork, subject.key.value)
          restarted <- provider()
          restartedProcess <- IO(newProcess())
          _ <- pipeline(restartedProcess, restarted).use { publisher =>
            publisher.wake *> awaitCompleted(db, subject.key)
          }
          calls <- restarted.calls.get
          _ <- subject.assertNothingRestored
        } yield assertEquals(calls, 0, "no provider call and no write for a deleted entity")
      }
    }
  }

  test(
    "HDQ-09 a displaced worker's completion, retry and failure stay fenced while the restarted worker holds the claim"
  ) {
    environment { (db, newProcess) =>
      for {
        first <- IO(newProcess())
        subject <- seed(db, first, candidateWork = false)
        _ <- successful(first.work.enqueue(subject.key, now))
        crashed <- successful(first.work.claim("crashed-worker", Instant.now(), 30.seconds))
          .flatMap(IO.fromOption(_)(new AssertionError("Expected claim")))
        _ <- LeaseTestSupport.expire(db, MongoCollections.EmbeddingWork, subject.key.value)
        release <- Deferred[IO, Unit]
        blocked <- provider(Some(release))
        restarted <- IO(newProcess())
        result <- pipeline(restarted, blocked).use { publisher =>
          for {
            _ <- publisher.wake
            _ <- blocked.called.get.timeout(30.seconds)
            before <- storedWork(db, subject.key).map(_.get)
            late <- IO(newProcess())
            complete <- late.work.complete(crashed).value
            retry <- late.work.retry(crashed, Instant.now()).value
            fail <- late.work.fail(crashed, EmbeddingWorkFailure.RetryExhausted, Instant.now()).value
            renewed <- successful(late.work.renew(crashed, 30.seconds))
            after <- storedWork(db, subject.key).map(_.get)
            _ <- release.complete(())
            _ <- awaitCompleted(db, subject.key)
          } yield (before, after, List(complete, retry, fail), renewed)
        }
        (before, after, stale, renewed) = result
        embedding <- subject.storedEmbedding
      } yield {
        stale.foreach(outcome => assertEquals(outcome, Left(RepositoryError.Conflict)))
        assert(!renewed)
        assertNotEquals(before.getString(MongoFields.LeaseToken), crashed.leaseToken)
        assertEquals(after.getString(MongoFields.LeaseToken), before.getString(MongoFields.LeaseToken))
        assertEquals(after.getString(MongoFields.LeaseOwner), before.getString(MongoFields.LeaseOwner))
        assertEquals(after.getString(MongoFields.State), "Processing")
        assertEquals(embedding.map(_.sourceHash), Some(subject.sourceHash))
      }
    }
  }
}
