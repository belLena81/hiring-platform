package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource, Deferred, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

final class MongoEmbeddingWorkIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private def database: Resource[IO, (MongoDatabase[IO], MongoTransactionRunner)] =
    mongoResource.map { fixture =>
      fixture.database -> MongoTransactionRunner.sessions(
        fixture.client,
        RepositoryError.Conflict,
        diagnostics = Diagnostics.noop
      )
    }

  private def successful[A](result: RepositoryIO[A]): IO[A] =
    result.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failure: $error")), IO.pure))

  private def claimed(repository: EmbeddingWorkRepository, at: Instant): IO[ClaimedEmbeddingWork] =
    successful(repository.claim("worker", at, at.plusSeconds(30)))
      .flatMap(IO.fromOption(_)(new AssertionError("Expected durable claim")))

  private def stored(db: MongoDatabase[IO], key: EmbeddingWorkKey): IO[Document] =
    MongoRepositoryTestSupport
      .findOne(db, MongoCollections.EmbeddingWork, Filters.eq(MongoFields.Id, key.value))
      .flatMap(IO.fromOption(_)(new AssertionError("Expected stored work")))

  test("deletion removes candidate embeddings and rejects stale and fresh deleted-account writes") {
    database.use { case (db, transactions) =>
      val users = new MongoUserRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
      val id = UserId(UUID.randomUUID())
      val embedding = EntityEmbedding(List(0.1f), EmbeddingMeta("voyage-4-lite", "candidate-source", now))
      val user = User(
        id,
        Some(email(s"$id@example.com")),
        "Candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Engineer"), None))),
        now,
        embedding = Some(embedding)
      )
      for {
        _ <- successful(users.insert(user))
        before <- successful(users.findVersioned(id)).flatMap(IO.fromOption(_)(new AssertionError("Missing candidate")))
        _ <- successful(users.deleteAccount(id, now, "Deleted candidate", MutationWriteContext.directWrite))
        raw <- MongoRepositoryTestSupport
          .findOne(db, MongoCollections.Users, Filters.eq(MongoFields.Id, id.value.toString))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing tombstone")))
        after <- successful(users.findVersioned(id)).flatMap(IO.fromOption(_)(new AssertionError("Missing tombstone")))
        stale <- users.updateEmbedding(before, embedding).value
        fresh <- users.updateEmbedding(after, embedding).value
      } yield {
        assert(!raw.containsKey(MongoFields.Embedding))
        assert(!raw.containsKey(MongoFields.EmbeddingMeta))
        assertEquals(after.value.accountStatus, AccountStatus.Deleted)
        assertEquals(stale, Left(RepositoryError.Conflict))
        assertEquals(fresh, Left(RepositoryError.Conflict))
      }
    }
  }

  test("deletion during an in-flight provider call leaves a raw embedding-free tombstone") {
    database.use { case (db, transactions) =>
      val users = new MongoUserRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
      val jobs = new MongoJobRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val id = UserId(UUID.randomUUID())
      val user = User(
        id,
        None,
        "Candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
        now
      )
      val key = EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, id.value.toString)
      for {
        _ <- successful(users.insert(user))
        _ <- successful(work.enqueue(key, now))
        started <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        provider = new EmbeddingService {
          def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
            (started.complete(()).void *> release.get).as(Right(EmbeddingVector(List(0.1f), "voyage-4-lite", 1)))
        }
        _ <- com.example.graphQL.cats.service.search.EmbeddingPipeline
          .resource(
            work,
            users,
            jobs,
            provider,
            "voyage-4-lite",
            8,
            1,
            1,
            10.millis,
            30.seconds,
            diagnostics = Diagnostics.noop
          )
          .use { publisher =>
            publisher.wake *> started.get *>
              successful(users.deleteAccount(id, now, "Deleted candidate", MutationWriteContext.directWrite)) *>
              release.complete(()).void *> awaitAbsent(db, key)
          }
        raw <- MongoRepositoryTestSupport
          .findOne(db, MongoCollections.Users, Filters.eq(MongoFields.Id, id.value.toString))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing tombstone")))
        deleted <- successful(users.find(id)).flatMap(IO.fromOption(_)(new AssertionError("Missing tombstone")))
      } yield {
        assertEquals(deleted.accountStatus, AccountStatus.Deleted)
        assert(!raw.containsKey(MongoFields.Embedding))
        assert(!raw.containsKey(MongoFields.EmbeddingMeta))
      }
    }
  }

  test("in-flight embeddings recover after real interview scheduling fences candidate and job revisions") {
    List(false, true).traverse_ { candidateWork =>
      mongoResource.use { fixture =>
        val at = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
        val candidateId = UserId(UUID.randomUUID())
        val recruiterId = UserId(UUID.randomUUID())
        val starts = at.plusSeconds(172800)
        val ends = starts.plusSeconds(3600)
        val workflow = com.example.graphQL.cats.domain.workflow.InterviewWorkflow
          .create(
            com.example.graphQL.cats.domain.workflow.InterviewWorkflowId(UUID.randomUUID()),
            com.example.graphQL.cats.domain.model.Identifiers.ApplicationId(UUID.randomUUID()),
            candidateId,
            recruiterId,
            com.example.graphQL.cats.domain.workflow.InterviewInterval(starts, ends),
            at.plusSeconds(300),
            UUID.randomUUID(),
            ApplicationStatus.Accepted
          )
          .fold(error => fail(s"Invalid workflow: $error"), identity)
        val users = MongoUserRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val jobs = MongoJobRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        val workflows = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        val scheduling = new com.example.graphQL.cats.service.application.InterviewSchedulingService(
          users,
          jobs,
          applications,
          workflows,
          5.minutes
        )
        val work = new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop)
        for {
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
          _ <- InterviewSchedulingFixtures.seed(fixture.database, List(workflow), at)
          application <- successful(applications.find(workflow.applicationId))
            .flatMap(IO.fromOption(_)(new AssertionError("Missing application")))
          beforeUser <- successful(users.findVersioned(candidateId))
            .flatMap(IO.fromOption(_)(new AssertionError("Missing candidate")))
          beforeJob <- successful(jobs.findVersioned(application.jobId))
            .flatMap(IO.fromOption(_)(new AssertionError("Missing job")))
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          calls <- Ref.of[IO, Int](0)
          provider = new EmbeddingService {
            def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
              calls
                .updateAndGet(_ + 1)
                .flatMap(count =>
                  (if (count == 1) started.complete(()).void *> release.get else IO.unit)
                    .as(Right(EmbeddingVector(List(0.1f), "voyage-4-lite", 1)))
                )
          }
          changed =
            if (candidateWork)
              com.example.graphQL.cats.service.search.EmbeddingWork.CandidateProfileChanged(candidateId)
            else com.example.graphQL.cats.service.search.EmbeddingWork.JobChanged(application.jobId)
          key = com.example.graphQL.cats.service.search.DurableEmbeddingWorkPublisher.keyFor(changed)
          _ <- successful(work.enqueue(key, at))
          _ <- com.example.graphQL.cats.service.search.EmbeddingPipeline
            .resource(
              work,
              users,
              jobs,
              provider,
              "voyage-4-lite",
              8,
              1,
              1,
              10.millis,
              30.seconds,
              diagnostics = Diagnostics.noop,
              durableRetryBase = 10.millis,
              durableRetryCap = 20.millis
            )
            .use { publisher =>
              for {
                _ <- publisher.wake
                _ <- started.get
                scheduled <- scheduling
                  .schedule(
                    com.example.graphQL.cats.service.ActorContext(recruiterId, UserRole.Recruiter),
                    workflow.applicationId,
                    starts,
                    ends,
                    UUID.randomUUID()
                  )
                  .value
                _ = assert(scheduled.isRight, s"Scheduling failed: $scheduled")
                fencedUser <- successful(users.findVersioned(candidateId))
                  .flatMap(IO.fromOption(_)(new AssertionError("Missing candidate")))
                fencedJob <- successful(jobs.findVersioned(application.jobId))
                  .flatMap(IO.fromOption(_)(new AssertionError("Missing job")))
                _ = assert(fencedUser.version > beforeUser.version)
                _ = assert(fencedJob.version > beforeJob.version)
                _ <- release.complete(())
                _ <- awaitAbsent(fixture.database, key)
              } yield ()
            }
          count <- calls.get
          user <- successful(users.find(candidateId)).flatMap(IO.fromOption(_)(new AssertionError("Missing candidate")))
          job <- successful(jobs.find(application.jobId)).flatMap(IO.fromOption(_)(new AssertionError("Missing job")))
        } yield {
          assertEquals(count, 2)
          val source =
            if (candidateWork) user.candidateProfile.map(SearchableText.candidate) else Some(SearchableText.job(job))
          val metadata = if (candidateWork) user.embedding.map(_.meta) else job.embedding.map(_.meta)
          assertEquals(metadata.map(_.sourceHash), source.map(com.example.graphQL.cats.shared.crypto.SourceHash.sha256))
        }
      }
    }
  }

  private def awaitAbsent(db: MongoDatabase[IO], key: EmbeddingWorkKey, remaining: Int = 200): IO[Unit] =
    MongoRepositoryTestSupport
      .findOne(db, MongoCollections.EmbeddingWork, Filters.eq(MongoFields.Id, key.value))
      .flatMap {
        case None                     => IO.unit
        case Some(_) if remaining > 0 => IO.sleep(20.millis) *> awaitAbsent(db, key, remaining - 1)
        case Some(record)             => IO.raiseError(new AssertionError(s"Work did not complete: $record"))
      }

  test("singleton Admin can inspect and CAS-repair failed work; other actors and stale generations cannot") {
    database.use { case (db, transactions) =>
      val users = new MongoUserRepository(db, transactions, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val admin = User(UserId(UUID.randomUUID()), None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
      val candidate = User(
        UserId(UUID.randomUUID()),
        Some(email(s"${UUID.randomUUID()}@example.com")),
        "Candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
        now
      )
      val key = EmbeddingWorkKey(EmbeddingWorkKind.Job, UUID.randomUUID().toString)
      for {
        _ <- successful(users.insert(admin))
        _ <- successful(users.insert(candidate))
        _ <- successful(work.enqueue(key, now))
        lease <- claimed(work, now)
        _ <- successful(work.fail(lease, EmbeddingWorkFailure.InvalidResponse, now))
        denied <- work.repairFailed(key, 1L, candidate.id, now, transactions).value
        status <- successful(work.inspectForAdmin(key, admin.id, transactions))
        repaired <- successful(work.repairFailed(key, 1L, admin.id, now, transactions))
        stale <- successful(work.repairFailed(key, 1L, admin.id, now, transactions))
        current <- successful(work.inspectForAdmin(key, admin.id, transactions))
      } yield {
        assertEquals(denied, Left(RepositoryError.AuthorityRevoked))
        assertEquals(status.map(_.failure), Some(Some(EmbeddingWorkFailure.InvalidResponse)))
        assert(repaired)
        assert(!stale)
        assertEquals(current.map(_.generation), Some(2L))
        assertEquals(current.map(_.state), Some(EmbeddingWorkState.Ready))
        assertEquals(current.map(_.failure), Some(None))
      }
    }
  }

  test("renewal requires a live matching generation and token") {
    database.use { case (db, _) =>
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val key = EmbeddingWorkKey(EmbeddingWorkKind.Job, UUID.randomUUID().toString)
      for {
        _ <- successful(work.enqueue(key, now))
        first <- claimed(work, now)
        extended <- successful(work.renew(first, now.plusSeconds(20), now.plusSeconds(60)))
        busy <- successful(work.claim("other", now.plusSeconds(31), now.plusSeconds(61)))
        _ <- successful(work.enqueue(key, now.plusSeconds(32)))
        newer <- successful(work.renew(first, now.plusSeconds(33), now.plusSeconds(90)))
        expired <- successful(work.renew(first, now.plusSeconds(61), now.plusSeconds(90)))
      } yield {
        assert(extended)
        assertEquals(busy, None)
        assert(!newer)
        assert(!expired)
      }
    }
  }

  test("a lease is reclaimable at exactly its expiry instant and held one millisecond earlier") {
    database.use { case (db, _) =>
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val key = EmbeddingWorkKey(EmbeddingWorkKind.Job, UUID.randomUUID().toString)
      val leaseUntil = now.plusSeconds(30)
      for {
        _ <- successful(work.enqueue(key, now))
        first <- claimed(work, now)
        held <- successful(work.claim("other", leaseUntil.minusMillis(1L), leaseUntil.plusSeconds(30)))
        renewedAtExpiry <- successful(work.renew(first, leaseUntil, leaseUntil.plusSeconds(30)))
        reclaimed <- successful(work.claim("other", leaseUntil, leaseUntil.plusSeconds(30)))
        document <- stored(db, key)
      } yield {
        assertEquals(held, None)
        assert(!renewedAtExpiry)
        assert(reclaimed.exists(_.leaseToken != first.leaseToken))
        assertEquals(document.getString(MongoFields.LeaseOwner), "other")
        assertEquals(document.getDate(MongoFields.UpdatedAt), Date.from(leaseUntil))
      }
    }
  }

  test("active embedding work can be enqueued transactionally without replacing its lease") {
    database.use { case (db, transactions) =>
      val repository = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val key = EmbeddingWorkKey(EmbeddingWorkKind.Job, UUID.randomUUID().toString)
      for {
        _ <- successful(repository.enqueue(key, now))
        first <- claimed(repository, now)
        _ <- successful(transactions.run(session => repository.enqueue(session, key, now.plusSeconds(1))))
        document <- stored(db, key)
      } yield {
        assertEquals(document.getLong(MongoFields.Generation).longValue, 2L)
        assertEquals(document.getString(MongoFields.State), "Processing")
        assertEquals(document.getString(MongoFields.LeaseToken), first.leaseToken)
        assertEquals(document.getDate(MongoFields.LeaseUntil), Date.from(now.plusSeconds(30)))
        assertEquals(document.getDate(MongoFields.AvailableAt), Date.from(now))
      }
    }
  }

  test("generation and reclaimed lease token fence old completion retry and failure") {
    database.use { case (db, _) =>
      val repository = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val key = EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, UUID.randomUUID().toString)
      for {
        _ <- successful(repository.enqueue(key, now))
        original <- claimed(repository, now)
        _ <- successful(repository.enqueue(key, now.plusSeconds(1)))
        oldComplete <- repository.complete(original).value
        oldRetry <- repository.retry(original, now.plusSeconds(2)).value
        oldFailure <- repository.fail(original, EmbeddingWorkFailure.RetryExhausted, now.plusSeconds(2)).value
        beforeExpiry <- successful(repository.claim("other", now.plusSeconds(2), now.plusSeconds(32)))
        newer <- claimed(repository, now.plusSeconds(31))
        sameGeneration <- claimed(repository, now.plusSeconds(62))
        reclaimedComplete <- repository.complete(newer).value
        reclaimedRetry <- repository.retry(newer, now.plusSeconds(63)).value
        reclaimedFailure <- repository.fail(newer, EmbeddingWorkFailure.DocumentTooLarge, now.plusSeconds(63)).value
        document <- stored(db, key)
        _ <- successful(repository.complete(sameGeneration))
        remaining <- MongoRepositoryTestSupport.count(db, MongoCollections.EmbeddingWork)
      } yield {
        List(oldComplete, oldRetry, oldFailure, reclaimedComplete, reclaimedRetry, reclaimedFailure)
          .foreach(result => assertEquals(result, Left(RepositoryError.Conflict)))
        assertEquals(beforeExpiry, None)
        assertEquals(newer.generation, 2L)
        assertEquals(sameGeneration.generation, 2L)
        assertNotEquals(newer.leaseToken, sameGeneration.leaseToken)
        assertEquals(document.getString(MongoFields.LeaseToken), sameGeneration.leaseToken)
        assertEquals(remaining, 0L)
      }
    }
  }

  test("failed and delayed work reset to ready and clear terminal metadata") {
    database.use { case (db, transactions) =>
      val repository = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val key = EmbeddingWorkKey(EmbeddingWorkKind.Job, UUID.randomUUID().toString)
      for {
        _ <- successful(transactions.run(session => repository.enqueue(session, key, now)))
        original <- claimed(repository, now)
        _ <- successful(repository.fail(original, EmbeddingWorkFailure.DocumentTooLarge, now))
        _ <- successful(transactions.run(session => repository.enqueue(session, key, now.plusSeconds(1))))
        reset <- stored(db, key)
        ready <- claimed(repository, now.plusSeconds(1))
        _ <- successful(repository.retry(ready, now.plusSeconds(100)))
        _ <- successful(repository.enqueue(key, now.plusSeconds(2)))
        delayedReset <- stored(db, key)
      } yield {
        assertEquals(reset.getString(MongoFields.State), "Ready")
        assert(!reset.containsKey(MongoFields.Failure))
        assert(!reset.containsKey(MongoFields.FinishedAt))
        assert(!reset.containsKey(MongoFields.LeaseToken))
        assertEquals(delayedReset.getInteger(MongoFields.Attempts).intValue, 0)
        assertEquals(delayedReset.getDate(MongoFields.AvailableAt), Date.from(now.plusSeconds(2)))
        assertEquals(delayedReset.getLong(MongoFields.Generation).longValue, 3L)
        assertEquals(delayedReset.getDate(MongoFields.CreatedAt), Date.from(now))
      }
    }
  }

  test("job and candidate mutations commit newer source and work while provider claims remain active") {
    database.use { case (db, transactions) =>
      val work = new MongoEmbeddingWorkRepository(db, Diagnostics.noop)
      val jobs = new MongoJobRepository(db, transactions, work, Diagnostics.noop)
      val users = new MongoUserRepository(db, transactions, work, Diagnostics.noop)
      val jobId = JobId(UUID.randomUUID())
      val candidateId = UserId(UUID.randomUUID())
      val job = Job(
        jobId,
        UserId(UUID.randomUUID()),
        "Scala Engineer",
        "Hiring infrastructure",
        List("Cats Effect"),
        Set("Scala"),
        Location("Cyprus", "Nicosia", remote = true),
        JobStatus.Open,
        now,
        now
      )
      val candidate = User(
        candidateId,
        Some(email(s"$candidateId@example.com")),
        "Candidate",
        UserRole.Candidate,
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Engineer"), None))),
        now
      )
      val profile = CandidateProfile(Set("Scala", "Kafka"), Some("Updated engineer"), None)
      val embedding = EntityEmbedding(List(0.1f), EmbeddingMeta("voyage-4-lite", "old-source", now))
      for {
        _ <- successful(jobs.createWithEvents(job, now, Nil, MutationWriteContext.directWrite))
        _ <- successful(users.insert(candidate))
        _ <- successful(
          work.enqueue(EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, candidateId.value.toString), now)
        )
        first <- claimed(work, now)
        second <- claimed(work, now)
        observedJob <- successful(jobs.findVersioned(jobId))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing job")))
        observedUser <- successful(users.findVersioned(candidateId))
          .flatMap(IO.fromOption(_)(new AssertionError("Missing candidate")))
        _ <- successful(
          jobs.updateWithEvents(
            observedJob,
            job.copy(title = "Staff Scala Engineer"),
            now.plusSeconds(1),
            Nil,
            MutationWriteContext.directWrite
          )
        )
        _ <- successful(
          users.updateProfile(
            candidateId,
            UserProfile.Candidate(profile),
            now.plusSeconds(1),
            MutationWriteContext.directWrite
          )
        )
        staleJob <- jobs.updateEmbedding(observedJob, embedding).value
        staleUser <- users.updateEmbedding(observedUser, embedding).value
        completions <- List(first, second).traverse(claim => work.complete(claim).value)
        currentJob <- successful(jobs.find(jobId))
        currentUser <- successful(users.find(candidateId))
        documents <- List(first.key, second.key).traverse(stored(db, _))
      } yield {
        assertEquals(staleJob, Left(RepositoryError.Conflict))
        assertEquals(staleUser, Left(RepositoryError.Conflict))
        completions.foreach(result => assertEquals(result, Left(RepositoryError.Conflict)))
        assertEquals(currentJob.map(_.title), Some("Staff Scala Engineer"))
        assertEquals(currentUser.flatMap(_.candidateProfile), Some(profile))
        assertEquals(currentJob.flatMap(_.embedding), None)
        assertEquals(currentUser.flatMap(_.embedding), None)
        documents.foreach(document => assertEquals(document.getLong(MongoFields.Generation).longValue, 2L))
      }
    }
  }
}
