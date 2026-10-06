package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.AccountValueFixtures.email
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import munit.CatsEffectSuite
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.{Date, UUID}
import scala.concurrent.duration.*

final class MongoEmbeddingWorkIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(image))

  private def database: Resource[IO, (MongoDatabase[IO], MongoTransactionRunner)] =
    Resource
      .make(IO.blocking {
        val instance = new ReplicaSet
        val _ = instance
          .withExposedPorts(27017)
          .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
        try {
          instance.start()
          val result = instance.execInContainer(
            "mongosh",
            "--quiet",
            "--eval",
            "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
          )
          if (result.getExitCode != 0) throw new AssertionError(result.getStderr)
          instance
        } catch {
          case error: Throwable => instance.stop(); throw error
        }
      })(instance => IO.blocking(instance.stop()))
      .flatMap { instance =>
        def awaitPrimary(remaining: Int): IO[Unit] =
          IO.blocking(instance.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary"))
            .flatMap { result =>
              if (result.getExitCode == 0 && result.getStdout.trim == "true") IO.unit
              else if (remaining > 0) IO.sleep(250.millis) *> awaitPrimary(remaining - 1)
              else IO.raiseError(new AssertionError("Mongo primary election failed"))
            }
        Resource.eval(awaitPrimary(60)) *> MongoDatabaseProbe
          .clientResource(
            s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
          )
          .evalMap { client =>
            client
              .getDatabase(s"embedding_work_${UUID.randomUUID()}")
              .map(
                _ ->
                  MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = Diagnostics.noop)
              )
          }
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
