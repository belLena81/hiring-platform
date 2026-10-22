package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.port.{
  EmbeddingWorkKey,
  EmbeddingWorkKind,
  MutationWriteContext,
  RepositoryF,
  Versioned
}
import com.example.graphQL.cats.service.protocol.{HiringReadModel, IdempotencyRequest}
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.shared.MillisecondClock
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** MSC-01: `version` is the content revision and moves only on content changes. Submissions and interview scheduling
  * advance `jobs.admissionFence`, embedding writes are guarded by `version` but never advance it, and account deletion
  * still conflicts with the paths that read an account.
  */
final class ContentRevisionFenceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val fixtureTime = Instant.parse("2026-10-05T12:00:00Z")
  private val publisher = new EmbeddingWorkPublisher[IO] { def wake: IO[Unit] = IO.unit }
  private val millisecondClock: cats.effect.Clock[IO] = MillisecondClock(cats.effect.Clock[IO])
  private val embedding = EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta("voyage-4-lite", "source", fixtureTime))

  private def user(role: UserRole, name: String): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      name,
      role,
      role match {
        case UserRole.Candidate => Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Revision organization", None)))
        case UserRole.Admin     => None
      },
      fixtureTime
    )

  private def openJob(recruiter: User): Job =
    Job(
      JobId(UUID.randomUUID()),
      recruiter.id,
      "Revision Scala job",
      "Revision description",
      List("Revision requirement"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobState.Open,
      fixtureTime,
      fixtureTime
    )

  private final class Scenario(
      val fixture: MongoAccessEvaluationSupport.Fixture,
      val applications: ApplicationService[IO],
      val jobService: JobService[IO],
      val jobs: MongoJobRepository,
      val users: MongoUserRepository,
      val recruiter: User,
      val job: Job
  ) {
    def count(collection: String): IO[Long] = MongoRepositoryTestSupport.count(fixture.database, collection)
    def stored(collection: String, id: String): IO[org.bson.Document] =
      MongoRepositoryTestSupport
        .findOne(fixture.database, collection, Filters.eq("_id", id))
        .flatMap(IO.fromOption(_)(new AssertionError(s"$collection/$id missing")))
    def jobDocument: IO[org.bson.Document] = stored(MongoCollections.Jobs, job.id.value.toString)
    def observedJob: IO[Versioned[Job]] =
      require(jobs.findVersioned(job.id)).flatMap(found => IO.fromOption(found)(new AssertionError("job missing")))
  }

  private def require[A](effect: RepositoryF[IO, A]): IO[A] =
    effect.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"repository failure $error")), IO.pure))

  private def long(document: org.bson.Document, field: String): Long =
    Option(document.get(field)).fold(0L)(_.asInstanceOf[Number].longValue())

  private def request(input: String): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(UUID.randomUUID(), input)

  private def scenario[A](candidates: List[User])(body: Scenario => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val users =
        MongoUserRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop[IO]
        )
      val jobs = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val applicationRepository =
        MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val receipts =
        MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val idempotent = Idempotent(receipts, millisecondClock)
      val readModel: HiringReadModel[IO] =
        com.example.graphQL.cats.service.HiringReadService(users, jobs, applicationRepository)
      val recruiter = user(UserRole.Recruiter, "Revision recruiter")
      val job = openJob(recruiter)
      val applications = new ApplicationService[IO](
        users,
        jobs,
        applicationRepository,
        readModel,
        idempotent,
        millisecondClock,
        cats.effect.std.UUIDGen[IO]
      )
      val jobService = new JobService[IO](
        users,
        jobs,
        publisher,
        idempotent,
        Diagnostics.noop[IO],
        millisecondClock,
        cats.effect.std.UUIDGen[IO]
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- (recruiter :: candidates).traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
        )
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(job))
        result <- body(new Scenario(fixture, applications, jobService, jobs, users, recruiter, job))
      } yield result
    }

  private def actor(candidate: User): ActorContext = ActorContext(candidate.id, candidate.role)

  test("MSC-01.1 an edit observed before sixteen concurrent submissions still succeeds at the next revision") {
    val candidates = List.tabulate(16)(i => user(UserRole.Candidate, s"Revision candidate $i"))
    scenario(candidates) { s =>
      for {
        observed <- s.observedJob
        outcomes <- candidates
          .map(c => s.applications.submitApplication(request(s"submit:${c.id.value}"), actor(c), s.job.id).value)
          .parSequence
        edited = observed.value.copy(title = "Edited after submissions", updatedAt = fixtureTime.plusSeconds(60))
        result <- s.jobs
          .updateWithEvents(observed, edited, edited.updatedAt, Nil, MutationWriteContext.directWrite)
          .value
        document <- s.jobDocument
      } yield {
        val accepted = outcomes.count(_.isRight).toLong
        assert(accepted >= 1L, clue(outcomes))
        assertEquals(result, Right(Versioned(edited, observed.version + 1L)))
        assertEquals(long(document, "version"), observed.version + 1L)
        assertEquals(long(document, "admissionFence"), accepted)
        assertEquals(document.getString("title"), "Edited after submissions")
        assertEquals(document.getDate("updatedAt").toInstant, edited.updatedAt)
      }
    }
  }

  test("MSC-01.1 a content edit rewrites only content: embedding, its metadata and the admission fence survive") {
    scenario(Nil) { s =>
      for {
        observed <- s.observedJob
        _ <- require(s.jobs.updateEmbedding(observed, embedding))
        raw <- s.jobDocument
        _ <- MongoRepositoryTestSupport
          .collection(s.fixture.database, MongoCollections.Jobs)
          .flatMap(
            _.updateOne(
              Filters.eq("_id", s.job.id.value.toString),
              com.mongodb.client.model.Updates.set("admissionFence", Long.box(3L))
            )
          )
        current <- s.observedJob
        edited = current.value.copy(location = Location("Cyprus", None, remote = true), title = "Edited again")
        _ <- require(s.jobs.updateWithEvents(current, edited, fixtureTime, Nil, MutationWriteContext.directWrite))
        after <- s.jobDocument
      } yield {
        assertEquals(after.get("embedding"), raw.get("embedding"))
        assertEquals(after.get("embeddingMeta"), raw.get("embeddingMeta"))
        assertEquals(long(after, "admissionFence"), 3L)
        assert(!after.get("location", classOf[org.bson.Document]).containsKey("city"), clue(after.toJson))
        assertEquals(long(after, "version"), long(raw, "version") + 1L)
      }
    }
  }

  test("MSC-01.2 closing racing a submission serializes: the application precedes the closure or is rejected") {
    val candidate = user(UserRole.Candidate, "Revision race candidate")
    List.range(0, 15).traverse { _ =>
      scenario(List(candidate)) { s =>
        for {
          (submitted, closed) <- (
            s.applications.submitApplication(request("submit"), actor(candidate), s.job.id).value,
            s.jobService.closeJob(request("close"), ActorContext(s.recruiter.id, s.recruiter.role), s.job.id).value
          ).parTupled
          applicationRows <- s.count(MongoCollections.Applications)
          application <- MongoRepositoryTestSupport.findOne(
            s.fixture.database,
            MongoCollections.Applications,
            new org.bson.Document()
          )
          document <- s.jobDocument
        } yield {
          assertEquals(applicationRows, if (submitted.isRight) 1L else 0L)
          submitted.left.foreach(error =>
            assert(
              error == UseCaseError.Repository(RepositoryError.Conflict) ||
                error == UseCaseError.Domain(com.example.graphQL.cats.domain.error.DomainError.JobMustBeOpen),
              clue(error)
            )
          )
          assertEquals(document.getString("status"), if (closed.isRight) "Closed" else "Open")
          // Both may commit only with the application first; never an application created after the closure.
          if (submitted.isRight && closed.isRight)
            assert(
              !application.exists(_.getDate("createdAt").toInstant.isAfter(document.getDate("closedAt").toInstant)),
              clue((application, document))
            )
          assertEquals(long(document, "admissionFence"), if (submitted.isRight) 1L else 0L)
          assertEquals(long(document, "version"), if (closed.isRight) 1L else 0L)
        }
      }
    }
  }

  test("MSC-01.3 an embedding completion at a revision does not stale a profile edit and advances no revision") {
    val candidate = user(UserRole.Candidate, "Revision embedding candidate")
    scenario(List(candidate)) { s =>
      for {
        observed <- require(s.users.findVersioned(candidate.id)).flatMap(found =>
          IO.fromOption(found)(new AssertionError("candidate missing"))
        )
        _ <- require(s.users.updateEmbedding(observed, embedding))
        afterEmbedding <- s.stored(MongoCollections.Users, candidate.id.value.toString)
        _ <- require(
          s.users.updateProfile(
            candidate.id,
            UserProfile.Candidate(CandidateProfile(Set("Scala", "Kafka"), Some("Backend engineer"), None)),
            fixtureTime.plusSeconds(1),
            MutationWriteContext.directWrite
          )
        )
        afterProfile <- s.stored(MongoCollections.Users, candidate.id.value.toString)
      } yield {
        assertEquals(long(afterEmbedding, "version"), observed.version)
        assert(afterEmbedding.containsKey("embedding"))
        assertEquals(long(afterProfile, "version"), observed.version + 1L)
      }
    }
  }

  test("MSC-01.4 a stale embedding is rejected after the content moved on and writes nothing") {
    scenario(Nil) { s =>
      for {
        observed <- s.observedJob
        edited = observed.value.copy(title = "Moved on", updatedAt = fixtureTime.plusSeconds(1))
        // The edit goes through a repository with real durable embedding work, so the requeue is observable.
        durable = MongoJobRepository.transactional(
          s.fixture.database,
          s.fixture.client,
          new MongoEmbeddingWorkRepository(s.fixture.database, Diagnostics.noop[IO]),
          Diagnostics.noop[IO]
        )
        _ <- require(
          durable.updateWithEvents(observed, edited, edited.updatedAt, Nil, MutationWriteContext.directWrite)
        )
        work <- MongoRepositoryTestSupport.findOne(
          s.fixture.database,
          MongoCollections.EmbeddingWork,
          Filters.eq("_id", EmbeddingWorkKey(EmbeddingWorkKind.Job, s.job.id.value.toString).value)
        )
        stale <- s.jobs.updateEmbedding(observed, embedding).value
        document <- s.jobDocument
        fresh <- s.observedJob
        accepted <- s.jobs.updateEmbedding(fresh, embedding).value
        afterFresh <- s.jobDocument
      } yield {
        assert(work.exists(row => long(row, "generation") >= 1L), clue(work))
        assertEquals(stale, Left(RepositoryError.Conflict))
        assert(!document.containsKey("embedding"), clue(document.toJson))
        assertEquals(long(document, "version"), observed.version + 1L)
        assertEquals(accepted, Right(()))
        assertEquals(long(afterFresh, "version"), fresh.version)
      }
    }
  }
}
