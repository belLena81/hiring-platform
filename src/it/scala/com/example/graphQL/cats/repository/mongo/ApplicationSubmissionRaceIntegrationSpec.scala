package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.shared.MillisecondClock
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{HiringReadModel, IdempotencyRequest}
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Real services over replica-set MongoDB: submissions racing each other, a job closure and a repeated request key. */
final class ApplicationSubmissionRaceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val fixtureTime = Instant.parse("2026-10-05T12:00:00Z")
  private val publisher = new EmbeddingWorkPublisher[IO] { def wake: IO[Unit] = IO.unit }

  private def user(role: UserRole, name: String = ""): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      if (name.isEmpty) s"Race $role" else name,
      role,
      role match {
        case UserRole.Candidate => Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Race organization", None)))
        case UserRole.Admin     => None
      },
      fixtureTime
    )

  private def openJob(recruiter: User): Job =
    Job(
      JobId(UUID.randomUUID()),
      recruiter.id,
      "Race Scala job",
      "Race description",
      List("Race requirement"),
      Set("Scala"),
      Location("Cyprus", "Nicosia", remote = false),
      JobStatus.Open,
      fixtureTime,
      fixtureTime
    )

  private final class Scenario(
      val fixture: MongoAccessEvaluationSupport.Fixture,
      val applications: ApplicationService[IO],
      val jobs: JobService[IO],
      val recruiter: User,
      val job: Job
  ) {
    def count(collection: String): IO[Long] = MongoRepositoryTestSupport.count(fixture.database, collection)
    def jobDocument: IO[org.bson.Document] =
      MongoRepositoryTestSupport
        .findOne(fixture.database, MongoCollections.Jobs, Filters.eq("_id", job.id.value.toString))
        .flatMap(IO.fromOption(_)(new AssertionError("job missing")))
  }

  private val millisecondClock: cats.effect.Clock[IO] = MillisecondClock(cats.effect.Clock[IO])

  private def request(key: UUID, input: String): IdempotencyRequest = IdempotencyRequest.fromCanonicalInput(key, input)

  private def scenario[A](candidates: List[User])(body: Scenario => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      val users = MongoUserRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      )
      val jobRepository = MongoJobRepository.transactional(
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
        com.example.graphQL.cats.service.HiringReadService(users, jobRepository, applicationRepository)
      val recruiter = user(UserRole.Recruiter)
      val job = openJob(recruiter)
      val applications = new ApplicationService[IO](
        users,
        jobRepository,
        applicationRepository,
        readModel,
        idempotent,
        millisecondClock,
        cats.effect.std.UUIDGen[IO]
      )
      val jobs = new JobService[IO](
        users,
        jobRepository,
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
        result <- body(new Scenario(fixture, applications, jobs, recruiter, job))
      } yield result
    }

  private def actor(candidate: User): ActorContext = ActorContext(candidate.id, candidate.role)

  private def isConflict(error: UseCaseError): Boolean = error == UseCaseError.Repository(RepositoryError.Conflict)
  private def isClosedJob(error: UseCaseError): Boolean = error == UseCaseError.Domain(DomainError.JobMustBeOpen)

  test("ARR-03.1 sixteen candidates applying in parallel to one open job are accepted or conflict with matching rows") {
    val candidates = List.tabulate(16)(i => user(UserRole.Candidate, s"Race candidate $i"))
    scenario(candidates) { s =>
      for {
        outcomes <- candidates
          .map(c =>
            s.applications
              .submitApplication(request(UUID.randomUUID(), s"submit:${c.id.value}"), actor(c), s.job.id)
              .value
          )
          .parSequence
        applicationRows <- s.count(MongoCollections.Applications)
        historyRows <- s.count(MongoCollections.ApplicationEvents)
        outboxRows <- s.count(MongoCollections.EventOutbox)
        job <- s.jobDocument
      } yield {
        val accepted = outcomes.count(_.isRight)
        val conflicts = outcomes.count(_.left.exists(isConflict))
        val other = outcomes.collect { case Left(error) if !isConflict(error) => error }
        println(s"ARR-03.1 observed: candidates=16 accepted=$accepted conflicts=$conflicts")
        assertEquals(other, Nil, clue(outcomes))
        assertEquals(accepted + conflicts, 16)
        assert(accepted >= 1, clue(outcomes))
        assertEquals(applicationRows, accepted.toLong)
        assertEquals(historyRows, accepted.toLong)
        assertEquals(outboxRows, accepted.toLong)
        // Every accepted submission advanced the job revision exactly once.
        assertEquals(job.get("version").asInstanceOf[Number].longValue(), accepted.toLong)
      }
    }
  }

  /*
   * Soundness: a submission commits only if the job document still matches its _id and status Open (the observed
   * version is deliberately not matched, so concurrent submissions retry instead of conflicting), and every committed
   * submission or closure bumps the version. Hence a closure and a submission that both commit are serialized with the
   * submission first (the closure's Closed status would fail the submission's guard otherwise), and a submission that
   * observed Closed cannot commit. Observable consequences asserted below:
   * the final version equals the number of committed writers (a lost guard would lose a bump), the application exists
   * exactly when the submission was accepted, and a rejected submission is only Conflict or "job must be open".
   */
  test("ARR-03.2 closing a job racing a submission never leaves an application committed after the closure") {
    val candidate = user(UserRole.Candidate)
    val rounds = List.range(0, 20)
    rounds
      .traverse { _ =>
        scenario(List(candidate)) { s =>
          for {
            (submitted, closed) <- (
              s.applications.submitApplication(request(UUID.randomUUID(), "submit"), actor(candidate), s.job.id).value,
              s.jobs
                .closeJob(request(UUID.randomUUID(), "close"), ActorContext(s.recruiter.id, s.recruiter.role), s.job.id)
                .value
            ).parTupled
            applicationRows <- s.count(MongoCollections.Applications)
            historyRows <- s.count(MongoCollections.ApplicationEvents)
            job <- s.jobDocument
          } yield {
            val committedWriters = List(submitted.isRight, closed.isRight).count(identity).toLong
            submitted.left.foreach(error => assert(isConflict(error) || isClosedJob(error), clue(error)))
            closed.left.foreach(error => assert(isConflict(error), clue(error)))
            assertEquals(applicationRows, if (submitted.isRight) 1L else 0L)
            assertEquals(historyRows, applicationRows)
            assertEquals(job.getString("status"), if (closed.isRight) "Closed" else "Open")
            assertEquals(job.get("version").asInstanceOf[Number].longValue(), committedWriters)
            (submitted.isRight, closed.isRight)
          }
        }
      }
      .map(results =>
        println(
          s"ARR-03.2 observed over ${rounds.size} rounds: (submitAccepted, closeAccepted) counts=${results.groupBy(identity).view.mapValues(_.size).toMap}"
        )
      )
  }

  test("ARR-03.3 six identical submissions with one idempotency key create one application and one receipt") {
    val candidate = user(UserRole.Candidate)
    scenario(List(candidate)) { s =>
      val key = UUID.randomUUID()
      val identical = request(key, s"submit:${s.job.id.value}")
      for {
        outcomes <- List
          .fill(6)(s.applications.submitApplication(identical, actor(candidate), s.job.id).value)
          .parSequence
        later <- s.applications.submitApplication(identical, actor(candidate), s.job.id).value
        applicationRows <- s.count(MongoCollections.Applications)
        historyRows <- s.count(MongoCollections.ApplicationEvents)
        outboxRows <- s.count(MongoCollections.EventOutbox)
        receiptRows <- s.count(MongoCollections.MutationReceipts)
      } yield {
        val payloads = (later :: outcomes).map {
          case Right(application) => application
          case Left(error)        => fail(s"unexpected $error")
        }
        assertEquals(payloads.distinct.size, 1, clue(payloads))
        assertEquals((applicationRows, historyRows, outboxRows, receiptRows), (1L, 1L, 1L, 1L))
      }
    }
  }
}
