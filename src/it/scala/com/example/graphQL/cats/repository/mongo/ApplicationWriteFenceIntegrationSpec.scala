package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.mongodb.client.model.Filters
import mongo4cats.models.client.ClientSessionOptions
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** The acting user's predicate travels into the application write transaction: a deleted, wrong-role or non-owning
  * actor cannot commit, even racing the deletion or when the service check is bypassed.
  */
final class ApplicationWriteFenceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val at = Instant.parse("2026-10-05T12:00:00Z")
  private val rounds = 25

  private def user(role: UserRole, name: String, singleton: Boolean = false): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      name,
      role,
      role match {
        case UserRole.Candidate => Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        case UserRole.Recruiter => Some(UserProfile.Recruiter(RecruiterProfile("Fence organization", None)))
        case UserRole.Admin     => None
      },
      at,
      adminSingleton = singleton
    )

  private def openJob(owner: User): Job =
    Job(
      JobId(UUID.randomUUID()),
      owner.id,
      s"Fence job ${UUID.randomUUID()}",
      "Fence description",
      List("Fence requirement"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobStatus.Open,
      at,
      at
    )

  private final class Harness(val fixture: MongoAccessEvaluationSupport.Fixture) {
    val users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )
    val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])

    def insertUser(value: User): IO[Unit] =
      MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
    def insertJob(value: Job): IO[Unit] =
      MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
    def insertApplication(value: Application): IO[Unit] =
      MongoRepositoryTestSupport.insertOne(
        fixture.database,
        MongoCollections.Applications,
        MongoHiringCodecs.application(value)
      )
    def count(collection: String): IO[Long] = MongoRepositoryTestSupport.count(fixture.database, collection)
    def countWhere(collection: String, filter: org.bson.conversions.Bson): IO[Long] =
      MongoRepositoryTestSupport.count(fixture.database, collection, filter)

    def snapshot(job: Job): JobSubmissionSnapshot = JobSubmissionSnapshot(job.id, JobStatus.Open, 0L)

    def submit(candidate: User, job: Job): IO[Either[RepositoryError, Unit]] = {
      val application = Application.create(ApplicationId(UUID.randomUUID()), candidate.id, job.id, at)
      val event = ApplicationEvent(
        ApplicationEventId(UUID.randomUUID()),
        application.id,
        None,
        ApplicationStatus.Created,
        candidate.id,
        at,
        None,
        None,
        Some(candidate.role)
      )
      applications
        .createForOpenJobWithEvents(
          snapshot(job),
          application,
          event,
          Nil,
          ApplicationWriteActor(candidate.id, candidate.role),
          MutationWriteContext.directWrite
        )
        .value
    }

    def created(candidate: User, job: Job): IO[Application] = {
      val application = Application.create(ApplicationId(UUID.randomUUID()), candidate.id, job.id, at)
      insertApplication(application).as(application)
    }

    def accept(application: Application, actor: User): IO[Either[RepositoryError, Unit]] = {
      val event = ApplicationEvent(
        ApplicationEventId(UUID.randomUUID()),
        application.id,
        Some(ApplicationStatus.Created),
        ApplicationStatus.Accepted,
        actor.id,
        at.plusSeconds(60),
        None,
        None,
        Some(actor.role)
      )
      applications
        .updateStatusWithEvents(
          application.copy(status = ApplicationStatus.Accepted, updatedAt = at.plusSeconds(60)),
          event,
          Nil,
          ApplicationWriteActor(actor.id, actor.role),
          MutationWriteContext.directWrite
        )
        .value
    }

    /** Retries only the typed `Conflict` of exhausted write-conflict retries, bounded by time, not by attempt count. */
    def delete(subject: User): IO[Unit] = {
      def attempt: IO[Unit] =
        users
          .deleteAccount(
            subject.id,
            at.plusSeconds(30),
            s"deleted-${subject.id.value}",
            MutationWriteContext.directWrite
          )
          .value
          .flatMap {
            case Right(_)                       => IO.unit
            case Left(RepositoryError.Conflict) => IO.sleep(20.millis) *> attempt
            case Left(error)                    => IO.raiseError(new AssertionError(s"deletion failed: $error"))
          }
      attempt.timeoutTo(60.seconds, IO.raiseError(new AssertionError("deletion kept conflicting for 60 seconds")))
    }

    private val fence = new MongoApplicationWriteFence(fixture.database)

    /** The write transaction takes its snapshot, then the actor's deletion commits, then the fence step runs: the stale
      * snapshot still shows an active account, so only the fence write can stop the commit.
      */
    def fenceAfterConcurrentDeletion(
        actor: User,
        step: (MongoApplicationWriteFence, Option[mongo4cats.client.ClientSession[IO]]) => RepositoryF[IO, Unit]
    ): IO[Either[Throwable, Either[RepositoryError, Unit]]] =
      fixture.client.startSession(ClientSessionOptions()).use { session =>
        for {
          _ <- session.startTransaction
          _ <- MongoSessionOperations.findOne(
            Mongo4catsCollections.documents(fixture.database, MongoCollections.Users),
            Some(session),
            MongoFilter.eq(MongoFields.Id, actor.id.value.toString)
          )
          _ <- delete(actor)
          outcome <- step(fence, Some(session)).value.attempt
          _ <- session.abortTransaction.attempt
        } yield outcome
      }

    def status(application: Application): IO[Option[String]] =
      MongoRepositoryTestSupport
        .findOne(fixture.database, MongoCollections.Applications, Filters.eq("_id", application.id.value.toString))
        .map(_.map(_.getString("status")))
  }

  private def harness[A](body: Harness => IO[A]): IO[A] =
    mongoResource.use { fixture =>
      MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO]) *> body(new Harness(fixture))
    }

  private def revokedOrConflict(result: Either[RepositoryError, Unit]): Boolean =
    result.left.exists(error => error == RepositoryError.AuthorityRevoked || error == RepositoryError.Conflict)

  test("ARR-02.1 a candidate deletion racing a submission never leaves an application after the tombstone") {
    harness { h =>
      List.range(0, rounds).traverse { _ =>
        val candidate = user(UserRole.Candidate, s"Fence candidate ${UUID.randomUUID()}")
        val recruiter = user(UserRole.Recruiter, s"Fence recruiter ${UUID.randomUUID()}")
        val job = openJob(recruiter)
        for {
          _ <- List(candidate, recruiter).traverse_(h.insertUser) *> h.insertJob(job)
          (submitted, _) <- (h.submit(candidate, job), h.delete(candidate)).parTupled
          rows <- h.countWhere(MongoCollections.Applications, Filters.eq("candidateId", candidate.id.value.toString))
          history <- h.countWhere(
            MongoCollections.ApplicationEvents,
            Filters.eq("actorId", candidate.id.value.toString)
          )
          // Whoever won, the tombstone is committed now: a later submission is refused by the store.
          late <- h.submit(candidate, job)
          lateRows <- h.countWhere(
            MongoCollections.Applications,
            Filters.eq("candidateId", candidate.id.value.toString)
          )
          fence <- MongoRepositoryTestSupport.findOne(
            h.fixture.database,
            MongoCollections.OutboxSubjectFences,
            Filters.eq("_id", candidate.id.value.toString)
          )
        } yield {
          assert(submitted.isRight || revokedOrConflict(submitted), clue(submitted))
          assertEquals(rows, if (submitted.isRight) 1L else 0L)
          assertEquals(history, rows)
          assertEquals(late, Left(RepositoryError.AuthorityRevoked))
          assertEquals(lateRows, rows, "no application appears after the tombstone commit")
          assertEquals(fence.map(_.getBoolean("deleted").booleanValue), Option(true))
          submitted.isRight
        }
      }
    }.map(results => println(s"ARR-02.1 observed over $rounds rounds: submitAccepted=${results.count(identity)}"))
  }

  test("ARR-02.2 a recruiter deletion racing a status change never changes the application after the tombstone") {
    harness { h =>
      List.range(0, rounds).traverse { _ =>
        val candidate = user(UserRole.Candidate, s"Fence candidate ${UUID.randomUUID()}")
        val recruiter = user(UserRole.Recruiter, s"Fence recruiter ${UUID.randomUUID()}")
        val job = openJob(recruiter)
        for {
          _ <- List(candidate, recruiter).traverse_(h.insertUser) *> h.insertJob(job)
          application <- h.created(candidate, job)
          (changed, _) <- (h.accept(application, recruiter), h.delete(recruiter)).parTupled
          after <- h.status(application)
          history <- h.countWhere(
            MongoCollections.ApplicationEvents,
            Filters.eq("applicationId", application.id.value.toString)
          )
          other = application.copy(id = ApplicationId(UUID.randomUUID()), candidateId = UserId(UUID.randomUUID()))
          _ <- h.insertApplication(other)
          late <- h.accept(other, recruiter)
          lateStatus <- h.status(other)
        } yield {
          assert(changed.isRight || revokedOrConflict(changed), clue(changed))
          assertEquals(after, Some(if (changed.isRight) "Accepted" else "Created"))
          assertEquals(history, if (changed.isRight) 1L else 0L)
          assertEquals(late, Left(RepositoryError.AuthorityRevoked))
          assertEquals(lateStatus, Some("Created"))
          changed.isRight
        }
      }
    }.map(results => println(s"ARR-02.2 observed over $rounds rounds: changeAccepted=${results.count(identity)}"))
  }

  private def isWriteConflict(outcome: Either[Throwable, Either[RepositoryError, Unit]]): Boolean =
    outcome.left.exists(MongoErrors.isTransient)

  test("ARR-02.1 a submission whose snapshot predates the candidate's committed deletion conflicts on the fence") {
    harness { h =>
      val candidate = user(UserRole.Candidate, s"Stale candidate ${UUID.randomUUID()}")
      h.insertUser(candidate) *> h
        .fenceAfterConcurrentDeletion(
          candidate,
          (fence, session) =>
            fence.forSubmit(session, ApplicationWriteActor(candidate.id, candidate.role), JobId(UUID.randomUUID()))
        )
        .map(outcome => assert(isWriteConflict(outcome), clue(outcome)))
    }
  }

  test("ARR-02.2 a status change whose snapshot predates the recruiter's committed deletion conflicts on the fence") {
    harness { h =>
      val recruiter = user(UserRole.Recruiter, s"Stale recruiter ${UUID.randomUUID()}")
      val job = openJob(recruiter)
      h.insertUser(recruiter) *> h.insertJob(job) *> h
        .fenceAfterConcurrentDeletion(
          recruiter,
          (fence, session) =>
            fence.forStatusChange(session, ApplicationWriteActor(recruiter.id, recruiter.role), job.id)
        )
        .map(outcome => assert(isWriteConflict(outcome), clue(outcome)))
    }
  }

  test(
    "ARR-02.3 the store refuses a non-owner recruiter, a candidate, an unknown actor and a recruiter submitter"
  ) {
    harness { h =>
      val candidate = user(UserRole.Candidate, "Fence candidate")
      val owner = user(UserRole.Recruiter, "Fence owner")
      val other = user(UserRole.Recruiter, "Fence other recruiter")
      val admin = user(UserRole.Admin, "Fence admin", singleton = true)
      val job = openJob(owner)
      for {
        _ <- List(candidate, owner, other, admin).traverse_(h.insertUser) *> h.insertJob(job)
        application <- h.created(candidate, job)
        nonOwner <- h.accept(application, other)
        byCandidate <- h.accept(application, candidate)
        unknown <- h.accept(application, user(UserRole.Recruiter, "Fence ghost"))
        recruiterSubmit <- h.submit(owner, job)
        unchanged <- h.status(application)
        historyBefore <- h.count(MongoCollections.ApplicationEvents)
        owned <- h.accept(application, owner)
        afterOwner <- h.status(application)
        adminApplication <- h.created(candidate.copy(id = UserId(UUID.randomUUID())), job)
        byAdmin <- h.accept(adminApplication, admin)
        adminStatus <- h.status(adminApplication)
      } yield {
        assertEquals(nonOwner, Left(RepositoryError.AuthorityRevoked))
        assertEquals(byCandidate, Left(RepositoryError.AuthorityRevoked))
        assertEquals(unknown, Left(RepositoryError.AuthorityRevoked))
        assertEquals(recruiterSubmit, Left(RepositoryError.AuthorityRevoked))
        assertEquals(unchanged, Some("Created"))
        assertEquals(historyBefore, 0L)
        assertEquals(owned, Right(()))
        assertEquals(afterOwner, Some("Accepted"))
        assertEquals((byAdmin, adminStatus), (Right(()), Some("Accepted")))
      }
    }
  }

  test("ARR-02.3 a recruiter cannot reach a victim's application by pairing its id with their own job") {
    harness { h =>
      val victimCandidate = user(UserRole.Candidate, "Forgery victim")
      val victimRecruiter = user(UserRole.Recruiter, "Forgery victim recruiter")
      val attacker = user(UserRole.Recruiter, "Forgery attacker")
      val victimJob = openJob(victimRecruiter)
      val ownJob = openJob(attacker)
      for {
        _ <- List(victimCandidate, victimRecruiter, attacker).traverse_(h.insertUser)
        _ <- h.insertJob(victimJob) *> h.insertJob(ownJob)
        victim <- h.created(victimCandidate, victimJob)
        forged = victim.copy(jobId = ownJob.id)
        forgedJob <- h.accept(forged, attacker)
        forgedCandidate <- h.accept(victim.copy(jobId = ownJob.id, candidateId = attacker.id), attacker)
        status <- h.status(victim)
        stored <- MongoRepositoryTestSupport.findOne(
          h.fixture.database,
          MongoCollections.Applications,
          Filters.eq("_id", victim.id.value.toString)
        )
        history <- h.count(MongoCollections.ApplicationEvents)
      } yield {
        assertEquals(forgedJob, Left(RepositoryError.Conflict))
        assertEquals(forgedCandidate, Left(RepositoryError.Conflict))
        assertEquals(status, Some("Created"))
        assertEquals(stored.map(_.getString("jobId")), Some(victimJob.id.value.toString))
        assertEquals(history, 0L)
      }
    }
  }

  test("ARR-02.1 a never-fenced active candidate gets a fence document on first submission and keeps submitting") {
    harness { h =>
      val recruiter = user(UserRole.Recruiter, "Fence recruiter")
      val jobs = List.fill(3)(openJob(recruiter))
      val candidates = List.tabulate(12)(i => user(UserRole.Candidate, s"First fence candidate $i"))
      for {
        _ <- h.insertUser(recruiter) *> candidates.traverse_(h.insertUser) *> jobs.traverse_(h.insertJob)
        // Two parallel first writes per candidate hit the upsert of the same absent fence document.
        results <- candidates.flatTraverse(c => jobs.take(2).parTraverse(job => h.submit(c, job)).map(_.toList))
        fences <- h.count(MongoCollections.OutboxSubjectFences)
        rows <- h.count(MongoCollections.Applications)
      } yield {
        val failures = results.collect { case Left(error) => error }
        println(s"ARR-02.1 first-fence upsert race: attempts=${results.size} failures=$failures")
        assert(failures.forall(_ == RepositoryError.Conflict), clue(failures))
        assertEquals(rows, results.count(_.isRight).toLong)
        assertEquals(fences, candidates.size.toLong)
      }
    }
  }

  private final case class Run(accepted: Int, failed: Int, millis: Long, errors: List[RepositoryError])

  test("ARR-02.2 measured fence contention: one recruiter changing sixteen applications versus sixteen recruiters") {
    harness { h =>
      def scenario(sameRecruiter: Boolean): IO[Run] = {
        val recruiters =
          if (sameRecruiter) List(user(UserRole.Recruiter, s"Hot ${UUID.randomUUID()}"))
          else List.tabulate(16)(i => user(UserRole.Recruiter, s"Spread $i ${UUID.randomUUID()}"))
        val pairs = List.tabulate(16)(i => recruiters(i % recruiters.size)).map(owner => owner -> openJob(owner))
        for {
          _ <- recruiters.traverse_(h.insertUser) *> pairs.traverse_(pair => h.insertJob(pair._2))
          apps <- pairs.traverse(pair => h.created(user(UserRole.Candidate, "Contention candidate"), pair._2))
          started <- IO.monotonic
          results <- apps.zip(pairs).parTraverse((app, pair) => h.accept(app, pair._1))
          elapsed <- IO.monotonic.map(_ - started)
        } yield Run(
          results.count(_.isRight),
          results.count(_.isLeft),
          elapsed.toMillis,
          results.collect { case Left(error) => error }
        )
      }
      for {
        hot <- List.fill(3)(scenario(true)).sequence
        spread <- List.fill(3)(scenario(false)).sequence
      } yield {
        println(s"ARR-02.2 contention one recruiter x16: $hot")
        println(s"ARR-02.2 contention sixteen recruiters: $spread")
        assert(hot.forall(_.accepted >= 1), clue(hot))
        assert(spread.forall(_.accepted == 16), clue(spread))
      }
    }
  }
}
