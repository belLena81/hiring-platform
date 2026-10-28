package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.{ActorContext, HiringReadService, UseCaseError}
import com.example.graphQL.cats.service.mutation.TestIdempotency
import munit.CatsEffectSuite

/** Counts the store operations each authorized read issues, with the in-memory fakes behind counting wrappers. */
final class AuthorizedReadRoundTripsSpec extends CatsEffectSuite {
  private val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)
  private val eventPage = ApplicationEventPageRequest(None, PageSize.fromInt(10).toOption.get)
  private val otherRecruiterId = UserId(java.util.UUID.fromString("00000000-0000-0000-0000-0000000000a2"))
  private val otherRecruiter = recruiter.copy(id = otherRecruiterId)

  /** Names of the operations issued, in order. */
  private type Calls = Ref[IO, Vector[String]]

  private final class CountingUsers(delegate: UserRepository[IO], calls: Calls) extends UserRepository[IO] {
    export delegate.{relatedUsers, updateEmbedding}
    private def record[A](name: String)(read: RepositoryF[IO, A]): RepositoryF[IO, A] =
      RepositoryF.lift(calls.update(_ :+ name)) *> read
    override def findActor(id: UserId) = record("users.findActor")(delegate.findActor(id))
    override def find(id: UserId) = record("users.find")(delegate.find(id))
    override def findVersioned(id: UserId) = record("users.findVersioned")(delegate.findVersioned(id))
  }

  private final class CountingJobs(delegate: JobRepository[IO], calls: Calls) extends JobRepository[IO] {
    export delegate.{
      relatedJobs,
      findSubmissionSnapshot,
      findOpen,
      nearbyJobs,
      jobDiscoveryFacets,
      findAll,
      findByRecruiter,
      createWithEvents,
      updateWithEvents,
      updateEmbedding
    }
    private def record[A](name: String)(read: RepositoryF[IO, A]): RepositoryF[IO, A] =
      RepositoryF.lift(calls.update(_ :+ name)) *> read
    override def findOwner(id: JobId) = record("jobs.findOwner")(delegate.findOwner(id))
    override def find(id: JobId) = record("jobs.find")(delegate.find(id))
    override def findVersioned(id: JobId) = record("jobs.findVersioned")(delegate.findVersioned(id))
  }

  private final class CountingApplications(delegate: ApplicationRepository[IO], calls: Calls)
      extends ApplicationRepository[IO] {
    export delegate.{createForOpenJobWithEvents, updateStatusWithEvents}
    private def record[A](name: String)(read: RepositoryF[IO, A]): RepositoryF[IO, A] =
      RepositoryF.lift(calls.update(_ :+ name)) *> read
    override def find(id: ApplicationId) = record("applications.find")(delegate.find(id))
    override def findByCandidate(scope: HiringReadScope, page: ApplicationPageRequest) =
      record("applications.findByCandidate")(delegate.findByCandidate(scope, page))
    override def findByJob(scope: HiringReadScope, jobId: JobId, page: ApplicationPageRequest) =
      record("applications.findByJob")(delegate.findByJob(scope, jobId, page))
    override def applicationCounts(scope: HiringReadScope, jobIds: List[JobId]) =
      record("applications.applicationCounts")(delegate.applicationCounts(scope, jobIds))
    override def history(scope: HiringReadScope, applicationId: ApplicationId, page: ApplicationEventPageRequest) =
      record("applications.history")(delegate.history(scope, applicationId, page))
  }

  private final case class Rig(
      calls: Calls,
      applications: ApplicationService[IO],
      readModel: HiringReadService[IO],
      access: AuthorizedApplicationAccess[IO]
  )

  private def rig(extraUsers: List[User] = Nil, jobs: Map[JobId, Job] = Map(jobId -> openJob)): IO[Rig] =
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      usersRef <- Ref.of[IO, Map[UserId, User]](
        (List(candidate, recruiter, admin) ++ extraUsers).map(user => user.id -> user).toMap
      )
      jobsRef <- Ref.of[IO, Map[JobId, Job]](jobs)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map(applicationId -> createdApplication))
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errorRef <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      users = new CountingUsers(new InMemoryUsers(usersRef), calls)
      jobStore = new CountingJobs(new InMemoryJobs(jobsRef), calls)
      applicationStore = new CountingApplications(
        new InMemoryApplications(applicationsRef, eventsRef, errorRef, jobLookup = id => jobsRef.get.map(_.get(id))),
        calls
      )
      readModel = HiringReadService(users, jobStore, applicationStore)
      authorization = com.example.graphQL.cats.service.auth.ActorAuthorization(users)
    } yield Rig(
      calls,
      new ApplicationService(
        users,
        jobStore,
        applicationStore,
        readModel,
        TestIdempotency.noop,
        FixedTestClock.at(now),
        cats.effect.std.UUIDGen[IO]
      ),
      readModel,
      AuthorizedApplicationAccess(authorization, jobStore, applicationStore)
    )

  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)

  test("AC-02.1 jobApplications issues one actor read and one applications aggregation") {
    for {
      r <- rig()
      result <- r.applications.jobApplications(recruiterActor, jobId, page).value
      calls <- r.calls.get
    } yield {
      assertEquals(result.map(_.map(_.id)), Right(List(applicationId)))
      assertEquals(calls, Vector("users.findActor", "jobs.findOwner", "applications.findByJob"))
    }
  }

  test("AC-02.1 jobApplications issues no further actor read once the request resolved its authenticated actor") {
    for {
      r <- rig()
      authenticated <- r.readModel.viewer(recruiterActor).value.map(_.toOption.get)
      result <- r.applications.jobApplications(authenticated, jobId, page).value
      calls <- r.calls.get
    } yield {
      assertEquals(result.map(_.map(_.id)), Right(List(applicationId)))
      assertEquals(calls, Vector("users.findActor", "jobs.findOwner", "applications.findByJob"))
    }
  }

  test("AC-02.1 jobApplications never reads a whole user or a whole job") {
    for {
      r <- rig()
      _ <- r.applications.jobApplications(recruiterActor, jobId, page).value
      calls <- r.calls.get
    } yield assertEquals(calls.filter(call => call.endsWith(".find") || call.endsWith(".findVersioned")), Vector.empty)
  }

  test(
    "AC-02.2 applicationHistory by the owning candidate issues one actor read, one application read and the history"
  ) {
    for {
      r <- rig()
      result <- r.readModel
        .applicationHistory(ActorContext(candidateId, UserRole.Candidate), applicationId, eventPage)
        .value
      calls <- r.calls.get
    } yield {
      assert(result.isRight, result.toString)
      assertEquals(calls, Vector("users.findActor", "applications.find", "applications.history"))
    }
  }

  test("AC-02.2 applicationHistory by the managing recruiter reads the job ownership once and no whole job") {
    for {
      r <- rig()
      result <- r.readModel.applicationHistory(recruiterActor, applicationId, eventPage).value
      calls <- r.calls.get
    } yield {
      assert(result.isRight, result.toString)
      assertEquals(
        calls,
        Vector("users.findActor", "applications.find", "jobs.findOwner", "applications.history")
      )
    }
  }

  test("AC-02.3 jobApplications refuses a recruiter who does not own the job before any aggregation") {
    for {
      r <- rig(extraUsers = List(otherRecruiter))
      result <- r.applications
        .jobApplications(ActorContext(otherRecruiterId, UserRole.Recruiter), jobId, page)
        .value
      calls <- r.calls.get
    } yield {
      assertEquals(result.map(_.size), Left(UseCaseError.Domain(DomainError.Forbidden)))
      assert(!calls.contains("applications.findByJob"), calls.toString)
    }
  }

  test("AC-02.3 jobApplications refuses a candidate and reports an unknown job as not found") {
    for {
      r <- rig()
      forbidden <- r.applications.jobApplications(ActorContext(candidateId, UserRole.Candidate), jobId, page).value
      missing <- r.applications.jobApplications(recruiterActor, JobId(java.util.UUID.randomUUID()), page).value
    } yield {
      assertEquals(forbidden.map(_.size), Left(UseCaseError.Domain(DomainError.Forbidden)))
      assertEquals(missing.map(_.size), Left(UseCaseError.Domain(DomainError.NotFound("job"))))
    }
  }

  test("AC-02.5 viewing an application returns the loaded application without a second application read") {
    for {
      r <- rig()
      viewed <- r.readModel.canViewApplication(ActorContext(candidateId, UserRole.Candidate), applicationId).value
      calls <- r.calls.get
    } yield {
      assertEquals(viewed.map(_.id), Right(applicationId))
      assertEquals(calls, Vector("users.findActor", "applications.find"))
    }
  }

  test("AC-02.4 managing an application resolves the actor, the application and the job ownership once each") {
    for {
      r <- rig()
      managed <- r.access.manage(recruiterActor, applicationId).value
      calls <- r.calls.get
    } yield {
      assertEquals(managed.map(_.job.recruiterId), Right(recruiterId))
      assertEquals(calls, Vector("users.findActor", "applications.find", "jobs.findOwner"))
    }
  }

  test("AC-02.4 an admin manages any application and another recruiter is refused") {
    for {
      r <- rig(extraUsers = List(otherRecruiter))
      admin <- r.access.manage(ActorContext(adminId, UserRole.Admin), applicationId).value
      other <- r.access.manage(ActorContext(otherRecruiterId, UserRole.Recruiter), applicationId).value
    } yield {
      assert(admin.isRight, admin.toString)
      assertEquals(other.map(_ => ()), Left(UseCaseError.Domain(DomainError.Forbidden)))
    }
  }

  test("AC-02.4 managing an unknown application, or one whose job is gone, is not found") {
    for {
      r <- rig(jobs = Map.empty)
      noJob <- r.access.manage(recruiterActor, applicationId).value
      unknown <- r.access.manage(recruiterActor, ApplicationId(java.util.UUID.randomUUID())).value
    } yield {
      assertEquals(noJob.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("job"))))
      assertEquals(unknown.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("application"))))
    }
  }
}
