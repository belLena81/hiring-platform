package com.example.graphQL.cats.service

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, CandidateProfile, EntityEmbedding, Job, JobStatus, Location, RecruiterProfile, User, UserProfile, UserRole}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import java.time.Instant
import java.util.UUID

private[cats] object ServiceFixtures {
  val now: Instant = Instant.parse("2026-09-16T10:15:30Z")
  val later: Instant = Instant.parse("2026-09-16T11:15:30Z")
  val candidateId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  val recruiterId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  val adminId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  val jobId: JobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  val applicationId: ApplicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000005"))

  val candidate: User = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now)
  val recruiter: User = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
  val admin: User = User(adminId, Some("admin@example.com"), "Admin", UserRole.Admin, None, now, adminSingleton = true)
  val openJob: Job = Job(
    jobId,
    recruiterId,
    "Senior Scala Developer",
    "Build backend services",
    List("Scala"),
    Set("Scala"),
    Location("Ukraine", "Kyiv", remote = true),
    JobStatus.Open,
    now,
    now
  )

  val createdApplication: Application = Application.create(applicationId, candidateId, jobId, now)

  private[cats] trait RefBackedLookup[Id, Value] {
    protected def ref: Ref[IO, Map[Id, Value]]

    protected def findOne(id: Id): IO[Option[Value]] =
      ref.get.map(_.get(id))

    protected def findAll(ids: List[Id]): IO[List[Value]] =
      ref.get.map(values => ids.distinct.flatMap(values.get))
  }

  final class InMemoryUsers(protected val ref: Ref[IO, Map[UserId, User]])
      extends UserRepository[IO]
      with RefBackedLookup[UserId, User] {
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] =
      findOne(id).map(Right(_))

    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
      findAll(ids).map(Right(_))

    override def updateEmbedding(id: UserId, observedVersion: Long, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      ref.modify { users =>
        users.get(id) match {
          case Some(user) if user.version == observedVersion => (users + (id -> user.copy(embedding = Some(embedding))), Right(()))
          case None => (users, Left(RepositoryError.Conflict))
          case Some(_) => (users, Left(RepositoryError.Conflict))
        }
      }
  }

  final class InMemoryJobs(protected val ref: Ref[IO, Map[JobId, Job]])
      extends JobRepository[IO]
      with RefBackedLookup[JobId, Job] {
    override def find(id: JobId): IO[Either[RepositoryError, Option[Job]]] =
      findOne(id).map(Right(_))

    override def findMany(ids: List[JobId]): IO[Either[RepositoryError, List[Job]]] =
      findAll(ids).map(Right(_))

    override def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      ref.get.map(_.values.filter { job =>
        job.status == JobStatus.Open &&
          filter.city.forall(_ == job.location.city) &&
          filter.skills.subsetOf(job.skills) &&
          filter.createdAfter.forall(!job.createdAt.isBefore(_)) &&
          matches(page)(job) &&
          keysetAfter(page.cursor.map(cursor => cursor.createdAt -> cursor.id.value.toString))(job)(_.createdAt, _.id.value.toString)
      }.toList).map(keysetPage(_, page.pageSize.value)(_.createdAt, _.id.value.toString)).map(Right(_))

    override def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      ref.get.map(_.values.filter(job =>
        matches(page)(job) &&
          keysetAfter(page.cursor.map(cursor => cursor.createdAt -> cursor.id.value.toString))(job)(_.createdAt, _.id.value.toString)
      ).toList).map(keysetPage(_, page.pageSize.value)(_.createdAt, _.id.value.toString)).map(Right(_))

    override def findByRecruiter(recruiterId: UserId, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]] =
      ref.get.map(_.values.filter(job =>
        job.recruiterId == recruiterId &&
          matches(page)(job) &&
          keysetAfter(page.cursor.map(cursor => cursor.createdAt -> cursor.id.value.toString))(job)(_.createdAt, _.id.value.toString)
      ).toList).map(keysetPage(_, page.pageSize.value)(_.createdAt, _.id.value.toString)).map(Right(_))

    override def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]] =
      ref.update(_ + (job.id -> job)).as(Right(()))

    override def update(job: Job, now: Instant): IO[Either[RepositoryError, Job]] = {
      val persisted = job.copy(version = job.version + 1L)
      ref.update(_ + (job.id -> persisted)).as(Right(persisted))
    }

    override def updateEmbedding(id: JobId, observedVersion: Long, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      ref.modify { jobs =>
        jobs.get(id) match {
          case Some(job) if job.version == observedVersion =>
            (jobs + (id -> job.copy(embedding = Some(embedding))), Right(()))
          case None => (jobs, Left(RepositoryError.Conflict))
          case Some(_) => (jobs, Left(RepositoryError.Conflict))
        }
      }

    private def matches(page: JobPageRequest)(job: Job): Boolean =
      page.status.forall(_ == job.status)

  }

  final class InMemoryApplications(
      applications: Ref[IO, Map[ApplicationId, Application]],
      events: Ref[IO, Vector[ApplicationEvent]],
      nextCreateError: Ref[IO, Option[RepositoryError]]
  ) extends ApplicationRepository[IO] {
    override def find(id: ApplicationId): IO[Either[RepositoryError, Option[Application]]] =
      applications.get.map(_.get(id)).map(Right(_))

    override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
      applications.get.map(_.values.filter(application =>
        application.candidateId == candidateId &&
          matches(page)(application) &&
          keysetAfter(page.cursor.map(cursor => cursor.createdAt -> cursor.id.value.toString))(application)(_.createdAt, _.id.value.toString)
      ).toList).map(keysetPage(_, page.pageSize.value)(_.createdAt, _.id.value.toString)).map(Right(_))

    override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]] =
      applications.get.map(_.values.filter(application =>
        application.jobId == jobId &&
          matches(page)(application) &&
          keysetAfter(page.cursor.map(cursor => cursor.createdAt -> cursor.id.value.toString))(application)(_.createdAt, _.id.value.toString)
      ).toList).map(keysetPage(_, page.pageSize.value)(_.createdAt, _.id.value.toString)).map(Right(_))

    override def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[Either[RepositoryError, List[ApplicationEvent]]] =
      events.get.map(_.filter(event =>
        event.applicationId == applicationId &&
          keysetAfter(page.cursor.map(cursor => cursor.occurredAt -> cursor.id.value.toString))(event)(_.occurredAt, _.id.value.toString)
      ).toList).map(keysetPage(_, page.pageSize.value)(_.occurredAt, _.id.value.toString)).map(Right(_))

    private def create(application: Application, initialEvent: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
      applications.modify { current =>
        if (current.values.exists(existing => existing.candidateId == application.candidateId && existing.jobId == application.jobId)) {
          (current, Left(RepositoryError.DuplicateApplication))
        } else {
          (current + (application.id -> application), Right(()))
        }
      }.flatTap {
        case Right(()) => events.update(_ :+ initialEvent)
        case Left(_) => IO.unit
      }

    override def createForOpenJob(
        observedJob: Job,
        application: Application,
        initialEvent: ApplicationEvent
    ): IO[Either[RepositoryError, Unit]] =
      nextCreateError.modify(error => (None, error)) flatMap {
        case Some(error) => IO.pure(Left(error))
        case None if observedJob.status == JobStatus.Open && observedJob.id == application.jobId &&
            initialEvent.applicationId == application.id =>
          create(application, initialEvent)
        case None => IO.pure(Left(RepositoryError.Conflict))
      }

    override def updateStatus(
        application: Application,
        event: ApplicationEvent
    ): IO[Either[RepositoryError, Unit]] =
      applications.modify { current =>
        current.get(application.id) match {
          case Some(existing) if event.previousStatus.contains(existing.status) =>
            (current + (application.id -> application), Right(()))
          case Some(_) => (current, Left(RepositoryError.Conflict))
          case None => (current, Left(RepositoryError.Unavailable))
        }
      }.flatTap {
        case Right(()) => events.update(_ :+ event)
        case Left(_) => IO.unit
      }

    def allEvents: IO[Vector[ApplicationEvent]] =
      events.get

    def rejectNextCreateWith(error: RepositoryError): IO[Unit] =
      nextCreateError.set(Some(error))

    private def matches(page: ApplicationPageRequest)(application: Application): Boolean =
      page.status.forall(_ == application.status)
  }

  private def keysetAfter[A](cursor: Option[(Instant, String)])(value: A)(timestamp: A => Instant, id: A => String): Boolean =
    cursor.forall { case (cursorTimestamp, cursorId) =>
      timestamp(value).isBefore(cursorTimestamp) ||
        (timestamp(value) == cursorTimestamp && id(value) < cursorId)
    }

  private def keysetPage[A](values: List[A], pageSize: Int)(timestamp: A => Instant, id: A => String): List[A] =
    values.sortWith { (left, right) =>
      timestamp(left).isAfter(timestamp(right)) ||
        (timestamp(left) == timestamp(right) && id(left) > id(right))
    }.take(pageSize)
}
