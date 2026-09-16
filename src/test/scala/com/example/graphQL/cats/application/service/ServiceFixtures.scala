package com.example.graphQL.cats.application.service

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.application.port.{
  ApplicationPageRequest, ApplicationRepository, JobRepository, RepositoryError, UserRepository
}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, JobStatus, Location, User, UserRole}
import java.time.Instant
import java.util.UUID

private[service] object ServiceFixtures {
  val now: Instant = Instant.parse("2026-09-16T10:15:30Z")
  val later: Instant = Instant.parse("2026-09-16T11:15:30Z")
  val candidateId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  val recruiterId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  val adminId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  val jobId: JobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  val applicationId: ApplicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000005"))

  val candidate: User = User(candidateId, "candidate@example.com", "Candidate", UserRole.Candidate, None, now)
  val recruiter: User = User(recruiterId, "recruiter@example.com", "Recruiter", UserRole.Recruiter, None, now)
  val admin: User = User(adminId, "admin@example.com", "Admin", UserRole.Admin, None, now, adminSingleton = true)
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

  final class InMemoryUsers(ref: Ref[IO, Map[UserId, User]]) extends UserRepository[IO] {
    override def find(id: UserId): IO[Option[User]] =
      ref.get.map(_.get(id))
  }

  final class InMemoryJobs(ref: Ref[IO, Map[JobId, Job]]) extends JobRepository[IO] {
    override def find(id: JobId): IO[Option[Job]] =
      ref.get.map(_.get(id))

    override def create(job: Job): IO[Either[RepositoryError, Unit]] =
      ref.update(_ + (job.id -> job)).as(Right(()))

    override def update(job: Job): IO[Either[RepositoryError, Unit]] =
      ref.update(_ + (job.id -> job)).as(Right(()))
  }

  final class InMemoryApplications(
      applications: Ref[IO, Map[ApplicationId, Application]],
      events: Ref[IO, Vector[ApplicationEvent]]
  ) extends ApplicationRepository[IO] {
    override def find(id: ApplicationId): IO[Option[Application]] =
      applications.get.map(_.get(id))

    override def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[List[Application]] =
      applications.get.map(_.values.filter(_.candidateId == candidateId).filter(matches(page)).toList)

    override def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[List[Application]] =
      applications.get.map(_.values.filter(_.jobId == jobId).filter(matches(page)).toList)

    override def create(application: Application, initialEvent: ApplicationEvent): IO[Either[RepositoryError, Unit]] =
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

    override def updateStatus(
        application: Application,
        event: ApplicationEvent
    ): IO[Either[RepositoryError, Unit]] =
      applications.update(_ + (application.id -> application)).as(Right(())) <* events.update(_ :+ event)

    def allEvents: IO[Vector[ApplicationEvent]] =
      events.get

    private def matches(page: ApplicationPageRequest)(application: Application): Boolean =
      page.status.forall(_ == application.status)
  }
}
