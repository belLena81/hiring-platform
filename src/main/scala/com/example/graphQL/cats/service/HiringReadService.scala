package com.example.graphQL.cats.service

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.service.protocol.{HiringReadModel, UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.shared.pagination.ApplicationEventPageRequest

final class HiringReadService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository
) extends HiringReadModel {
  private val authorization = ActorAuthorization(users)

  override def user(id: UserId): UseCaseIO[Option[User]] =
    read(users.find(id))

  override def viewer(actor: ActorContext): UseCaseIO[AuthenticatedActor] =
    authorization.resolve(actor).map(AuthenticatedActor(actor, _))

  override def users(ids: List[UserId]): UseCaseIO[List[User]] =
    read(users.findMany(ids))

  override def canViewUserEmail(actor: ActorContext, userId: UserId): UseCaseIO[Boolean] =
    authorization
      .resolve(actor)
      .map(viewer => viewer.id == userId || (viewer.role == UserRole.Admin && viewer.adminSingleton))

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseIO[Set[UserId]] =
    authorization.resolve(actor).map { viewer =>
      if (viewer.role == UserRole.Admin && viewer.adminSingleton) userIds.toSet
      else userIds.filter(_ == viewer.id).toSet
    }

  override def job(id: JobId): UseCaseIO[Option[Job]] =
    read(jobs.find(id))

  override def jobs(ids: List[JobId]): UseCaseIO[List[Job]] =
    read(jobs.findMany(ids))

  override def application(id: ApplicationId): UseCaseIO[Option[Application]] =
    read(applications.find(id))

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseIO[Unit] =
    for {
      user <- authorization.resolve(actor)
      application <- read(applications.find(applicationId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
      _ <-
        if (application.candidateId == user.id && user.role == UserRole.Candidate) UseCase.pure(())
        else if (user.role == UserRole.Admin && user.adminSingleton) UseCase.pure(())
        else if (user.role == UserRole.Recruiter)
          read(jobs.find(application.jobId))
            .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
            .subflatMap(job => Either.cond(job.recruiterId == user.id, (), UseCaseError.Domain(DomainError.Forbidden)))
        else UseCase.left(UseCaseError.Domain(DomainError.Forbidden))
    } yield ()

  override def applicationHistory(
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseIO[List[ApplicationEvent]] =
    read(applications.history(applicationId, page))

  private def read[A](
      value: cats.effect.IO[Either[com.example.graphQL.cats.repository.protocol.RepositoryError, A]]
  ): UseCaseIO[A] =
    UseCase.repository(value)
}

object HiringReadService {
  def apply(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository
  ): HiringReadService =
    new HiringReadService(users, jobs, applications)
}
