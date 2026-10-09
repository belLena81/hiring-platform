package com.example.graphQL.cats.service

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, UserRole}
import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, RepositoryIO, UserRepository}
import com.example.graphQL.cats.service.protocol.{HiringReadModel, UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.domain.pagination.ApplicationEventPageRequest

import com.example.graphQL.cats.service.read.*

final class HiringReadService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository
) extends HiringReadModel {
  private val authorization = ActorAuthorization(users)

  private def scope(actor: ActorContext): UseCaseIO[HiringReadScope] =
    read(users.find(actor.userId))
      .subflatMap(_.toRight(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
      .subflatMap(HiringReadScope.validated(actor, _, authorization))

  override def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]): UseCaseIO[List[RelatedUser]] =
    scope(actor).flatMap(current => read(users.relatedUsers(current, keys.distinct)))

  override def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]): UseCaseIO[List[RelatedJob]] =
    scope(actor).flatMap(current => read(jobs.relatedJobs(current, keys.distinct)))

  override def viewer(actor: ActorContext): UseCaseIO[AuthenticatedActor] =
    authorization.resolve(actor).map(AuthenticatedActor(actor, _))

  override def canViewUserEmail(actor: ActorContext, userId: UserId): UseCaseIO[Boolean] =
    authorization
      .resolve(actor)
      .map(viewer => viewer.id == userId || (viewer.role == UserRole.Admin && viewer.adminSingleton))

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseIO[Set[UserId]] =
    authorization.resolve(actor).map { viewer =>
      if (viewer.role == UserRole.Admin && viewer.adminSingleton) userIds.toSet
      else userIds.filter(_ == viewer.id).toSet
    }

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
      actor: ActorContext,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseIO[List[ApplicationEvent]] =
    for {
      _ <- canViewApplication(actor, applicationId)
      current <- scope(actor)
      values <- read(applications.history(current, applicationId, page))
    } yield values

  private def read[A](value: RepositoryIO[A]): UseCaseIO[A] =
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
