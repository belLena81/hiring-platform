package com.example.graphQL.cats.service

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, UserRole}
import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, UserRepository}
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

  override def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]): UseCaseIO[List[RelatedUser]] =
    authorization
      .readScope(actor)
      .flatMap((_, current) => UseCase.repository(users.relatedUsers(current, keys.distinct)))

  override def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]): UseCaseIO[List[RelatedJob]] =
    authorization.readScope(actor).flatMap((_, current) => UseCase.repository(jobs.relatedJobs(current, keys.distinct)))

  override def viewer(actor: ActorContext): UseCaseIO[AuthenticatedActor] =
    authorization.resolve(actor).map(AuthenticatedActor(actor, _))

  override def canViewUserEmail(actor: ActorContext, userId: UserId): UseCaseIO[Boolean] =
    authorization
      .resolve(actor)
      .map(viewer => viewer.id == userId || authorization.isAdmin(viewer))

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseIO[Set[UserId]] =
    authorization.resolve(actor).map { viewer =>
      if (authorization.isAdmin(viewer)) userIds.toSet
      else userIds.filter(_ == viewer.id).toSet
    }

  override def application(id: ApplicationId): UseCaseIO[Option[Application]] =
    UseCase.repository(applications.find(id))

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseIO[Unit] =
    for {
      user <- authorization.resolve(actor)
      application <- UseCase.found(applications.find(applicationId), "application")
      _ <-
        if (application.candidateId == user.id && user.role == UserRole.Candidate) EitherT.rightT[IO, UseCaseError](())
        else if (authorization.isAdmin(user)) EitherT.rightT[IO, UseCaseError](())
        else if (user.role == UserRole.Recruiter)
          UseCase
            .found(jobs.find(application.jobId), "job")
            .flatMap(job => UseCase.ensure(job.recruiterId == user.id, UseCaseError.Domain(DomainError.Forbidden)))
        else EitherT.leftT[IO, Unit](UseCaseError.Domain(DomainError.Forbidden))
    } yield ()

  override def applicationHistory(
      actor: ActorContext,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseIO[List[ApplicationEvent]] =
    for {
      _ <- canViewApplication(actor, applicationId)
      (_, current) <- authorization.readScope(actor)
      values <- UseCase.repository(applications.history(current, applicationId, page))
    } yield values
}

object HiringReadService {
  def apply(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository
  ): HiringReadService =
    new HiringReadService(users, jobs, applications)
}
