package com.example.graphQL.cats.service

import cats.data.EitherT
import cats.Monad
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, UserRole}
import com.example.graphQL.cats.service.port.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.service.protocol.{HiringReadModel, UseCaseF}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.domain.pagination.ApplicationEventPageRequest

import com.example.graphQL.cats.service.read.*

final class HiringReadService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) extends HiringReadModel[F] {
  private val authorization = ActorAuthorization(users)

  override def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]): UseCaseF[F, List[RelatedUser]] =
    authorization
      .readScope(actor)
      .flatMap((_, current) => UseCaseF.repository(users.relatedUsers(current, keys.distinct)))

  override def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]): UseCaseF[F, List[RelatedJob]] =
    authorization
      .readScope(actor)
      .flatMap((_, current) => UseCaseF.repository(jobs.relatedJobs(current, keys.distinct)))

  override def viewer(actor: ActorContext): UseCaseF[F, AuthenticatedActor] =
    authorization.resolve(actor).map(AuthenticatedActor(actor, _))

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): UseCaseF[F, Set[UserId]] =
    authorization.resolve(actor).map { viewer =>
      if (authorization.isAdmin(viewer)) userIds.toSet
      else userIds.filter(_ == viewer.id).toSet
    }

  override def application(id: ApplicationId): UseCaseF[F, Option[Application]] =
    UseCaseF.repository(applications.find(id))

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, Unit] =
    for {
      user <- authorization.resolve(actor)
      application <- UseCaseF.found(applications.find(applicationId), "application")
      _ <-
        if (application.candidateId == user.id && user.role == UserRole.Candidate || authorization.isAdmin(user))
          EitherT.rightT[F, UseCaseError](())
        else if (user.role == UserRole.Recruiter)
          UseCaseF
            .found(jobs.find(application.jobId), "job")
            .flatMap(job =>
              UseCaseF.ensure[F](authorization.canManage(user, job), UseCaseError.Domain(DomainError.Forbidden))
            )
        else EitherT.leftT[F, Unit](UseCaseError.Domain(DomainError.Forbidden))
    } yield ()

  override def applicationHistory(
      actor: ActorContext,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseF[F, List[ApplicationEvent]] =
    for {
      _ <- canViewApplication(actor, applicationId)
      (_, current) <- authorization.readScope(actor)
      values <- UseCaseF.repository(applications.history(current, applicationId, page))
    } yield values
}
