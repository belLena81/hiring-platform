package com.example.graphQL.cats.service

import cats.data.EitherT
import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, UserRole}
import com.example.graphQL.cats.service.application.AuthorizedApplicationAccess
import com.example.graphQL.cats.service.port.{ActorRecord, ApplicationRepository, JobRepository, UserRepository}
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
  private val accessible = AuthorizedApplicationAccess(authorization, jobs, applications)

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

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): UseCaseF[F, Application] =
    authorization.resolve(actor).flatMap(canView(_, applicationId))

  /** The owning Candidate and Admin view any loaded application; a Recruiter must manage its job. */
  private def canView(record: ActorRecord, applicationId: ApplicationId): UseCaseF[F, Application] =
    UseCaseF.found(applications.find(applicationId), "application").flatMap { application =>
      if (application.candidateId == record.id && record.role == UserRole.Candidate || authorization.isAdmin(record))
        EitherT.rightT[F, UseCaseError](application)
      else if (record.role == UserRole.Recruiter) accessible.manageLoaded(record, application).as(application)
      else UseCaseF.forbidUnless[F](false).as(application)
    }

  override def applicationHistory(
      actor: ActorContext,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): UseCaseF[F, List[ApplicationEvent]] =
    for {
      record <- authorization.resolve(actor)
      _ <- canView(record, applicationId)
      scope <- authorization.scopeOf(actor, record)
      values <- UseCaseF.repository(applications.history(scope, applicationId, page))
    } yield values
}
