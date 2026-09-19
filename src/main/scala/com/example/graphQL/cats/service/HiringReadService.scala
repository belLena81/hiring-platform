package com.example.graphQL.cats.service

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.service.protocol.HiringReadModel
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.shared.pagination.ApplicationEventPageRequest

final class HiringReadService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) extends HiringReadModel[F] {
  private val authorization = ActorAuthorization(users)

  override def user(id: UserId): F[Option[User]] =
    users.find(id)

  override def users(ids: List[UserId]): F[List[User]] =
    users.findMany(ids)

  override def canViewUserEmail(actor: ActorContext, userId: UserId): F[Boolean] =
    authorization.resolve(actor).map {
      case Right(viewer) => viewer.id == userId || (viewer.role == UserRole.Admin && viewer.adminSingleton)
      case Left(_) => false
    }

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): F[Set[UserId]] =
    authorization.resolve(actor).map {
      case Right(viewer) if viewer.role == UserRole.Admin && viewer.adminSingleton => userIds.toSet
      case Right(viewer) => userIds.filter(_ == viewer.id).toSet
      case Left(_) => Set.empty
    }

  override def job(id: JobId): F[Option[Job]] =
    jobs.find(id)

  override def jobs(ids: List[JobId]): F[List[Job]] =
    jobs.findMany(ids)

  override def application(id: ApplicationId): F[Option[Application]] =
    applications.find(id)

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): F[Either[UseCaseError, Unit]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => error.asLeft[Unit].pure[F]
      case Right(user) =>
        applications.find(applicationId).flatMap {
          case None => UseCaseError.domain(DomainError.NotFound("application")).asLeft[Unit].pure[F]
          case Some(application) if application.candidateId == user.id && user.role == UserRole.Candidate =>
            ().asRight[UseCaseError].pure[F]
          case Some(_) if user.role == UserRole.Admin && user.adminSingleton =>
            ().asRight[UseCaseError].pure[F]
          case Some(application) if user.role == UserRole.Recruiter =>
            jobs.find(application.jobId).map {
              case Some(job) if job.recruiterId == user.id => Right(())
              case Some(_) => Left(UseCaseError.domain(DomainError.Forbidden))
              case None => Left(UseCaseError.domain(DomainError.NotFound("job")))
            }
          case Some(_) => UseCaseError.domain(DomainError.Forbidden).asLeft[Unit].pure[F]
        }
    }

  override def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): F[List[ApplicationEvent]] =
    applications.history(applicationId, page)
}

object HiringReadService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      applications: ApplicationRepository[F]
  ): HiringReadService[F] =
    new HiringReadService(users, jobs, applications)
}
