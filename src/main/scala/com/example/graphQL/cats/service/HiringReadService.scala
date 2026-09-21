package com.example.graphQL.cats.service

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, RepositoryError, UserRepository}
import com.example.graphQL.cats.service.protocol.HiringReadModel
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.shared.pagination.ApplicationEventPageRequest

final class HiringReadService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) extends HiringReadModel[F] {
  private val authorization = ActorAuthorization(users)

  override def user(id: UserId): F[Either[UseCaseError, Option[User]]] =
    read(users.find(id))

  override def users(ids: List[UserId]): F[Either[UseCaseError, List[User]]] =
    read(users.findMany(ids))

  override def canViewUserEmail(actor: ActorContext, userId: UserId): F[Either[UseCaseError, Boolean]] =
    authorization.resolve(actor).map {
      case Right(viewer) => Right(viewer.id == userId || (viewer.role == UserRole.Admin && viewer.adminSingleton))
      case Left(error) => Left(error)
    }

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): F[Either[UseCaseError, Set[UserId]]] =
    authorization.resolve(actor).map {
      case Right(viewer) if viewer.role == UserRole.Admin && viewer.adminSingleton => Right(userIds.toSet)
      case Right(viewer) => Right(userIds.filter(_ == viewer.id).toSet)
      case Left(error) => Left(error)
    }

  override def job(id: JobId): F[Either[UseCaseError, Option[Job]]] =
    read(jobs.find(id))

  override def jobs(ids: List[JobId]): F[Either[UseCaseError, List[Job]]] =
    read(jobs.findMany(ids))

  override def application(id: ApplicationId): F[Either[UseCaseError, Option[Application]]] =
    read(applications.find(id))

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): F[Either[UseCaseError, Unit]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => error.asLeft[Unit].pure[F]
      case Right(user) =>
        read(applications.find(applicationId)).flatMap {
          case Left(error) => error.asLeft[Unit].pure[F]
          case Right(None) => UseCaseError.Domain(DomainError.NotFound("application")).asLeft[Unit].pure[F]
          case Right(Some(application)) if application.candidateId == user.id && user.role == UserRole.Candidate =>
            ().asRight[UseCaseError].pure[F]
          case Right(Some(_)) if user.role == UserRole.Admin && user.adminSingleton =>
            ().asRight[UseCaseError].pure[F]
          case Right(Some(application)) if user.role == UserRole.Recruiter =>
            read(jobs.find(application.jobId)).map {
              case Left(error) => Left(error)
              case Right(Some(job)) if job.recruiterId == user.id => Right(())
              case Right(Some(_)) => Left(UseCaseError.Domain(DomainError.Forbidden))
              case Right(None) => Left(UseCaseError.Domain(DomainError.NotFound("job")))
            }
          case Right(Some(_)) => UseCaseError.Domain(DomainError.Forbidden).asLeft[Unit].pure[F]
        }
    }

  override def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): F[Either[UseCaseError, List[ApplicationEvent]]] =
    read(applications.history(applicationId, page))

  private def read[A](value: F[Either[RepositoryError, A]]): F[Either[UseCaseError, A]] =
    value.map(_.leftMap(UseCaseError.Repository.apply))
}

object HiringReadService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      applications: ApplicationRepository[F]
  ): HiringReadService[F] =
    new HiringReadService(users, jobs, applications)
}
