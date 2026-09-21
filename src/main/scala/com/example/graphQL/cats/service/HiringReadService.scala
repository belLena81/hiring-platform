package com.example.graphQL.cats.service

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, RepositoryError, UserRepository}
import com.example.graphQL.cats.service.protocol.HiringReadModel
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.shared.pagination.ApplicationEventPageRequest

final class HiringReadService(
    users: UserRepository[IO],
    jobs: JobRepository[IO],
    applications: ApplicationRepository[IO]
) extends HiringReadModel {
  private val authorization = ActorAuthorization(users)

  override def user(id: UserId): IO[Either[UseCaseError, Option[User]]] =
    read(users.find(id))

  override def viewer(actor: ActorContext): IO[Either[UseCaseError, AuthenticatedActor]] =
    authorization.resolve(actor).map(_.map(AuthenticatedActor(actor, _)))

  override def users(ids: List[UserId]): IO[Either[UseCaseError, List[User]]] =
    read(users.findMany(ids))

  override def canViewUserEmail(actor: ActorContext, userId: UserId): IO[Either[UseCaseError, Boolean]] =
    authorization.resolve(actor).map {
      case Right(viewer) => Right(viewer.id == userId || (viewer.role == UserRole.Admin && viewer.adminSingleton))
      case Left(error) => Left(error)
    }

  override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]): IO[Either[UseCaseError, Set[UserId]]] =
    authorization.resolve(actor).map {
      case Right(viewer) if viewer.role == UserRole.Admin && viewer.adminSingleton => Right(userIds.toSet)
      case Right(viewer) => Right(userIds.filter(_ == viewer.id).toSet)
      case Left(error) => Left(error)
    }

  override def job(id: JobId): IO[Either[UseCaseError, Option[Job]]] =
    read(jobs.find(id))

  override def jobs(ids: List[JobId]): IO[Either[UseCaseError, List[Job]]] =
    read(jobs.findMany(ids))

  override def application(id: ApplicationId): IO[Either[UseCaseError, Option[Application]]] =
    read(applications.find(id))

  override def canViewApplication(actor: ActorContext, applicationId: ApplicationId): IO[Either[UseCaseError, Unit]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => IO.pure(error.asLeft[Unit])
      case Right(user) =>
        read(applications.find(applicationId)).flatMap {
          case Left(error) => IO.pure(error.asLeft[Unit])
          case Right(None) => IO.pure(UseCaseError.Domain(DomainError.NotFound("application")).asLeft[Unit])
          case Right(Some(application)) if application.candidateId == user.id && user.role == UserRole.Candidate =>
            IO.pure(().asRight[UseCaseError])
          case Right(Some(_)) if user.role == UserRole.Admin && user.adminSingleton =>
            IO.pure(().asRight[UseCaseError])
          case Right(Some(application)) if user.role == UserRole.Recruiter =>
            read(jobs.find(application.jobId)).map {
              case Left(error) => Left(error)
              case Right(Some(job)) if job.recruiterId == user.id => Right(())
              case Right(Some(_)) => Left(UseCaseError.Domain(DomainError.Forbidden))
              case Right(None) => Left(UseCaseError.Domain(DomainError.NotFound("job")))
            }
          case Right(Some(_)) => IO.pure(UseCaseError.Domain(DomainError.Forbidden).asLeft[Unit])
        }
    }

  override def applicationHistory(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[Either[UseCaseError, List[ApplicationEvent]]] =
    read(applications.history(applicationId, page))

  private def read[A](value: IO[Either[RepositoryError, A]]): IO[Either[UseCaseError, A]] =
    value.map(_.leftMap(UseCaseError.Repository.apply))
}

object HiringReadService {
  def apply(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      applications: ApplicationRepository[IO]
  ): HiringReadService =
    new HiringReadService(users, jobs, applications)
}
