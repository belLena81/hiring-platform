package com.example.graphQL.cats.service.auth

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.UserRepository
import com.example.graphQL.cats.service.{ActorContext, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{AccountStatus, Job, JobStatus, User, UserRole}

final class ActorAuthorization[F[_]: Monad](users: UserRepository[F]) {
  def resolve(actor: ActorContext): F[Either[UseCaseError, User]] =
    users.find(actor.userId).map {
      case None => UseCaseError.authentication(AuthenticationError.Unauthorized).asLeft[User]
      case Some(user) if user.accountStatus != AccountStatus.Active =>
        UseCaseError.authentication(AuthenticationError.Unauthorized).asLeft[User]
      case Some(user) if user.role != actor.role => UseCaseError.domain(DomainError.Forbidden).asLeft[User]
      case Some(user) if user.role == UserRole.Admin && !user.adminSingleton =>
        UseCaseError.authentication(AuthenticationError.SingletonAdminViolation).asLeft[User]
      case Some(user) => user.asRight[UseCaseError]
    }

  def canManageJobs(user: User): Boolean =
    user.role == UserRole.Recruiter || (user.role == UserRole.Admin && user.adminSingleton)

  def canManage(user: User, job: Job): Boolean =
    (user.role == UserRole.Admin && user.adminSingleton) || (user.role == UserRole.Recruiter && job.recruiterId == user.id)

  def canView(user: User, job: Job): Boolean =
    job.status == JobStatus.Open || canManage(user, job)
}

object ActorAuthorization {
  def apply[F[_]: Monad](users: UserRepository[F]): ActorAuthorization[F] =
    new ActorAuthorization(users)
}
