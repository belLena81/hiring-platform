package com.example.graphQL.cats.service.auth

import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.UserRepository
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{AccountStatus, Job, JobStatus, User, UserRole}

final class ActorAuthorization(users: UserRepository) {
  def resolve(actor: ActorContext, allowDeleted: Boolean = false): UseCaseIO[User] =
    actor match {
      case authenticated: AuthenticatedActor =>
        UseCase.fromEither(validate(authenticated.claims, authenticated.viewer, allowDeleted))
      case _ =>
        UseCase
          .repository(users.find(actor.userId))
          .subflatMap(_.toRight(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
          .subflatMap(validate(actor, _, allowDeleted))
    }

  def validate(actor: ActorContext, user: User, allowDeleted: Boolean = false): Either[UseCaseError, User] =
    if (user.accountStatus != AccountStatus.Active && !allowDeleted)
      UseCaseError.Authentication(AuthenticationError.Unauthorized).asLeft[User]
    else if (user.role != actor.role) UseCaseError.Domain(DomainError.Forbidden).asLeft[User]
    else if (user.role == UserRole.Admin && !user.adminSingleton)
      UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation).asLeft[User]
    else user.asRight[UseCaseError]

  def canManageJobs(user: User): Boolean =
    user.role == UserRole.Recruiter || (user.role == UserRole.Admin && user.adminSingleton)

  def canManage(user: User, job: Job): Boolean =
    (user.role == UserRole.Admin && user.adminSingleton) || (user.role == UserRole.Recruiter && job.recruiterId == user.id)

  def canView(user: User, job: Job): Boolean =
    job.status == JobStatus.Open || canManage(user, job)
}

object ActorAuthorization {
  def apply(users: UserRepository): ActorAuthorization =
    new ActorAuthorization(users)
}
