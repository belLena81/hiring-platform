package com.example.graphQL.cats.service.auth

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.UserRepository
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.service.protocol.UseCaseIO
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{AccountStatus, Job, JobStatus, User, UserRole}

final class ActorAuthorization(users: UserRepository) {
  def resolve(actor: ActorContext, allowDeleted: Boolean = false): UseCaseIO[User] =
    actor match {
      case authenticated: AuthenticatedActor =>
        EitherT.fromEither[IO](validate(authenticated.claims, authenticated.viewer, allowDeleted))
      case _ => persisted(actor).subflatMap(validate(actor, _, allowDeleted))
    }

  /** The live Admin record, never the token claims: Active, singleton Admin only. */
  def requireAdmin(actor: ActorContext): UseCaseIO[User] =
    persisted(actor).subflatMap(user =>
      Either.cond(
        user.accountStatus == AccountStatus.Active && isAdmin(user),
        user,
        UseCaseError.Authentication(AuthenticationError.Unauthorized)
      )
    )

  /** Only a freshly persisted, validated actor can grant a repository read scope. */
  def readScope(actor: ActorContext): UseCaseIO[(User, HiringReadScope)] =
    persisted(actor).subflatMap(user => HiringReadScope.validated(actor, user, this).tupleLeft(user))

  def validate(actor: ActorContext, user: User, allowDeleted: Boolean = false): Either[UseCaseError, User] =
    if (user.accountStatus != AccountStatus.Active && !allowDeleted)
      UseCaseError.Authentication(AuthenticationError.Unauthorized).asLeft[User]
    else if (user.role != actor.role) UseCaseError.Domain(DomainError.Forbidden).asLeft[User]
    else if (user.role == UserRole.Admin && !user.adminSingleton)
      UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation).asLeft[User]
    else user.asRight[UseCaseError]

  def isAdmin(user: User): Boolean = user.role == UserRole.Admin && user.adminSingleton

  def canManageJobs(user: User): Boolean = user.role == UserRole.Recruiter || isAdmin(user)

  def canManage(user: User, job: Job): Boolean =
    isAdmin(user) || (user.role == UserRole.Recruiter && job.recruiterId == user.id)

  def canView(user: User, job: Job): Boolean =
    job.status == JobStatus.Open || canManage(user, job)

  private def persisted(actor: ActorContext): UseCaseIO[User] =
    UseCaseIO
      .repository(users.find(actor.userId))
      .subflatMap(_.toRight(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
}

object ActorAuthorization {
  def apply(users: UserRepository): ActorAuthorization =
    new ActorAuthorization(users)
}
