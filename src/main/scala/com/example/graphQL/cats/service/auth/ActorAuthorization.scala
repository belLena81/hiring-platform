package com.example.graphQL.cats.service.auth

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{ActorRecord, JobOwner, RepositoryF, UserRepository}
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.service.protocol.UseCaseF
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{AccountStatus, JobStatus, User, UserRole}

/** Authorization decisions over the actor projection. An [[AuthenticatedActor]] already carries the projection read for
  * this request, so resolving it again costs no store read.
  */
final class ActorAuthorization[F[_]: Monad](users: UserRepository[F]) {
  def resolve(actor: ActorContext, allowDeleted: Boolean = false): UseCaseF[F, ActorRecord] =
    actor match {
      case authenticated: AuthenticatedActor =>
        EitherT.fromEither[F](validate(authenticated.claims, authenticated.viewer, allowDeleted))
      case _ => persisted(actor).subflatMap(validate(actor, _, allowDeleted))
    }

  /** The live Admin record, never the token claims: Active, singleton Admin only. */
  def requireAdmin(actor: ActorContext): UseCaseF[F, ActorRecord] =
    persisted(actor).subflatMap(record =>
      Either.cond(
        record.accountStatus == AccountStatus.Active && isAdmin(record),
        record,
        UseCaseError.Authentication(AuthenticationError.Unauthorized)
      )
    )

  /** Only a validated actor projection can grant a repository read scope; it reuses the request's resolved actor. */
  def readScope(actor: ActorContext): UseCaseF[F, (ActorRecord, HiringReadScope)] =
    resolve(actor).flatMap(record => scopeOf(actor, record).tupleLeft(record))

  /** Like [[readScope]], but always re-reads the actor, for revalidating after a long step such as a provider call: a
    * request-cached actor must not outlive the account it was read from.
    */
  def currentReadScope(actor: ActorContext): UseCaseF[F, (ActorRecord, HiringReadScope)] =
    persisted(actor)
      .subflatMap(validate(actor, _))
      .flatMap(record => scopeOf(actor, record).tupleLeft(record))

  /** The read scope of an actor already resolved by [[resolve]]. */
  def scopeOf(actor: ActorContext, record: ActorRecord): UseCaseF[F, HiringReadScope] =
    EitherT.fromEither[F](HiringReadScope.validated(actor, record, this))

  /** The actor's own user with its profile, for operations that return or edit it. No embedding is read. */
  def resolveProfile(actor: ActorContext, allowDeleted: Boolean = false): UseCaseF[F, User] =
    loaded(users.find(actor.userId)).subflatMap(user => validate(actor, ActorRecord.of(user), allowDeleted).as(user))

  /** The actor's own user including its embedding, for search ranking that compares the candidate embedding. */
  def resolveWithEmbedding(actor: ActorContext): UseCaseF[F, User] =
    loaded(users.findVersioned(actor.userId).map(_.map(_.value))).subflatMap(user =>
      validate(actor, ActorRecord.of(user), allowDeleted = false).as(user)
    )

  /** [[resolveWithEmbedding]] together with the read scope it grants. */
  def readScopeWithEmbedding(actor: ActorContext): UseCaseF[F, (User, HiringReadScope)] =
    resolveWithEmbedding(actor).flatMap(user => scopeOf(actor, ActorRecord.of(user)).tupleLeft(user))

  def validate(
      actor: ActorContext,
      record: ActorRecord,
      allowDeleted: Boolean = false
  ): Either[UseCaseError, ActorRecord] =
    if (record.accountStatus != AccountStatus.Active && !allowDeleted)
      UseCaseError.Authentication(AuthenticationError.Unauthorized).asLeft[ActorRecord]
    else if (record.role != actor.role) UseCaseError.Domain(DomainError.Forbidden).asLeft[ActorRecord]
    else if (record.role == UserRole.Admin && !record.adminSingleton)
      UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation).asLeft[ActorRecord]
    else record.asRight[UseCaseError]

  def isAdmin(record: ActorRecord): Boolean = record.role == UserRole.Admin && record.adminSingleton

  def canManageJobs(record: ActorRecord): Boolean = record.role == UserRole.Recruiter || isAdmin(record)

  def canManage(record: ActorRecord, job: JobOwner): Boolean =
    isAdmin(record) || (record.role == UserRole.Recruiter && job.recruiterId == record.id)

  def canView(record: ActorRecord, job: JobOwner): Boolean =
    job.status == JobStatus.Open || canManage(record, job)

  private def persisted(actor: ActorContext): UseCaseF[F, ActorRecord] =
    loaded(users.findActor(actor.userId))

  private def loaded[A](read: RepositoryF[F, Option[A]]): UseCaseF[F, A] =
    UseCaseF
      .repository(read)
      .subflatMap(_.toRight(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
}
