package com.example.graphQL.cats.service.read

import com.example.graphQL.cats.domain.model.{User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization

/** Only a freshly persisted, validated actor can grant a repository read scope. */
final class HiringReadScope private (val userId: UserId, val role: UserRole)
object HiringReadScope {
  def validated(
      actor: ActorContext,
      user: User,
      authorization: ActorAuthorization
  ): Either[UseCaseError, HiringReadScope] =
    authorization.validate(actor, user).map(current => new HiringReadScope(current.id, current.role))
}

enum UserRelationKey {
  case ApplicationCandidate(applicationId: ApplicationId, userId: UserId)
  case JobRecruiter(jobId: JobId, userId: UserId)
}
final case class JobRelationKey(applicationId: ApplicationId, jobId: JobId)
final case class RelatedUser(key: UserRelationKey, value: User)
final case class RelatedJob(key: JobRelationKey, value: com.example.graphQL.cats.domain.model.Job)
