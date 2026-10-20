package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{
  AccountCredentials,
  EntityEmbedding,
  PasswordHash,
  User,
  UserPageRequest,
  UserProfile
}
import com.example.graphQL.cats.service.read.*
import java.time.Instant

trait UserRepository[F[_]] {
  def relatedUsers(scope: HiringReadScope, keys: List[UserRelationKey]): RepositoryF[F, List[RelatedUser]]

  /** The authorization projection of a user (see [[ActorRecord]]); never carries a profile, credential or embedding. */
  def findActor(id: UserId): RepositoryF[F, Option[ActorRecord]]

  /** The user with its profile. The embedding vector and the credential are not read: `embedding` is always `None`. */
  def find(id: UserId): RepositoryF[F, Option[User]]

  /** The whole user including its embedding, for the embedding pipeline and search ranking, with its revision. */
  def findVersioned(id: UserId): RepositoryF[F, Option[Versioned[User]]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryF[F, Unit]
  def updateEmbedding(observed: Versioned[User], embedding: EntityEmbedding): RepositoryF[F, Unit]
}

trait UserAccountRepository[F[_]] {
  def bootstrap(
      user: User,
      passwordHash: PasswordHash,
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
  def initialized: RepositoryF[F, Boolean]
  def createAccount(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
  def findByCanonicalName(nameCanonical: String): RepositoryF[F, Option[AccountCredentials]]
  def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryF[F, User]
  def listAccounts(page: UserPageRequest): RepositoryF[F, List[User]]
  def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext
  ): RepositoryF[F, Unit]
}
