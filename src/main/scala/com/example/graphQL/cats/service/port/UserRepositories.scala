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

trait UserRepository {
  def relatedUsers(scope: HiringReadScope, keys: List[UserRelationKey]): RepositoryIO[List[RelatedUser]]
  def find(id: UserId): RepositoryIO[Option[User]]
  def findVersioned(id: UserId): RepositoryIO[Option[Versioned[User]]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryIO[Unit]
  def updateEmbedding(observed: Versioned[User], embedding: EntityEmbedding): RepositoryIO[Unit]
}

trait UserAccountRepository {
  def bootstrap(
      user: User,
      passwordHash: PasswordHash,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def initialized: RepositoryIO[Boolean]
  def createAccount(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def findByCanonicalName(nameCanonical: String): RepositoryIO[Option[AccountCredentials]]
  def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[User]
  def listAccounts(page: UserPageRequest): RepositoryIO[List[User]]
  def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
}
