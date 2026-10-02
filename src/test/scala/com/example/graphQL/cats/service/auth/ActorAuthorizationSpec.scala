package com.example.graphQL.cats.service.auth

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{AccountStatus, EntityEmbedding, User, UserRole}
import com.example.graphQL.cats.service.port.{RepositoryIO, UserRepository}
import com.example.graphQL.cats.service.{ActorContext, AuthenticationError, ServiceFixtures, UseCaseError}
import munit.CatsEffectSuite

final class ActorAuthorizationSpec extends CatsEffectSuite {
  private val activeUser = ServiceFixtures.recruiter
  private val deletedUser = activeUser.copy(accountStatus = AccountStatus.Deleted, profile = None)

  test("resolve rejects a deleted user even when the actor role matches") {
    val authorization = ActorAuthorization(repository(Map(deletedUser.id -> deletedUser)))

    authorization.resolve(ActorContext(deletedUser.id, UserRole.Recruiter)).value.map { result =>
      assertEquals(result, Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
    }
  }

  test("resolve accepts an active user with a matching actor role") {
    val authorization = ActorAuthorization(repository(Map(activeUser.id -> activeUser)))

    authorization.resolve(ActorContext(activeUser.id, UserRole.Recruiter)).value.map { result =>
      assertEquals(result, Right(activeUser))
    }
  }

  private def repository(values: Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]): UserRepository =
    new ServiceFixtures.VersionedUserRepositoryTestAdapter {
      override def find(
          id: com.example.graphQL.cats.domain.model.Identifiers.UserId
      ): RepositoryIO[Option[User]] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(values.get(id))))

      override def findMany(
          ids: List[com.example.graphQL.cats.domain.model.Identifiers.UserId]
      ): RepositoryIO[List[User]] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(ids.flatMap(values.get))))

      override def updateEmbedding(
          id: com.example.graphQL.cats.domain.model.Identifiers.UserId,
          embedding: EntityEmbedding
      ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))
    }
}
