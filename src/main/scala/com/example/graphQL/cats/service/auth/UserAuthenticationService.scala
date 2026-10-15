package com.example.graphQL.cats.service.auth

import cats.Functor

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{RepositoryF, UserRepository}
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.protocol.UserAuthenticator

final class UserAuthenticationService[F[_]: Functor](users: UserRepository[F]) extends UserAuthenticator[F] {
  override def actorFor(userId: UserId): RepositoryF[F, Option[ActorContext]] =
    users
      .find(userId)
      .map(
        _.filter(_.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active)
          .map(user => ActorContext(user.id, user.role))
      )

  override def actorForVerifiedToken(userId: UserId): RepositoryF[F, Option[ActorContext]] =
    users.find(userId).map(_.map(user => ActorContext(user.id, user.role)))
}
