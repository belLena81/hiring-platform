package com.example.graphQL.cats.service.auth

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{RepositoryIO, UserRepository}
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.protocol.UserAuthenticator

final class UserAuthenticationService(users: UserRepository) extends UserAuthenticator {
  override def actorFor(userId: UserId): RepositoryIO[Option[ActorContext]] =
    users
      .find(userId)
      .map(
        _.filter(_.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active)
          .map(user => ActorContext(user.id, user.role))
      )

  override def actorForVerifiedToken(userId: UserId): RepositoryIO[Option[ActorContext]] =
    users.find(userId).map(_.map(user => ActorContext(user.id, user.role)))
}

object UserAuthenticationService {
  def apply(users: UserRepository): UserAuthenticationService =
    new UserAuthenticationService(users)
}
