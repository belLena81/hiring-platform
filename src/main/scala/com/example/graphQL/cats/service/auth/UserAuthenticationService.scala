package com.example.graphQL.cats.service.auth

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.UserRepository
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.protocol.UserAuthenticator

final class UserAuthenticationService(users: UserRepository) extends UserAuthenticator {
  override def actorFor(userId: UserId): IO[Either[RepositoryError, Option[ActorContext]]] =
    users.find(userId).map(_.map(_.filter(_.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active)
      .map(user => ActorContext(user.id, user.role))))
}

object UserAuthenticationService {
  def apply(users: UserRepository): UserAuthenticationService =
    new UserAuthenticationService(users)
}
