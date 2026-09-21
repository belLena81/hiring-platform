package com.example.graphQL.cats.service.auth

import cats.Functor
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.UserRepository
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.protocol.UserAuthenticator

final class UserAuthenticationService[F[_]: Functor](users: UserRepository[F]) extends UserAuthenticator[F] {
  override def actorFor(userId: UserId): F[Either[RepositoryError, Option[ActorContext]]] =
    users.find(userId).map(_.map(_.filter(_.accountStatus == com.example.graphQL.cats.domain.model.AccountStatus.Active)
      .map(user => ActorContext(user.id, user.role))))
}

object UserAuthenticationService {
  def apply[F[_]: Functor](users: UserRepository[F]): UserAuthenticationService[F] =
    new UserAuthenticationService(users)
}
