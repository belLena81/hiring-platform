package com.example.graphQL.cats.service.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import java.time.Instant

final case class SignUpInput(name: String, role: UserRole, password: String, profile: Option[UserProfile])
final case class BootstrapAdminInput(name: String, password: String)
final case class LoginInput(name: String, password: String)
final case class AccountProfileInput(profile: UserProfile)

trait AccountUseCases {
  def signUp(input: SignUpInput, now: Instant, userId: Identifiers.UserId): IO[Either[UseCaseError, (User, AccountToken)]]
  def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: Identifiers.UserId): IO[Either[UseCaseError, (User, AccountToken)]]
  def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]]
  def issueToken(userId: Identifiers.UserId, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
    {
      val _ = (userId, now)
      IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable)))
    }
  def me(actor: ActorContext): IO[Either[UseCaseError, User]]
  def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]]
  def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]]
  def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]]
}
