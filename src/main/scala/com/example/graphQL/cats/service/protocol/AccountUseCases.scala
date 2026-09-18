package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import java.time.Instant

final case class SignUpInput(name: String, role: UserRole, password: String, profile: Option[UserProfile])
final case class BootstrapAdminInput(name: String, password: String)
final case class LoginInput(name: String, password: String)
final case class AccountProfileInput(profile: UserProfile)

trait AccountUseCases[F[_]] {
  def signUp(input: SignUpInput, now: Instant, userId: Identifiers.UserId): F[Either[UseCaseError, (User, AccountToken)]]
  def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: Identifiers.UserId): F[Either[UseCaseError, (User, AccountToken)]]
  def login(input: LoginInput, now: Instant): F[Either[UseCaseError, (User, AccountToken)]]
  def me(actor: ActorContext): F[Either[UseCaseError, User]]
  def updateMyProfile(actor: ActorContext, input: AccountProfileInput): F[Either[UseCaseError, User]]
  def deleteMyAccount(actor: ActorContext, now: Instant): F[Either[UseCaseError, Unit]]
  def listUsers(actor: ActorContext, page: UserPageRequest): F[Either[UseCaseError, List[User]]]
}
