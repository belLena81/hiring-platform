package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ActorContext

final case class SignUpInput(name: String, role: UserRole, password: String, profile: Option[UserProfile])
final case class LoginInput(name: String, password: String)
final case class AccountProfileInput(profile: UserProfile)

trait AccountUseCases[F[_]] {
  def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseF[F, (User, AccountToken)]
  def login(request: IdempotencyRequest, input: LoginInput): UseCaseF[F, (User, AccountToken)]
  def me(actor: ActorContext): UseCaseF[F, User]
  def updateMyProfile(request: IdempotencyRequest, actor: ActorContext, input: AccountProfileInput): UseCaseF[F, User]
  def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseF[F, String]
  def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseF[F, AccountDeletionStatus]
  def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseF[F, List[User]]
}
