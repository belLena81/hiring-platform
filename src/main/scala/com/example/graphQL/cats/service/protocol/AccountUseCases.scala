package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.ActorContext

final case class SignUpInput(name: String, role: UserRole, password: String, profile: Option[UserProfile])
final case class BootstrapAdminInput(name: String, password: String)
final case class LoginInput(name: String, password: String)
final case class AccountProfileInput(profile: UserProfile)

trait AccountUseCases {
  def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)]
  def bootstrapAdmin(request: IdempotencyRequest, input: BootstrapAdminInput): UseCaseIO[(User, AccountToken)]
  def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)]
  def me(actor: ActorContext): UseCaseIO[User]
  def updateMyProfile(request: IdempotencyRequest, actor: ActorContext, input: AccountProfileInput): UseCaseIO[User]
  def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[Unit]
  def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]]
}
