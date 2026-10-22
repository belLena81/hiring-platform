package com.example.graphQL.cats.service

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.service.port.ActorRecord

sealed trait ActorContext {
  def userId: UserId
  def role: UserRole
}

object ActorContext {
  private final case class Claims(userId: UserId, role: UserRole) extends ActorContext

  def apply(userId: UserId, role: UserRole): ActorContext = Claims(userId, role)
}

final class AuthenticatedActor private[service] (val claims: ActorContext, val viewer: ActorRecord)
    extends ActorContext {
  override val userId: UserId = claims.userId
  override val role: UserRole = claims.role
}

object AuthenticatedActor {
  private[service] def apply(claims: ActorContext, viewer: ActorRecord): AuthenticatedActor =
    new AuthenticatedActor(claims, viewer)
}
