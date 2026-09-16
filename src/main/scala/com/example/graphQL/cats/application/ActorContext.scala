package com.example.graphQL.cats.application

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole

final case class ActorContext(userId: UserId, role: UserRole)
