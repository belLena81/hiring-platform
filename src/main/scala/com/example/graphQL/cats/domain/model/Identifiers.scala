package com.example.graphQL.cats.domain.model

import java.util.UUID

object Identifiers {
  sealed trait UserTag
  sealed trait JobTag
  sealed trait ApplicationTag
  sealed trait ApplicationEventTag

  opaque type Id[Tag] = UUID

  object Id {
    def apply[Tag](value: UUID): Id[Tag] = value
  }

  type UserId = Id[UserTag]
  object UserId {
    def apply(value: UUID): UserId = Id[UserTag](value)
  }

  type JobId = Id[JobTag]
  object JobId {
    def apply(value: UUID): JobId = Id[JobTag](value)
  }

  type ApplicationId = Id[ApplicationTag]
  object ApplicationId {
    def apply(value: UUID): ApplicationId = Id[ApplicationTag](value)
  }

  type ApplicationEventId = Id[ApplicationEventTag]
  object ApplicationEventId {
    def apply(value: UUID): ApplicationEventId = Id[ApplicationEventTag](value)
  }

  extension [Tag](id: Id[Tag]) def value: UUID = id
}
