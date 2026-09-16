package com.example.graphQL.cats.domain.model

import java.util.UUID
import scala.annotation.targetName

object Identifiers {
  opaque type UserId = UUID
  object UserId {
    def apply(value: UUID): UserId = value
  }

  opaque type JobId = UUID
  object JobId {
    def apply(value: UUID): JobId = value
  }

  opaque type ApplicationId = UUID
  object ApplicationId {
    def apply(value: UUID): ApplicationId = value
  }

  opaque type ApplicationEventId = UUID
  object ApplicationEventId {
    def apply(value: UUID): ApplicationEventId = value
  }

  extension (id: UserId) @targetName("userIdValue") def value: UUID = id
  extension (id: JobId) @targetName("jobIdValue") def value: UUID = id
  extension (id: ApplicationId) @targetName("applicationIdValue") def value: UUID = id
  extension (id: ApplicationEventId) @targetName("applicationEventIdValue") def value: UUID = id
}
