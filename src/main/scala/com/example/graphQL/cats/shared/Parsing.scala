package com.example.graphQL.cats.shared

import cats.syntax.all.*
import java.util.UUID

object Parsing {
  def parseUuid(value: String): Either[Throwable, UUID] =
    Either.catchNonFatal(UUID.fromString(value))
}
