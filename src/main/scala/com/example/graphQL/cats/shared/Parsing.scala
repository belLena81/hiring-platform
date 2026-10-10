package com.example.graphQL.cats.shared

import cats.syntax.all.*
import java.util.UUID

object Parsing {
  def parseUuid(value: String): Either[Throwable, UUID] =
    Either.catchNonFatal(UUID.fromString(value))
}

/** Deterministic identifiers, so a retried operation derives the same id for the same seed. */
object StableIds {
  def uuid(seed: String): UUID = UUID.nameUUIDFromBytes(seed.getBytes(java.nio.charset.StandardCharsets.UTF_8))
}
