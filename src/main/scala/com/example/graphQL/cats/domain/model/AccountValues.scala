package com.example.graphQL.cats.domain.model

import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError

opaque type EmailAddress = String

object EmailAddress {
  def from(value: String): Either[DomainValidationError, EmailAddress] =
    validateText("email", value).toEither.leftMap(_.head)

  extension (email: EmailAddress) def value: String = email
}

/** An encoded password hash produced by the configured password hashing implementation. */
opaque type PasswordHash = String

object PasswordHash {

  /** Lifts an encoded value at the password hasher or persistence boundary. */
  def fromEncoded(value: String): PasswordHash = value

  extension (passwordHash: PasswordHash) def encoded: String = passwordHash
}
