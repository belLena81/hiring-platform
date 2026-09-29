package com.example.graphQL.cats

import com.example.graphQL.cats.domain.model.{EmailAddress, PasswordHash}

private[cats] object AccountValueFixtures {
  def email(value: String): EmailAddress =
    EmailAddress.from(value).fold(error => throw new AssertionError(s"Invalid email fixture: $error"), identity)

  def passwordHash(value: String): PasswordHash = PasswordHash.fromEncoded(value)
}
