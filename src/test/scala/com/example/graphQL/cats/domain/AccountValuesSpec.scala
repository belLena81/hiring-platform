package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.{EmailAddress, PasswordHash}
import munit.FunSuite
import scala.compiletime.testing.typeCheckErrors

final class AccountValuesSpec extends FunSuite {
  test("email addresses preserve current trimmed nonblank validation") {
    assertEquals(EmailAddress.from("  person@example.com  ").map(_.value), Right("person@example.com"))
    assert(EmailAddress.from(" ").isLeft)
    assert(EmailAddress.from("x" * 257).isLeft)
  }

  test("password hashes preserve the encoded value across their typed boundary") {
    val passwordHash = PasswordHash.fromEncoded("encoded-hash")

    assertEquals(passwordHash.encoded, "encoded-hash")
  }

  test("email addresses and password hashes cannot be confused with strings or each other") {
    val stringToEmail = typeCheckErrors("""
      import com.example.graphQL.cats.domain.model.EmailAddress
      val email: EmailAddress = "person@example.com"
    """)
    val stringToHash = typeCheckErrors("""
      import com.example.graphQL.cats.domain.model.PasswordHash
      val passwordHash: PasswordHash = "encoded-hash"
    """)
    val hashToEmail = typeCheckErrors("""
      import com.example.graphQL.cats.domain.model.{EmailAddress, PasswordHash}
      val passwordHash = PasswordHash.fromEncoded("encoded-hash")
      val email: EmailAddress = passwordHash
    """)

    assert(stringToEmail.nonEmpty)
    assert(stringToHash.nonEmpty)
    assert(hashToEmail.nonEmpty)
  }
}
