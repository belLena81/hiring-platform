package com.example.graphQL.cats.domain.model

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.pagination.{PageSize, TimestampIdCursor}
import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.text.Normalizer
import java.util.Locale

object AccountName {
  def normalized(value: String): String = Normalizer.normalize(value.trim, Normalizer.Form.NFKC)

  def canonical(value: String): String = normalized(value).toLowerCase(Locale.ROOT)
}

/** The name and password a person registers with. */
object Credentials {
  val PasswordMinBytes = 12

  /** Accumulates every violation; the valid result is the normalized account name. */
  def validate(name: String, password: String): ValidatedNel[DomainValidationError, String] =
    (validName(name), validPassword(password)).mapN((validName, _) => validName)

  private def validName(name: String): ValidatedNel[DomainValidationError, String] = {
    val normalized = AccountName.normalized(name)
    if (normalized.isEmpty) DomainValidationError.BlankField("name").invalidNel
    else if (normalized.length > FieldLimits.ShortTextMaxChars)
      DomainValidationError.TextTooLong("name", FieldLimits.ShortTextMaxChars, normalized.length).invalidNel
    else normalized.validNel
  }

  private def validPassword(password: String): ValidatedNel[DomainValidationError, Unit] = {
    val bytes = password.getBytes(StandardCharsets.UTF_8).length
    if (bytes < PasswordMinBytes) DomainValidationError.PasswordTooShort(PasswordMinBytes, bytes).invalidNel
    else if (bytes > FieldLimits.PasswordMaxBytes)
      DomainValidationError.ByteLengthExceeded("password", FieldLimits.PasswordMaxBytes, bytes).invalidNel
    else ().validNel
  }
}

final case class AccountCredentials(user: User, passwordHash: PasswordHash)

type UserCursor = TimestampIdCursor[UserId]
val UserCursor: TimestampIdCursor.type = TimestampIdCursor

final case class UserPageRequest(
    status: AccountStatus,
    role: Option[UserRole],
    cursor: Option[UserCursor],
    pageSize: PageSize
)

final case class AccountToken(value: String, expiresAt: Instant)
