package com.example.graphQL.cats.domain.model

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.shared.pagination.PageSize
import java.time.Instant
import java.text.Normalizer
import java.util.Locale

object AccountName {
  def canonical(value: String): String =
    Normalizer.normalize(value.trim, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT)
}

final case class AccountCredentials(user: User, passwordHash: String)

final case class UserCursor(createdAt: Instant, id: UserId)

final case class UserPageRequest(status: AccountStatus, role: Option[UserRole], cursor: Option[UserCursor], pageSize: PageSize)

final case class AccountToken(value: String, expiresAt: Instant)
