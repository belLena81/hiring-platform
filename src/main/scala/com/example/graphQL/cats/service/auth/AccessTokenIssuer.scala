package com.example.graphQL.cats.service.auth

import com.example.graphQL.cats.domain.model.{AccountToken, User}
import java.time.Instant

enum AccessTokenIssuanceError {
  case Unavailable
}

/** Infrastructure-owned token creation boundary used by account workflows. */
trait AccessTokenIssuer[F[_]] {
  def issue(user: User, now: Instant): F[Either[AccessTokenIssuanceError, AccountToken]]
}
