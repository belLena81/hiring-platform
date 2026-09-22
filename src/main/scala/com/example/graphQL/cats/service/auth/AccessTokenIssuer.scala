package com.example.graphQL.cats.service.auth

import cats.effect.IO
import com.example.graphQL.cats.domain.model.{AccountToken, User}
import java.time.Instant

enum AccessTokenIssuanceError {
  case Unavailable
}

/** Infrastructure-owned token creation boundary used by account workflows. */
trait AccessTokenIssuer {
  def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]]
}
