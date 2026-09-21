package com.example.graphQL.cats.infrastructure.auth

import cats.effect.IO
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.{AccountToken, User}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.auth.{AccessTokenIssuanceError, AccessTokenIssuer}
import java.time.Instant
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}

final class JwtAccessTokenIssuer(config: JwtAuthConfig) extends AccessTokenIssuer[IO] {
  override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
    IO.delay(JwtAccessTokenIssuer.issue(config, user.id, now)).attempt.map(_.left.map(_ => AccessTokenIssuanceError.Unavailable))
}

object JwtAccessTokenIssuer {
  def issue(config: JwtAuthConfig, userId: UserId, now: Instant): AccountToken = {
    val expiresAt = now.plusSeconds(config.accessTokenSeconds)
    val claim = JwtClaim()
      .about(userId.value.toString)
      .by(config.issuer)
      .to(config.audience)
      .issuedAt(now.getEpochSecond)
      .expiresAt(expiresAt.getEpochSecond)
    val token = JwtCirce.encode(claim, config.hmacSecret, JwtAlgorithm.HS256)
    AccountToken(token, expiresAt)
  }
}
