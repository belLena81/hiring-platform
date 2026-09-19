package com.example.graphQL.cats.infrastructure.auth

import cats.effect.IO
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.{AccountToken, User}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.auth.{AccessTokenIssuanceError, AccessTokenIssuer}
import io.circe.Json
import java.time.Instant
import pdi.jwt.JwtCirce

final class JwtAccessTokenIssuer(config: JwtAuthConfig) extends AccessTokenIssuer[IO] {
  override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
    IO.delay(JwtAccessTokenIssuer.issue(config, user.id, now)).attempt.map(_.left.map(_ => AccessTokenIssuanceError.Unavailable))
}

object JwtAccessTokenIssuer {
  def issue(config: JwtAuthConfig, userId: UserId, now: Instant): AccountToken = {
    val expiresAt = now.plusSeconds(config.accessTokenSeconds)
    val token = JwtCirce.encode(
      Json.obj("alg" -> Json.fromString("HS256")),
      Json.obj(
        "sub" -> Json.fromString(userId.value.toString),
        "iss" -> Json.fromString(config.issuer),
        "aud" -> Json.fromString(config.audience),
        "iat" -> Json.fromLong(now.getEpochSecond),
        "exp" -> Json.fromLong(expiresAt.getEpochSecond)
      ),
      config.hmacSecret
    )
    AccountToken(token, expiresAt)
  }
}
