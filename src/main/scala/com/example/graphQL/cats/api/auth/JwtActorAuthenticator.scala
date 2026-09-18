package com.example.graphQL.cats.api.auth

import cats.effect.IO
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import java.time.{Clock, Instant, ZoneOffset}
import org.http4s.Request
import org.typelevel.ci.CIString
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtOptions}
import scala.util.Try

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserAuthenticator[IO], now: IO[Instant]) {
  def authenticate(request: Request[IO]): IO[Option[ActorContext]] =
    config.hmacSecret match {
      case None => IO.pure(None)
      case Some(secret) =>
        bearerToken(request).fold(IO.pure(None)) { token =>
          JwtActorAuthenticator.verify(token, secret, config.issuer, config.audience, now).flatMap {
            case None => IO.pure(None)
            case Some(userId) => users.actorFor(userId)
          }
        }
    }

  private def bearerToken(request: Request[IO]): Option[String] =
    request.headers.get(CIString("Authorization")).flatMap { headers =>
      headers.toList match {
        case header :: Nil =>
          val value = header.value.trim
          Option.when(value.regionMatches(true, 0, "Bearer ", 0, 7))(value.drop(7).trim).filter(_.nonEmpty)
        case _ => None
      }
    }
}

object JwtActorAuthenticator {
  private val Algorithms = Seq(JwtAlgorithm.HS256)
  private val Options = JwtOptions(signature = true, expiration = true, notBefore = true, leeway = 0)

  def apply(config: JwtAuthConfig, users: UserAuthenticator[IO], now: IO[Instant]): JwtActorAuthenticator =
    new JwtActorAuthenticator(config, users, now)

  def verify(token: String, secret: String, issuer: String, audience: String, now: IO[Instant]): IO[Option[UserId]] =
    now.map { instant =>
      verifyAt(token, secret, issuer, audience, instant)
    }

  private[auth] def verifyAt(token: String, secret: String, issuer: String, audience: String, now: Instant): Option[UserId] =
    given clock: Clock = Clock.fixed(now, ZoneOffset.UTC)

    JwtCirce(clock)
      .decode(token, secret, Algorithms, Options)
      .toOption
      .filter(_.isValid(issuer, audience))
      .flatMap(_.subject)
      .flatMap(subject => Try(java.util.UUID.fromString(subject)).toOption.map(UserId(_)))
}
