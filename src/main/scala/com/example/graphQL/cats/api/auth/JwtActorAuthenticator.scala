package com.example.graphQL.cats.api.auth

import cats.effect.IO
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import java.time.{Clock, Instant, ZoneOffset}
import org.http4s.Request
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization
import org.typelevel.ci.CIString
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtOptions}
import io.circe.Json
import scala.util.Try

enum AuthFailure {
  case MalformedCredentials, InvalidToken, UnknownActor
}

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserAuthenticator[IO], now: IO[Instant]) {
  def authenticate(request: Request[IO]): IO[Option[ActorContext]] =
    authenticateDetailed(request).map(_.toOption.flatten)

  def authenticateDetailed(request: Request[IO]): IO[Either[AuthFailure, Option[ActorContext]]] =
    bearerToken(request) match {
      case Right(None) => IO.pure(Right(None))
      case Left(failure) => IO.pure(Left(failure))
      case Right(Some(token)) => config.hmacSecret match {
        case None => IO.pure(Right(None))
        case Some(secret) =>
          JwtActorAuthenticator.verify(token, secret, config.issuer, config.audience, now).flatMap {
            case None => IO.pure(Left(AuthFailure.InvalidToken))
            case Some(userId) => users.actorFor(userId).map {
              case Some(actor) => Right(Some(actor))
              case None => Left(AuthFailure.UnknownActor)
            }
          }
      }
    }

  private def bearerToken(request: Request[IO]): Either[AuthFailure, Option[String]] =
    request.headers.get(CIString("Authorization")).map(_.toList) match {
      case Some(values) if values.size != 1 => Left(AuthFailure.MalformedCredentials)
      case _ => request.headers.get[Authorization] match {
      case None => Right(None)
      case Some(Authorization(Credentials.Token(AuthScheme.Bearer, token))) if token.nonEmpty => Right(Some(token))
      case Some(_) => Left(AuthFailure.MalformedCredentials)
      }
    }
}

object JwtActorAuthenticator {
  private val Algorithms = Seq(JwtAlgorithm.HS256)
  private val Options = JwtOptions(signature = true, expiration = true, notBefore = true, leeway = 0)

  def apply(config: JwtAuthConfig, users: UserAuthenticator[IO], now: IO[Instant]): JwtActorAuthenticator =
    new JwtActorAuthenticator(config, users, now)

  def issue(config: JwtAuthConfig, userId: UserId, now: Instant): Option[(String, Instant)] =
    config.hmacSecret.map { secret =>
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
        secret
      )
      token -> expiresAt
    }

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
