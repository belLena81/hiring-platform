package com.example.graphQL.cats.api.auth

import cats.effect.{Clock as EffectClock, IO}
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import java.time.{Clock as JavaClock, Instant, ZoneOffset}
import org.http4s.Request
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtOptions}
import io.circe.Json
import scala.util.Try

enum AuthFailure {
  case MalformedCredentials, InvalidToken, UnknownActor
}

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserAuthenticator[IO], clock: EffectClock[IO]) {
  def authenticateDetailed(request: Request[IO]): IO[Either[AuthFailure, Option[ActorContext]]] =
    bearerToken(request) match {
      case Right(None) => IO.pure(Right(None))
      case Left(failure) => IO.pure(Left(failure))
      case Right(Some(token)) =>
        JwtActorAuthenticator.verify(token, config.hmacSecret, config.issuer, config.audience, clock).flatMap {
          case None => IO.pure(Left(AuthFailure.InvalidToken))
          case Some(userId) => users.actorFor(userId).map {
            case Some(actor) => Right(Some(actor))
            case None => Left(AuthFailure.UnknownActor)
          }
        }
    }

  private def bearerToken(request: Request[IO]): Either[AuthFailure, Option[String]] =
    request.headers.get(Authorization.headerInstance.name).map(_.toList) match {
      case None | Some(Nil) => Right(None)
      case Some(_ :: _ :: _) => Left(AuthFailure.MalformedCredentials)
      case Some(_) =>
        request.headers.get[Authorization] match {
          case Some(Authorization(Credentials.Token(AuthScheme.Bearer, token))) if token.nonEmpty => Right(Some(token))
          case _ => Left(AuthFailure.MalformedCredentials)
        }
    }
}

object JwtActorAuthenticator {
  private val Algorithms = Seq(JwtAlgorithm.HS256)
  private val Options = JwtOptions(signature = true, expiration = true, notBefore = true, leeway = 0)

  def apply(config: JwtAuthConfig, users: UserAuthenticator[IO], clock: EffectClock[IO]): JwtActorAuthenticator =
    new JwtActorAuthenticator(config, users, clock)

  def issue(config: JwtAuthConfig, userId: UserId, now: Instant): (String, Instant) = {
    val expiresAt = now.plusSeconds(config.accessTokenSeconds)
    val token = JwtCirce.encode(
      Json.obj("alg" -> Json.fromString(Algorithm)),
      Json.obj(
        "sub" -> Json.fromString(userId.value.toString),
        "iss" -> Json.fromString(config.issuer),
        "aud" -> Json.fromString(config.audience),
        "iat" -> Json.fromLong(now.getEpochSecond),
        "exp" -> Json.fromLong(expiresAt.getEpochSecond)
      ),
      config.hmacSecret
    )
    token -> expiresAt
  }

  def verify(token: String, secret: String, issuer: String, audience: String, clock: EffectClock[IO]): IO[Option[UserId]] =
    clock.realTimeInstant.map(instant => verifyAt(token, secret, issuer, audience, instant))

  private val Algorithm = "HS256"

  private[auth] def verifyAt(token: String, secret: String, issuer: String, audience: String, now: Instant): Option[UserId] =
    given clock: JavaClock = JavaClock.fixed(now, ZoneOffset.UTC)

    JwtCirce(clock)
      .decode(token, secret, Algorithms, Options)
      .toOption
      .filter(_.isValid(issuer, audience))
      .flatMap(_.subject)
      .flatMap(subject => Try(java.util.UUID.fromString(subject)).toOption.map(UserId(_)))
}
