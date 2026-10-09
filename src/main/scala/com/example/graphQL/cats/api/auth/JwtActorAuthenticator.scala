package com.example.graphQL.cats.api.auth

import cats.effect.{Clock as EffectClock, IO}
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.{UserId, parse as parseIdentifier}
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.shared.crypto.{Hmac, HmacJwt}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import org.http4s.Request
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization

enum AuthFailure {
  case MalformedCredentials, InvalidToken, UnknownActor, Unavailable
}

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserAuthenticator, clock: EffectClock[IO]) {
  def authenticate(request: Request[IO]): IO[Either[AuthFailure, Option[ActorContext]]] =
    (for {
      token <- EitherT.fromEither[IO](bearerToken(request))
      actor <- token.traverse { value =>
        for {
          userId <- EitherT.fromOptionF(
            JwtActorAuthenticator.verify(value, config.hmacSecret, config.issuer, config.audience, clock),
            AuthFailure.InvalidToken
          )
          actor <- users
            .actorForVerifiedToken(userId)
            .leftMap(_ => AuthFailure.Unavailable)
            .subflatMap(_.toRight(AuthFailure.UnknownActor))
        } yield actor
      }
    } yield actor).value

  private def bearerToken(request: Request[IO]): Either[AuthFailure, Option[String]] =
    request.headers.get(Authorization.headerInstance.name).map(_.toList) match {
      case None | Some(Nil)  => Right(None)
      case Some(_ :: _ :: _) => Left(AuthFailure.MalformedCredentials)
      case Some(_)           =>
        request.headers.get[Authorization] match {
          case Some(Authorization(Credentials.Token(AuthScheme.Bearer, token))) if token.nonEmpty => Right(Some(token))
          case _ => Left(AuthFailure.MalformedCredentials)
        }
    }
}

object JwtActorAuthenticator {
  def verify(
      token: String,
      secret: String,
      issuer: String,
      audience: String,
      clock: EffectClock[IO]
  ): IO[Option[UserId]] =
    clock.realTimeInstant.map(instant => verifyAt(token, secret, issuer, audience, instant))

  private[auth] def verifyAt(
      token: String,
      secret: String,
      issuer: String,
      audience: String,
      now: Instant
  ): Option[UserId] =
    HmacJwt
      .decode(token, Hmac.secretKey(secret.getBytes(UTF_8)), now, issuer, audience)
      .flatMap(_.subject)
      .flatMap(subject => parseIdentifier(subject)(UserId.apply))
}
