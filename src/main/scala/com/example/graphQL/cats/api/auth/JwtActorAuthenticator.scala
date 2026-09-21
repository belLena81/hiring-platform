package com.example.graphQL.cats.api.auth

import cats.effect.{Clock as EffectClock, IO}
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.infrastructure.auth.JwtAccessTokenIssuer
import com.example.graphQL.cats.shared.Parsing.parseUuid
import java.time.{Clock as JavaClock, Instant, ZoneOffset}
import org.http4s.Request
import org.http4s.{AuthScheme, Credentials}
import org.http4s.headers.Authorization
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtOptions}

enum AuthFailure {
  case MalformedCredentials, InvalidToken, UnknownActor, Unavailable
}

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserAuthenticator[IO], clock: EffectClock[IO]) {
  def authenticate(request: Request[IO]): IO[Either[AuthFailure, Option[ActorContext]]] =
    authenticateDetailed(request)

  def authenticateDetailed(request: Request[IO]): IO[Either[AuthFailure, Option[ActorContext]]] =
    (for {
      token <- EitherT.fromEither[IO](bearerToken(request))
      actor <- token.traverse { value =>
        for {
          userId <- EitherT.fromOptionF(
            JwtActorAuthenticator.verify(value, config.hmacSecret, config.issuer, config.audience, clock),
            AuthFailure.InvalidToken)
          actor <- EitherT(users.actorFor(userId).map(_.leftMap(_ => AuthFailure.Unavailable)
            .flatMap(_.toRight(AuthFailure.UnknownActor))))
        } yield actor
      }
    } yield actor).value

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
    val accountToken = JwtAccessTokenIssuer.issue(config, userId, now)
    accountToken.value -> accountToken.expiresAt
  }

  def verify(token: String, secret: String, issuer: String, audience: String, clock: EffectClock[IO]): IO[Option[UserId]] =
    clock.realTimeInstant.map(instant => verifyAt(token, secret, issuer, audience, instant))

  private[auth] def verifyAt(token: String, secret: String, issuer: String, audience: String, now: Instant): Option[UserId] =
    given clock: JavaClock = JavaClock.fixed(now, ZoneOffset.UTC)

    JwtCirce(clock)
      .decode(token, secret, Algorithms, Options)
      .toOption
      .filter(_.isValid(issuer, audience))
      .flatMap(_.subject)
      .flatMap(subject => parseUuid(subject).toOption.map(UserId(_)))
}
