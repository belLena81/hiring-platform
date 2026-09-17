package com.example.graphQL.cats.api.http

import cats.effect.IO
import com.example.graphQL.cats.application.ActorContext
import com.example.graphQL.cats.application.port.UserRepository
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import io.circe.Json
import io.circe.parser.parse
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.http4s.Request
import org.typelevel.ci.CIString
import scala.util.Try

final class JwtActorAuthenticator(config: JwtAuthConfig, users: UserRepository[IO], now: IO[Instant]) {
  def authenticate(request: Request[IO]): IO[Option[ActorContext]] =
    config.hmacSecret match {
      case None => IO.pure(None)
      case Some(secret) =>
        bearerToken(request).fold(IO.pure(None)) { token =>
          JwtActorAuthenticator.verify(token, secret, config.issuer, config.audience, now).flatMap {
            case None => IO.pure(None)
            case Some(userId) => users.find(userId).map(_.map(user => ActorContext(user.id, user.role)))
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
  private val Algorithm = "HS256"
  private val HmacSha256 = "HmacSHA256"
  private val Utf8 = StandardCharsets.UTF_8
  private val Base64Url = Base64.getUrlDecoder
  private val Base64UrlNoPadding = Base64.getUrlEncoder.withoutPadding()

  def verify(token: String, secret: String, issuer: String, audience: String, now: IO[Instant]): IO[Option[UserId]] =
    now.map { instant =>
      verifyAt(token, secret, issuer, audience, instant)
    }

  private[http] def sign(header: Json, payload: Json, secret: String): String = {
    val encodedHeader = encodeUtf8(header.noSpaces)
    val encodedPayload = encodeUtf8(payload.noSpaces)
    val signingInput = s"$encodedHeader.$encodedPayload"
    s"$signingInput.${signature(signingInput, secret)}"
  }

  private[http] def verifyAt(token: String, secret: String, issuer: String, audience: String, now: Instant): Option[UserId] =
    token.split("\\.", -1).toList match {
      case encodedHeader :: encodedPayload :: encodedSignature :: Nil =>
        for {
          header <- decodeJson(encodedHeader)
          payload <- decodeJson(encodedPayload)
          algorithm <- header.hcursor.get[String]("alg").toOption
          if algorithm == Algorithm
          expected = signature(s"$encodedHeader.$encodedPayload", secret)
          if MessageDigest.isEqual(expected.getBytes(Utf8), encodedSignature.getBytes(Utf8))
          subject <- payload.hcursor.get[String]("sub").toOption
          userId <- Try(java.util.UUID.fromString(subject)).toOption.map(UserId(_))
          tokenIssuer <- payload.hcursor.get[String]("iss").toOption
          if tokenIssuer == issuer
          if audienceMatches(payload, audience)
          expiresAt <- payload.hcursor.get[Long]("exp").toOption
          if expiresAt > now.getEpochSecond
          notBefore <- optionalLong(payload, "nbf")
          if notBefore.forall(_ <= now.getEpochSecond)
        } yield userId
      case _ => None
    }

  private def optionalLong(payload: Json, field: String): Option[Option[Long]] =
    payload.hcursor.downField(field).focus match {
      case None => Some(None)
      case Some(_) => payload.hcursor.get[Long](field).toOption.map(Some(_))
    }

  private def audienceMatches(payload: Json, expected: String): Boolean =
    payload.hcursor.get[String]("aud").toOption.contains(expected) ||
      payload.hcursor.get[List[String]]("aud").toOption.exists(_.contains(expected))

  private def decodeJson(value: String): Option[Json] =
    Try(new String(Base64Url.decode(value), Utf8)).toOption.flatMap(raw => parse(raw).toOption)

  private def encodeUtf8(value: String): String =
    Base64UrlNoPadding.encodeToString(value.getBytes(Utf8))

  private def signature(signingInput: String, secret: String): String = {
    val mac = Mac.getInstance(HmacSha256)
    mac.init(new SecretKeySpec(secret.getBytes(Utf8), HmacSha256))
    Base64UrlNoPadding.encodeToString(mac.doFinal(signingInput.getBytes(Utf8)))
  }
}
