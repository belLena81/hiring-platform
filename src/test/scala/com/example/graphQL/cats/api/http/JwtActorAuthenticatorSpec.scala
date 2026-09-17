package com.example.graphQL.cats.api.http

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.application.ActorContext
import com.example.graphQL.cats.application.port.{RepositoryError, UserRepository}
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{CandidateProfile, EntityEmbedding, User, UserRole}
import io.circe.Json
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import org.http4s.{Header, Method, Request, Uri}
import org.typelevel.ci.CIString

final class JwtActorAuthenticatorSpec extends CatsEffectSuite {
  private val secret = "01234567890123456789012345678901"
  private val issuer = "hiring-platform-local"
  private val audience = "hiring-graphql-api"
  private val now = Instant.parse("2026-09-17T12:00:00Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val candidate = User(candidateId, "candidate@example.com", "Candidate", UserRole.Candidate,
    Some(CandidateProfile(Set("scala"), None, None)), now)
  private val recruiter = candidate.copy(role = UserRole.Recruiter)

  test("valid signed bearer token authenticates the user role stored in Mongo-backed users") {
    val token = signedToken(candidateId, Json.obj("role" -> Json.fromString("Admin")))
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(Some(secret), issuer, audience),
      users(Map(candidateId -> recruiter)),
      IO.pure(now)
    )
    authenticator.authenticate(request(Some(token))).map { actor =>
      assertEquals(actor, Some(ActorContext(candidateId, UserRole.Recruiter)))
    }
  }

  test("invalid or missing bearer tokens do not authenticate") {
    val valid = signedToken(candidateId)
    val invalidTokens = List(
      None,
      Some(valid + "tampered"),
      Some("one.two"),
      Some("one.two.three.four"),
      Some(s"not-base64.${valid.split("\\.", -1)(1)}.${valid.split("\\.", -1)(2)}"),
      Some(signedToken(candidateId, Json.obj(), Json.obj("alg" -> Json.fromString("none")))),
      Some(signedToken(candidateId, Json.obj("aud" -> Json.fromString("other-api")))),
      Some(signedToken(candidateId, Json.obj("iss" -> Json.fromString("other-issuer")))),
      Some(signedToken(candidateId, Json.obj("exp" -> Json.fromLong(now.minusSeconds(1).getEpochSecond)))),
      Some(signedToken(candidateId, Json.obj("nbf" -> Json.fromLong(now.plusSeconds(60).getEpochSecond)))),
      Some(signedToken(candidateId, Json.obj("sub" -> Json.fromString("not-a-uuid"))))
    )
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(Some(secret), issuer, audience),
      users(Map(candidateId -> candidate)),
      IO.pure(now)
    )
    invalidTokens.traverse(token => authenticator.authenticate(request(token)).map(assertEquals(_, None)))
  }

  test("multiple Authorization headers do not authenticate") {
    val token = signedToken(candidateId)
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(Some(secret), issuer, audience),
      users(Map(candidateId -> candidate)),
      IO.pure(now)
    )
    val duplicated = Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).putHeaders(
      Header.Raw(CIString("Authorization"), s"Bearer $token"),
      Header.Raw(CIString("Authorization"), s"Bearer $token")
    )
    authenticator.authenticate(duplicated).map(assertEquals(_, None))
  }

  test("disabled auth and unknown users remain unauthenticated") {
    val token = signedToken(candidateId)
    val enabled = new JwtActorAuthenticator(JwtAuthConfig(Some(secret), issuer, audience), users(Map.empty), IO.pure(now))
    val disabled = new JwtActorAuthenticator(JwtAuthConfig(None, issuer, audience), users(Map(candidateId -> candidate)), IO.pure(now))
    for {
      missing <- enabled.authenticate(request(Some(token)))
      inactive <- disabled.authenticate(request(Some(token)))
    } yield {
      assertEquals(missing, None)
      assertEquals(inactive, None)
    }
  }

  private def request(token: Option[String]): Request[IO] =
    token.fold(Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))) { value =>
      Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
        .putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $value"))
    }

  private def signedToken(
      userId: UserId,
      overrideClaims: Json = Json.obj(),
      header: Json = Json.obj("alg" -> Json.fromString("HS256"))
  ): String = {
    val baseClaims = Json.obj(
      "sub" -> Json.fromString(userId.value.toString),
      "iss" -> Json.fromString(issuer),
      "aud" -> Json.fromString(audience),
      "exp" -> Json.fromLong(now.plusSeconds(300).getEpochSecond)
    )
    val payload = overrideClaims.asObject.fold(baseClaims)(fields => baseClaims.deepMerge(Json.fromJsonObject(fields)))
    JwtActorAuthenticator.sign(header, payload, secret)
  }

  private def users(values: Map[UserId, User]): UserRepository[IO] = new UserRepository[IO] {
    override def find(id: UserId): IO[Option[User]] = IO.pure(values.get(id))
    override def findMany(ids: List[UserId]): IO[List[User]] = IO.pure(ids.flatMap(values.get))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
  }
}
