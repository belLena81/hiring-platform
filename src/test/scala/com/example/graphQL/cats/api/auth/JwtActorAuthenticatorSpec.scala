package com.example.graphQL.cats.api.auth

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.service.{ActorContext, RepositoryError}
import com.example.graphQL.cats.service.auth.UserAuthenticationService
import com.example.graphQL.cats.service.protocol.UserAuthenticator
import com.example.graphQL.cats.repository.protocol.UserRepository
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{CandidateProfile, EntityEmbedding, User, UserProfile, UserRole}
import io.circe.Json

import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import org.http4s.{Header, Method, Request, Uri}
import org.typelevel.ci.CIString
import pdi.jwt.JwtCirce

final class JwtActorAuthenticatorSpec extends CatsEffectSuite {
  private val secret = "01234567890123456789012345678901"
  private val issuer = "hiring-platform-local"
  private val audience = "hiring-graphql-api"
  private val now = Instant.parse("2026-09-17T12:00:00Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val candidate = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("scala"), None, None))), now)
  private val recruiter = candidate.copy(role = UserRole.Recruiter)

  test("valid signed bearer token authenticates the user role stored in Mongo-backed users") {
    val token = signedToken(candidateId, Json.obj("role" -> Json.fromString("Admin")))
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(secret, issuer, audience),
      users(Map(candidateId -> recruiter)),
      IO.pure(now)
    )
    authenticator.authenticateDetailed(request(Some(token))).map { actor =>
      assertEquals(actor, Right(Some(ActorContext(candidateId, UserRole.Recruiter))))
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
      Some(unsignedToken(candidateId)),
      Some(signedToken(candidateId, Json.obj("aud" -> Json.fromString("other-api")))),
      Some(signedToken(candidateId, Json.obj("iss" -> Json.fromString("other-issuer")))),
      Some(signedToken(candidateId, Json.obj("exp" -> Json.fromLong(now.minusSeconds(1).getEpochSecond)))),
      Some(signedToken(candidateId, Json.obj("nbf" -> Json.fromLong(now.plusSeconds(60).getEpochSecond)))),
      Some(signedToken(candidateId, Json.obj("sub" -> Json.fromString("not-a-uuid"))))
    )
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(secret, issuer, audience),
      users(Map(candidateId -> candidate)),
      IO.pure(now)
    )
    invalidTokens.traverse { token =>
      authenticator.authenticateDetailed(request(token)).map { result =>
        token match {
          case None => assertEquals(result, Right(None))
          case Some(_) => assert(result.isLeft, clues(result))
        }
      }
    }
  }

  test("multiple Authorization headers do not authenticate") {
    val token = signedToken(candidateId)
    val authenticator = new JwtActorAuthenticator(
      JwtAuthConfig(secret, issuer, audience),
      users(Map(candidateId -> candidate)),
      IO.pure(now)
    )
    val duplicated = Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).putHeaders(
      Header.Raw(CIString("Authorization"), s"Bearer $token"),
      Header.Raw(CIString("Authorization"), s"Bearer $token")
    )
    authenticator.authenticateDetailed(duplicated).map(assertEquals(_, Left(AuthFailure.MalformedCredentials)))
  }

  test("unknown users remain unauthenticated") {
    val token = signedToken(candidateId)
    val enabled = new JwtActorAuthenticator(JwtAuthConfig(secret, issuer, audience), users(Map.empty), IO.pure(now))
    enabled.authenticateDetailed(request(Some(token))).map(assertEquals(_, Left(AuthFailure.UnknownActor)))
  }

  private def request(token: Option[String]): Request[IO] =
    token.fold(Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))) { value =>
      Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
        .putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $value"))
    }

  private def signedToken(
      userId: UserId,
      overrideClaims: Json = Json.obj()
  ): String = {
    val payload = tokenPayload(userId, overrideClaims)
    JwtCirce.encode(Json.obj("alg" -> Json.fromString("HS256")), payload, secret)
  }

  private def unsignedToken(userId: UserId): String =
    JwtCirce.encode(tokenPayload(userId))

  private def tokenPayload(userId: UserId, overrideClaims: Json = Json.obj()): Json = {
    val baseClaims = Json.obj(
      "sub" -> Json.fromString(userId.value.toString),
      "iss" -> Json.fromString(issuer),
      "aud" -> Json.fromString(audience),
      "exp" -> Json.fromLong(now.plusSeconds(300).getEpochSecond)
    )
    overrideClaims.asObject.fold(baseClaims)(fields => baseClaims.deepMerge(Json.fromJsonObject(fields)))
  }

  private def users(values: Map[UserId, User]): UserAuthenticator[IO] =
    UserAuthenticationService[IO](userRepository(values))

  private def userRepository(values: Map[UserId, User]): UserRepository[IO] = new UserRepository[IO] {
    override def find(id: UserId): IO[Option[User]] = IO.pure(values.get(id))
    override def findMany(ids: List[UserId]): IO[List[User]] = IO.pure(ids.flatMap(values.get))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
  }
}
