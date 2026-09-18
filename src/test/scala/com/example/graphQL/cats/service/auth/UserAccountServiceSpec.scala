package com.example.graphQL.cats.service.auth

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.JwtAuthConfig
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{UserAccountRepository, UserRepository}
import com.example.graphQL.cats.service.{AccountError, ActorContext, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.protocol.*
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class UserAccountServiceSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-18T10:00:00Z")
  private val userId = UserId(UUID.fromString("20000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("20000000-0000-0000-0000-000000000002"))
  private val recruiter = User(recruiterId, None, "Recruiter", UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
  private val deletedRecruiter = recruiter.copy(accountStatus = AccountStatus.Deleted, profile = None, deletedAt = Some(now))

  test("ordinary signup cannot create an Admin") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      result <- service.signUp(SignUpInput("Admin", UserRole.Admin, "password-password", None), now, userId)
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.AdminSignupForbidden)))
  }

  test("signup rejects a profile belonging to another role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      result <- service.signUp(SignUpInput(
        "Candidate",
        UserRole.Candidate,
        "password-password",
        Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))
      ), now, userId)
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
  }

  test("candidate and recruiter signup require their matching profile") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      candidate <- service.signUp(SignUpInput("Candidate", UserRole.Candidate, "password-password", None), now, userId)
      recruiter <- service.signUp(SignUpInput("Recruiter", UserRole.Recruiter, "password-password", None), now, recruiterId)
    } yield {
      assertEquals(candidate, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
      assertEquals(recruiter, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
    }
  }

  test("signup does not persist when JWT issuance is disabled") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, JwtAuthConfig(None, "issuer", "audience"))
      result <- service.signUp(
        SignUpInput("Candidate", UserRole.Candidate, "password-password",
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))),
        now,
        userId
      )
      stored <- accounts.values.get
    } yield {
      assertEquals(result, Left(UseCaseError.repository(RepositoryError.Unavailable)))
      assertEquals(stored, Map.empty)
    }
  }

  test("profile update rejects non-applicable role fields before persistence") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map(recruiter.id -> recruiter)), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      result <- service.updateMyProfile(
        ActorContext(recruiter.id, UserRole.Recruiter),
        AccountProfileInput(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
      )
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
  }

  test("valid recruiter signup remains supported") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      result <- service.signUp(SignUpInput(
        "Recruiter",
        UserRole.Recruiter,
        "password-password",
        Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))
      ), now, userId)
    } yield assert(result.isRight)
  }

  test("deleting an already deleted account is idempotent") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map(deletedRecruiter.id -> deletedRecruiter)), accounts, TestHasher, JwtAuthConfig(Some("secret"), "issuer", "audience"))
      result <- service.deleteMyAccount(ActorContext(deletedRecruiter.id, UserRole.Recruiter), now)
    } yield assertEquals(result, Right(()))
  }

  private object TestHasher extends PasswordHasher[IO] {
    override def hash(password: String): IO[String] = IO.pure(s"hash:$password")
    override def verify(encoded: String, password: String): IO[Boolean] = IO.pure(encoded == s"hash:$password")
  }

  private final class TestUsers(values: Map[UserId, User]) extends UserRepository[IO] {
    val ref: IO[Map[UserId, User]] = IO.pure(values)
    override def find(id: UserId): IO[Option[User]] = IO.pure(values.get(id))
    override def findMany(ids: List[UserId]): IO[List[User]] = IO.pure(ids.flatMap(values.get))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }

  private final class TestAccounts(
      initializedState: Boolean,
      val values: Ref[IO, Map[String, AccountCredentials]]
  ) extends UserAccountRepository[IO] {
    override def bootstrap(user: User, passwordHash: String): IO[Either[RepositoryError, Unit]] = IO.pure(Left(RepositoryError.Conflict))
    override def initialized: IO[Boolean] = IO.pure(initializedState)
    override def createAccount(user: User, passwordHash: String): IO[Either[RepositoryError, Unit]] =
      values.modify { current =>
        val key = AccountName.canonical(user.name)
        if (current.contains(key)) current -> Left(RepositoryError.Conflict)
        else (current.updated(key, AccountCredentials(user, passwordHash)), Right(()))
      }
    override def findByCanonicalName(nameCanonical: String): IO[Option[AccountCredentials]] = values.get.map(_.get(nameCanonical))
    override def updateProfile(userId: UserId, profile: UserProfile): IO[Either[RepositoryError, User]] = IO.pure(Left(RepositoryError.Unavailable))
    override def listAccounts(page: UserPageRequest): IO[List[User]] = IO.pure(Nil)
    override def deleteAccount(userId: UserId, now: Instant, tombstone: String): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }

  private object TestAccounts {
    def create(initialized: Boolean): IO[TestAccounts] = Ref.of[IO, Map[String, AccountCredentials]](Map.empty).map(new TestAccounts(initialized, _))
  }
}
