package com.example.graphQL.cats.service.auth

import cats.effect.{IO, Ref}
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
  private val admin = User(userId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
  private val deletedRecruiter = recruiter.copy(accountStatus = AccountStatus.Deleted, profile = None, deletedAt = Some(now))

  test("ordinary signup cannot create an Admin") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      result <- service.signUp(SignUpInput("Admin", UserRole.Admin, "password-password", None), now, userId)
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.AdminSignupForbidden)))
  }

  test("signup rejects a profile belonging to another role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
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
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      candidate <- service.signUp(SignUpInput("Candidate", UserRole.Candidate, "password-password", None), now, userId)
      recruiter <- service.signUp(SignUpInput("Recruiter", UserRole.Recruiter, "password-password", None), now, recruiterId)
    } yield {
      assertEquals(candidate, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
      assertEquals(recruiter, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
    }
  }

  test("profile role compatibility is defined once for account and persistence callers") {
    val candidate = Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
    val recruiter = Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))

    assert(UserProfile.matchesRole(UserRole.Candidate, candidate))
    assert(UserProfile.matchesRole(UserRole.Recruiter, recruiter))
    assert(UserProfile.matchesRole(UserRole.Admin, None))
    assert(!UserProfile.matchesRole(UserRole.Candidate, recruiter))
    assert(!UserProfile.matchesRole(UserRole.Admin, candidate))
  }

  test("signup persists account and returns an access token") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      result <- service.signUp(
        SignUpInput("Candidate", UserRole.Candidate, "password-password",
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))),
        now,
        userId
      )
      stored <- accounts.values.get
    } yield {
      assert(result.exists(_._2.value.nonEmpty))
      assertEquals(stored.size, 1)
    }
  }

  test("signup reports a blank name once while retaining other registration validation") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      result <- service.signUp(
        SignUpInput("   ", UserRole.Candidate, "password-password", Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))),
        now,
        userId
      )
    } yield assertEquals(result, Left(UseCaseError.ValidationFailed(cats.data.NonEmptyList.one(com.example.graphQL.cats.domain.error.DomainValidationError.BlankField("name")))))
  }

  test("token issuance failure does not persist an account and is not a repository failure") {
    val unavailableIssuer = new AccessTokenIssuer[IO] {
      override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
        IO.pure(Left(AccessTokenIssuanceError.Unavailable))
    }
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, unavailableIssuer)
      result <- service.signUp(
        SignUpInput("Candidate", UserRole.Candidate, "password-password", Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))),
        now,
        userId
      )
      stored <- accounts.values.get
    } yield {
      assertEquals(result, Left(UseCaseError.availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady)))
      assertEquals(stored, Map.empty)
    }
  }

  test("profile update rejects non-applicable role fields before persistence") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map(recruiter.id -> recruiter)), accounts, TestHasher, TestTokenIssuer)
      result <- service.updateMyProfile(
        ActorContext(recruiter.id, UserRole.Recruiter),
        AccountProfileInput(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now
      )
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
  }

  test("profile update rejects the profile-less Admin role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map(admin.id -> admin)), accounts, TestHasher, TestTokenIssuer)
      result <- service.updateMyProfile(
        ActorContext(admin.id, UserRole.Admin),
        AccountProfileInput(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now
      )
    } yield assertEquals(result, Left(UseCaseError.account(AccountError.ProfileUnsupportedForRole)))
  }

  test("valid recruiter signup remains supported") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      result <- service.signUp(SignUpInput(
        "Recruiter",
        UserRole.Recruiter,
        "password-password",
        Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))
      ), now, userId)
    } yield assert(result.isRight)
  }

  test("signup maps a canonical-name conflict to NameTaken") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map.empty), accounts, TestHasher, TestTokenIssuer)
      first <- service.signUp(SignUpInput(
        "Candidate Name",
        UserRole.Candidate,
        "password-password",
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
      ), now, userId)
      duplicate <- service.signUp(SignUpInput(
        " candidate name ",
        UserRole.Candidate,
        "password-password",
        Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
      ), now, recruiterId)
    } yield {
      assert(first.isRight)
      assertEquals(duplicate, Left(UseCaseError.account(AccountError.NameTaken)))
    }
  }

  test("unknown accounts use the hasher's dummy verification path") {
    for {
      unknownVerifications <- Ref.of[IO, Int](0)
      accounts <- TestAccounts.create(initialized = true)
      hasher = new PasswordHasher[IO] {
        override def hash(password: String): IO[String] = IO.pure(s"hash:$password")
        override def verify(encoded: String, password: String): IO[Boolean] = IO.pure(false)
        override def verifyUnknown(password: String): IO[Unit] = unknownVerifications.update(_ + 1)
      }
      service = new UserAccountService(new TestUsers(Map.empty), accounts, hasher, TestTokenIssuer)
      result <- service.login(LoginInput("Unknown", "password-password"), now)
      calls <- unknownVerifications.get
    } yield {
      assertEquals(result, Left(UseCaseError.account(AccountError.InvalidCredentials)))
      assertEquals(calls, 1)
    }
  }

  test("Argon2 unknown-user verification accepts arbitrary credentials without retaining their hash") {
    val hasher = new Argon2PasswordHasher(iterations = 1, memoryKilobytes = 8192, parallelism = 1)
    hasher.verifyUnknown("first-password").flatMap(_ => hasher.verifyUnknown("second-password")).map(assertEquals(_, ()))
  }

  test("deleting an already deleted account is idempotent") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = new UserAccountService(new TestUsers(Map(deletedRecruiter.id -> deletedRecruiter)), accounts, TestHasher, TestTokenIssuer)
      result <- service.deleteMyAccount(ActorContext(deletedRecruiter.id, UserRole.Recruiter), now)
    } yield assertEquals(result, Right(()))
  }

  private object TestHasher extends PasswordHasher[IO] {
    override def hash(password: String): IO[String] = IO.pure(s"hash:$password")
    override def verify(encoded: String, password: String): IO[Boolean] = IO.pure(encoded == s"hash:$password")
    override def verifyUnknown(password: String): IO[Unit] = IO.unit
  }

  private object TestTokenIssuer extends AccessTokenIssuer[IO] {
    override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
      IO.pure(Right(AccountToken(s"token-${user.id.value}", now.plusSeconds(900))))
  }

  private final class TestUsers(values: Map[UserId, User]) extends UserRepository[IO] {
    val ref: IO[Map[UserId, User]] = IO.pure(values)
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] = IO.pure(Right(values.get(id)))
    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] = IO.pure(Right(ids.flatMap(values.get)))
    override def updateEmbedding(id: UserId, observedVersion: Long, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
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
    override def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]] = values.get.map(values => Right(values.get(nameCanonical)))
    override def updateProfile(userId: UserId, profile: UserProfile, now: Instant): IO[Either[RepositoryError, User]] = IO.pure(Left(RepositoryError.Unavailable))
    override def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]] = IO.pure(Right(Nil))
    override def deleteAccount(userId: UserId, now: Instant, tombstone: String): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }

  private object TestAccounts {
    def create(initialized: Boolean): IO[TestAccounts] = Ref.of[IO, Map[String, AccountCredentials]](Map.empty).map(new TestAccounts(initialized, _))
  }
}
