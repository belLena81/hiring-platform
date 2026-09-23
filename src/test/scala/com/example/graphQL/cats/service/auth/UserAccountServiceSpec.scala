package com.example.graphQL.cats.service.auth

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{
  AnalyticsErasureRequestRepository,
  MutationEntityReference,
  MutationReceiptExecution,
  MutationReceiptFingerprint,
  MutationReceiptKey,
  MutationReceiptRepository,
  MutationWriteOutcome,
  MutationWriteContext,
  UserAccountRepository,
  UserRepository
}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{AccountError, ActorContext, AnalyticsError, UseCaseError}
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.*
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class UserAccountServiceSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-18T10:00:00Z")
  private val userId = UserId(UUID.fromString("20000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("20000000-0000-0000-0000-000000000002"))
  private val recruiter = User(
    recruiterId,
    None,
    "Recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))),
    now
  )
  private val admin = User(userId, None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
  private val deletedRecruiter =
    recruiter.copy(accountStatus = AccountStatus.Deleted, profile = None, deletedAt = Some(now))
  private val request = IdempotencyRequest.fromCanonicalInput(
    UUID.fromString("20000000-0000-0000-0000-000000000003"),
    "{}"
  )

  test("ordinary signup cannot create an Admin") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      result <- service.signUp(request, SignUpInput("Admin", UserRole.Admin, "password-password", None)).value
    } yield assertEquals(result, Left(UseCaseError.Account(AccountError.AdminSignupForbidden)))
  }

  test("signup rejects a profile belonging to another role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      result <- service
        .signUp(
          request,
          SignUpInput(
            "Candidate",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))
          )
        )
        .value
    } yield assertEquals(result, Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
  }

  test("candidate and recruiter signup require their matching profile") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      candidate <- service
        .signUp(request, SignUpInput("Candidate", UserRole.Candidate, "password-password", None))
        .value
      recruiter <- service
        .signUp(request, SignUpInput("Recruiter", UserRole.Recruiter, "password-password", None))
        .value
    } yield {
      assertEquals(candidate, Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
      assertEquals(recruiter, Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
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
      service = accountService(new TestUsers(Map.empty), accounts)
      result <- service
        .signUp(
          request,
          SignUpInput(
            "Candidate",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
          )
        )
        .value
      stored <- accounts.values.get
    } yield {
      assert(result.exists(_._2.value.nonEmpty))
      assertEquals(stored.size, 1)
    }
  }

  test("signup reports a blank name once while retaining other registration validation") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      result <- service
        .signUp(
          request,
          SignUpInput(
            "   ",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
          )
        )
        .value
    } yield assertEquals(
      result,
      Left(
        UseCaseError.ValidationFailed(
          cats.data.NonEmptyList.one(com.example.graphQL.cats.domain.error.DomainValidationError.BlankField("name"))
        )
      )
    )
  }

  test("token issuance failure does not persist an account and is not a repository failure") {
    val unavailableIssuer = new AccessTokenIssuer {
      override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
        IO.pure(Left(AccessTokenIssuanceError.Unavailable))
    }
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts, tokenIssuer = unavailableIssuer)
      result <- service
        .signUp(
          request,
          SignUpInput(
            "Candidate",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
          )
        )
        .value
      stored <- accounts.values.get
    } yield {
      assertEquals(
        result,
        Left(UseCaseError.Availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady))
      )
      assertEquals(stored, Map.empty)
    }
  }

  test("profile update rejects non-applicable role fields before persistence") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map(recruiter.id -> recruiter)), accounts)
      result <- service
        .updateMyProfile(
          request,
          ActorContext(recruiter.id, UserRole.Recruiter),
          AccountProfileInput(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        )
        .value
    } yield assertEquals(result, Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
  }

  test("profile update rejects the profile-less Admin role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map(admin.id -> admin)), accounts)
      result <- service
        .updateMyProfile(
          request,
          ActorContext(admin.id, UserRole.Admin),
          AccountProfileInput(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
        )
        .value
    } yield assertEquals(result, Left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole)))
  }

  test("valid recruiter signup remains supported") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      result <- service
        .signUp(
          request,
          SignUpInput(
            "Recruiter",
            UserRole.Recruiter,
            "password-password",
            Some(UserProfile.Recruiter(RecruiterProfile("Acme", None)))
          )
        )
        .value
    } yield assert(result.isRight)
  }

  test("signup maps a canonical-name conflict to NameTaken") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(new TestUsers(Map.empty), accounts)
      first <- service
        .signUp(
          request,
          SignUpInput(
            "Candidate Name",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
          )
        )
        .value
      duplicate <- service
        .signUp(
          request,
          SignUpInput(
            " candidate name ",
            UserRole.Candidate,
            "password-password",
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None)))
          )
        )
        .value
    } yield {
      assert(first.isRight)
      assertEquals(duplicate, Left(UseCaseError.Account(AccountError.NameTaken)))
    }
  }

  test("unknown accounts use the hasher's dummy verification path") {
    for {
      unknownVerifications <- Ref.of[IO, Int](0)
      accounts <- TestAccounts.create(initialized = true)
      hasher = new PasswordHasher {
        override def hash(password: String): IO[String] = IO.pure(s"hash:$password")
        override def verify(encoded: String, password: String): IO[Boolean] = IO.pure(false)
        override def verifyUnknown(password: String): IO[Unit] = unknownVerifications.update(_ + 1)
      }
      service = accountService(new TestUsers(Map.empty), accounts, hasher = hasher)
      result <- service.login(request, LoginInput("Unknown", "password-password")).value
      calls <- unknownVerifications.get
    } yield {
      assertEquals(result, Left(UseCaseError.Account(AccountError.InvalidCredentials)))
      assertEquals(calls, 1)
    }
  }

  test("Argon2 unknown-user verification accepts arbitrary credentials without retaining their hash") {
    Semaphore[IO](1).flatMap { permits =>
      val hasher = new Argon2PasswordHasher(iterations = 1, memoryKilobytes = 8192, parallelism = 1, permits)
      hasher
        .verifyUnknown("first-password")
        .flatMap(_ => hasher.verifyUnknown("second-password"))
        .map(assertEquals(_, ()))
    }
  }

  test("deleting an already deleted account is idempotent") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(deletedRecruiter.id -> deletedRecruiter)),
        accounts,
        erasureRequests = TestErasureRequests,
        idempotent = Idempotent(TransactionalReceipts)
      )
      result <- service.deleteMyAccount(request, ActorContext(deletedRecruiter.id, UserRole.Recruiter)).value
    } yield assertEquals(result, Right(()))
  }

  test("account deletion replay rejects a forged actor role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(deletedRecruiter.id -> deletedRecruiter)),
        accounts,
        erasureRequests = TestErasureRequests,
        idempotent = Idempotent(ReplayReceipts(MutationEntityReference("user", deletedRecruiter.id.value.toString)))
      )
      result <- service.deleteMyAccount(request, ActorContext(deletedRecruiter.id, UserRole.Admin)).value
    } yield assertEquals(result, Left(UseCaseError.Domain(com.example.graphQL.cats.domain.error.DomainError.Forbidden)))
  }

  test("analytics-aware account deletion rejects a non-transactional context") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(recruiter.id -> recruiter)),
        accounts,
        erasureRequests = TestErasureRequests
      )
      result <- service.deleteMyAccount(request, ActorContext(recruiter.id, UserRole.Recruiter)).value
    } yield assertEquals(result, Left(UseCaseError.Analytics(AnalyticsError.ErasureContextRequired)))
  }

  private def accountService(
      users: UserRepository,
      accounts: UserAccountRepository,
      hasher: PasswordHasher = TestHasher,
      tokenIssuer: AccessTokenIssuer = TestTokenIssuer,
      erasureRequests: AnalyticsErasureRequestRepository = AnalyticsErasureRequestRepository.unavailable,
      idempotent: Idempotent = Idempotent.noop
  ): UserAccountService =
    new UserAccountService(
      users,
      accounts,
      hasher,
      tokenIssuer,
      erasureRequests = erasureRequests,
      idempotent = idempotent,
      currentTime = IO.pure(now),
      randomId = IO.pure(userId.value)
    )

  private object TestHasher extends PasswordHasher {
    override def hash(password: String): IO[String] = IO.pure(s"hash:$password")
    override def verify(encoded: String, password: String): IO[Boolean] = IO.pure(encoded == s"hash:$password")
    override def verifyUnknown(password: String): IO[Unit] = IO.unit
  }

  private object TestTokenIssuer extends AccessTokenIssuer {
    override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
      IO.pure(Right(AccountToken(s"token-${user.id.value}", now.plusSeconds(900))))
  }

  private object TestErasureRequests extends AnalyticsErasureRequestRepository {
    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }

  private object TransactionalReceipts extends MutationReceiptRepository {
    private val context = new MutationWriteContext {}

    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
    ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
      val _ = (key, fingerprint, now, expiresAt)
      write(context).map(
        _.map {
          case MutationWriteOutcome.Rejected(error) => MutationReceiptExecution.Rejected(error)
          case MutationWriteOutcome.Applied(value)  => MutationReceiptExecution.Applied(value.value, value.entity)
        }
      )
    }
  }

  private final case class ReplayReceipts(reference: MutationEntityReference) extends MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => IO[Either[RepositoryError, MutationWriteOutcome[A, E]]]
    ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] = {
      val _ = (key, fingerprint, now, expiresAt, write)
      IO.pure(Right(MutationReceiptExecution.Replay(reference)))
    }
  }

  private final class TestUsers(values: Map[UserId, User]) extends UserRepository {
    val ref: IO[Map[UserId, User]] = IO.pure(values)
    override def find(id: UserId): IO[Either[RepositoryError, Option[User]]] = IO.pure(Right(values.get(id)))
    override def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]] =
      IO.pure(Right(ids.flatMap(values.get)))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
  }

  private final class TestAccounts(
      initializedState: Boolean,
      val values: Ref[IO, Map[String, AccountCredentials]]
  ) extends UserAccountRepository {
    override def bootstrap(
        user: User,
        passwordHash: String,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Unit]] = IO.pure(Left(RepositoryError.Conflict))
    override def initialized: IO[Either[RepositoryError, Boolean]] = IO.pure(Right(initializedState))
    override def createAccount(
        user: User,
        passwordHash: String,
        now: Instant,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Unit]] =
      values.modify { current =>
        val key = AccountName.canonical(user.name)
        if (current.contains(key)) current -> Left(RepositoryError.Conflict)
        else (current.updated(key, AccountCredentials(user, passwordHash)), Right(()))
      }
    override def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]] =
      values.get.map(values => Right(values.get(nameCanonical)))
    override def updateProfile(
        userId: UserId,
        profile: UserProfile,
        now: Instant,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, User]] = IO.pure(Left(RepositoryError.Unavailable))
    override def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]] = IO.pure(Right(Nil))
    override def deleteAccount(
        userId: UserId,
        now: Instant,
        tombstone: String,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }

  private object TestAccounts {
    def create(initialized: Boolean): IO[TestAccounts] =
      Ref.of[IO, Map[String, AccountCredentials]](Map.empty).map(new TestAccounts(initialized, _))
  }
}
