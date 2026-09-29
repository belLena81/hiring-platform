package com.example.graphQL.cats.service.auth

import cats.effect.{IO, Ref}
import cats.effect.std.Semaphore
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{
  AnalyticsErasureRequestRepository,
  MutationEntityReference,
  MutationReceiptExecution,
  MutationReceiptFingerprint,
  MutationReceiptKey,
  MutationReceiptRepository,
  MutationWriteOutcome,
  MutationWriteContext,
  RepositoryIO,
  UserAccountRepository,
  UserRepository
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{
  AccountError,
  ActorContext,
  AnalyticsError,
  Diagnostics,
  LogEvent,
  LogField,
  ServiceFixtures,
  UseCaseError
}
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.*
import munit.CatsEffectSuite
import com.example.graphQL.cats.AccountValueFixtures.passwordHash

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

  test("signup replay reissues an access token for the active account") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(recruiter.id -> recruiter)),
        accounts,
        idempotent = Idempotent(ReplayReceipts(MutationEntityReference("user", recruiter.id.value.toString)))
      )
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
    } yield assertEquals(result, Right(recruiter -> AccountToken(s"token-${recruiter.id.value}", now.plusSeconds(900))))
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

  test("unexpected token issuance failure records safe diagnostics and preserves availability result") {
    val failedIssuer = new AccessTokenIssuer {
      override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
        IO.raiseError(new IllegalStateException("sensitive token detail"))
    }
    for {
      accounts <- TestAccounts.create(initialized = true)
      captured <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(
            event: LogEvent,
            requestId: Option[String],
            fields: => Map[LogField, String]
        ): IO[Unit] = captured.update((event, fields) :: _)
      }
      service = accountService(
        new TestUsers(Map.empty),
        accounts,
        tokenIssuer = failedIssuer,
        diagnostics = diagnostics
      )
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
      entries <- captured.get
      stored <- accounts.values.get
    } yield {
      assertEquals(
        result,
        Left(UseCaseError.Availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady))
      )
      assertEquals(stored, Map.empty)
      assertEquals(entries.map(_._1), List(LogEvent.AccessTokenIssuanceFailed))
      assertEquals(entries.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assert(!entries.head._2.values.exists(_.contains("sensitive token detail")))
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
        override def hash(password: String): IO[PasswordHash] = IO.pure(passwordHash(s"hash:$password"))
        override def verify(encoded: PasswordHash, password: String): IO[Boolean] = IO.pure(false)
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
      Argon2PasswordHasher.resource(iterations = 1, memoryKilobytes = 8192, parallelism = 1, permits).use { hasher =>
        hasher
          .verifyUnknown("first-password")
          .flatMap(_ => hasher.verifyUnknown("second-password"))
          .map(assertEquals(_, ()))
      }
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
    } yield assert(result.exists(_.matches("[0-9a-f-]{36}")))
  }

  test("account deletion replay rejects a forged actor role") {
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(deletedRecruiter.id -> deletedRecruiter)),
        accounts,
        erasureRequests = TestErasureRequests,
        idempotent = Idempotent(
          ReplayReceipts(MutationEntityReference("analytics-erasure-receipt", "00000000-0000-0000-0000-000000000123"))
        )
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

  test("account deletion fails closed when the erasure worker is not ready") {
    val unavailableWorker = new AnalyticsErasureRequestRepository {
      override def workerReady(now: Instant): RepositoryIO[Unit] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))

      override def enqueue(
          userId: UserId,
          now: Instant,
          context: MutationWriteContext
      ): RepositoryIO[String] = com.example.graphQL.cats.service.port.RepositoryIO
        .fromIOEither(IO.raiseError(new AssertionError("deletion must not enqueue when worker preflight fails")))

      override def statusForSubject(
          userId: UserId,
          receiptId: String
      ): RepositoryIO[com.example.graphQL.cats.domain.model.AccountDeletionStatus] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))

      override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))

      override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))
    }
    for {
      accounts <- TestAccounts.create(initialized = true)
      service = accountService(
        new TestUsers(Map(recruiter.id -> recruiter)),
        accounts,
        erasureRequests = unavailableWorker,
        idempotent = Idempotent(TransactionalReceipts)
      )
      result <- service.deleteMyAccount(request, ActorContext(recruiter.id, UserRole.Recruiter)).value
    } yield assertEquals(result, Left(UseCaseError.Analytics(AnalyticsError.ErasureWorkerUnavailable)))
  }

  private def accountService(
      users: UserRepository,
      accounts: UserAccountRepository,
      hasher: PasswordHasher = TestHasher,
      tokenIssuer: AccessTokenIssuer = TestTokenIssuer,
      erasureRequests: AnalyticsErasureRequestRepository = AnalyticsErasureRequestRepository.unavailable,
      idempotent: Idempotent = com.example.graphQL.cats.service.mutation.TestIdempotency.noop,
      diagnostics: Diagnostics = Diagnostics.noop
  ): UserAccountService =
    new UserAccountService(
      users,
      accounts,
      hasher,
      tokenIssuer,
      erasureRequests = erasureRequests,
      embeddingWork = com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher.noop,
      idempotent = idempotent,
      diagnostics = diagnostics,
      clock = FixedTestClock.at(now),
      uuidGen = new UUIDGen[IO] {
        override def randomUUID: IO[UUID] = IO.pure(userId.value)
      }
    )

  private object TestHasher extends PasswordHasher {
    override def hash(password: String): IO[PasswordHash] = IO.pure(passwordHash(s"hash:$password"))
    override def verify(encoded: PasswordHash, password: String): IO[Boolean] =
      IO.pure(encoded.encoded == s"hash:$password")
    override def verifyUnknown(password: String): IO[Unit] = IO.unit
  }

  private object TestTokenIssuer extends AccessTokenIssuer {
    override def issue(user: User, now: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
      IO.pure(Right(AccountToken(s"token-${user.id.value}", now.plusSeconds(900))))
  }

  private object TestErasureRequests extends AnalyticsErasureRequestRepository {
    override def workerReady(now: Instant): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))

    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryIO[String] = com.example.graphQL.cats.service.port.RepositoryIO
      .fromIOEither(IO.pure(Right("00000000-0000-0000-0000-000000000123")))

    override def statusForSubject(
        userId: UserId,
        receiptId: String
    ): RepositoryIO[com.example.graphQL.cats.domain.model.AccountDeletionStatus] =
      com.example.graphQL.cats.service.port.RepositoryIO
        .fromIOEither(IO.pure(Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Pending)))

    override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))

    override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))
  }

  private object TransactionalReceipts extends MutationReceiptRepository {
    private val context = new MutationWriteContext {}

    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => RepositoryIO[MutationWriteOutcome[A, E]]
    ): RepositoryIO[MutationReceiptExecution[A, E]] = {
      val _ = (key, fingerprint, now, expiresAt)
      write(context).map(
        {
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
        write: MutationWriteContext => RepositoryIO[MutationWriteOutcome[A, E]]
    ): RepositoryIO[MutationReceiptExecution[A, E]] = com.example.graphQL.cats.service.port.RepositoryIO.fromEither(
      Right(MutationReceiptExecution.Replay(reference))
    )
  }

  private final class TestUsers(values: Map[UserId, User]) extends ServiceFixtures.VersionedUserRepositoryTestAdapter {
    val ref: IO[Map[UserId, User]] = IO.pure(values)
    override def find(id: UserId): RepositoryIO[Option[User]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(values.get(id))))
    override def findMany(ids: List[UserId]): RepositoryIO[List[User]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(ids.flatMap(values.get))))
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))
  }

  private final class TestAccounts(
      initializedState: Boolean,
      val values: Ref[IO, Map[String, AccountCredentials]]
  ) extends UserAccountRepository {
    override def bootstrap(
        user: User,
        passwordHash: PasswordHash,
        context: MutationWriteContext
    ): RepositoryIO[Unit] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Conflict)))
    override def initialized: RepositoryIO[Boolean] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(initializedState)))
    override def createAccount(
        user: User,
        passwordHash: PasswordHash,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(values.modify { current =>
      val key = AccountName.canonical(user.name)
      if (current.contains(key)) current -> Left(RepositoryError.Conflict)
      else (current.updated(key, AccountCredentials(user, passwordHash)), Right(()))
    })
    override def findByCanonicalName(nameCanonical: String): RepositoryIO[Option[AccountCredentials]] =
      com.example.graphQL.cats.service.port.RepositoryIO
        .fromIOEither(values.get.map(values => Right(values.get(nameCanonical))))
    override def updateProfile(
        userId: UserId,
        profile: UserProfile,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryIO[User] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))
    override def listAccounts(page: UserPageRequest): RepositoryIO[List[User]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
    override def deleteAccount(
        userId: UserId,
        now: Instant,
        tombstone: String,
        context: MutationWriteContext
    ): RepositoryIO[Unit] = com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(())))
  }

  private object TestAccounts {
    def create(initialized: Boolean): IO[TestAccounts] =
      Ref.of[IO, Map[String, AccountCredentials]](Map.empty).map(new TestAccounts(initialized, _))
  }
}
