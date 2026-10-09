package com.example.graphQL.cats.service.auth

import cats.data.ValidatedNel
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{UserId, parse as parseIdentifier}
import com.example.graphQL.cats.service.port.{
  AnalyticsErasureRequestRepository,
  MutationWriteContext,
  RepositoryError,
  UserAccountRepository,
  UserRepository
}
import com.example.graphQL.cats.service.*
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.shared.Parsing
import java.text.Normalizer
import java.time.Instant

final class UserAccountService(
    users: UserRepository,
    accounts: UserAccountRepository,
    hasher: PasswordHasher,
    tokenIssuer: AccessTokenIssuer,
    authenticationFingerprint: AuthenticationFingerprint,
    erasureRequests: AnalyticsErasureRequestRepository = AnalyticsErasureRequestRepository.unavailable,
    embeddingWork: EmbeddingWorkPublisher,
    idempotent: Idempotent,
    diagnostics: Diagnostics,
    clock: Clock[IO] = Clock[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends AccountUseCases {
  import Diagnostics.*
  private val authorization = ActorAuthorization(users)

  override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute(
      "signUp",
      Idempotent.publicActorScope(input.name),
      protectedRequest("signUp", input.name, request),
      accountReference,
      replayAccount(input.name, input.password)
    ) { context =>
      for {
        now <- UseCaseIO.liftIO(clock.realTimeInstant)
        userId <- UseCaseIO.liftIO(uuidGen.randomUUID.map(UserId.apply))
        result <- signUpOnce(input, now, userId, context)
      } yield result
    }

  private def signUpOnce(
      input: SignUpInput,
      now: Instant,
      userId: UserId,
      context: MutationWriteContext
  ): UseCaseIO[(User, AccountToken)] =
    if (input.role == UserRole.Admin) UseCaseIO.left(UseCaseError.Account(AccountError.AdminSignupForbidden))
    else if (!UserProfile.matchesRole(input.role, input.profile))
      UseCaseIO.left(UseCaseError.Account(AccountError.ProfileRoleMismatch))
    else
      (for {
        _ <- UseCaseIO.fromEither(validateRegistration(input).toEither.leftMap(UseCaseError.ValidationFailed.apply))
        _ <- UseCaseIO
          .repository(accounts.initialized)
          .subflatMap(initialized => Either.cond(initialized, (), UseCaseError.Account(AccountError.BootstrapRequired)))
        hash <- UseCaseIO.liftIO(hasher.hash(input.password))
        user = toUser(userId, input.name, input.role, input.profile, now)
        issued <- token(user, now)
        _ <- accounts.createAccount(user, hash, now, context).leftMap {
          case RepositoryError.Conflict => UseCaseError.Account(AccountError.NameTaken)
          case error                    => UseCaseError.Repository(error)
        }
      } yield issued).semiflatTap(_ => wakeCandidateAfterCommit)

  override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute(
      "login",
      Idempotent.publicActorScope(input.name),
      protectedRequest("login", input.name, request),
      accountReference,
      replayAccount(input.name, input.password)
    ) { _ =>
      UseCaseIO.liftIO(clock.realTimeInstant).flatMap(now => loginOnce(input, now))
    }

  private def loginOnce(input: LoginInput, now: Instant): UseCaseIO[(User, AccountToken)] =
    UseCaseIO.repository(accounts.findByCanonicalName(canonicalName(input.name))).flatMap {
      case Some(credentials) if credentials.user.accountStatus == AccountStatus.Active =>
        UseCaseIO.liftIO(hasher.verify(credentials.passwordHash, input.password)).flatMap {
          case false => UseCaseIO.left(UseCaseError.Account(AccountError.InvalidCredentials))
          case true  => token(credentials.user, now)
        }
      case _ =>
        UseCaseIO.liftIO(hasher.verifyUnknown(input.password)) *>
          UseCaseIO.left(UseCaseError.Account(AccountError.InvalidCredentials))
    }

  override def me(actor: ActorContext): UseCaseIO[User] =
    authorization.resolve(actor)

  override def updateMyProfile(
      request: IdempotencyRequest,
      actor: ActorContext,
      input: AccountProfileInput
  ): UseCaseIO[User] =
    idempotent.execute[User](
      "updateMyProfile",
      Idempotent.actorScope(actor),
      request,
      user => userReference(user),
      reference => replayUser(actor, reference)
    ) { context =>
      for {
        now <- UseCaseIO.liftIO(clock.realTimeInstant)
        user <- updateMyProfileOnce(actor, input, now, context)
      } yield user
    }

  private def updateMyProfileOnce(
      actor: ActorContext,
      input: AccountProfileInput,
      now: Instant,
      context: MutationWriteContext
  ): UseCaseIO[User] =
    authorization.resolve(actor).flatMap { user =>
      if (user.role == UserRole.Admin)
        UseCaseIO.left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole))
      else if (!UserProfile.matchesRole(user.role, Some(input.profile)))
        UseCaseIO.left(UseCaseError.Account(AccountError.ProfileRoleMismatch))
      else
        UseCaseIO.fromEither(
          UserProfile.validateFor(user.role, Some(input.profile)).toEither.leftMap(UseCaseError.ValidationFailed.apply)
        ) *>
          UseCaseIO
            .repository(accounts.updateProfile(user.id, input.profile, now, context))
            .semiflatTap(_ => wakeCandidateAfterCommit)
    }

  override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] =
    idempotent.execute[String](
      "deleteMyAccount",
      Idempotent.actorScope(actor),
      request,
      receiptReference,
      replayDeletion(actor)
    ) { context =>
      UseCaseIO.liftIO(clock.realTimeInstant).flatMap(now => deleteMyAccountOnce(actor, now, context))
    }

  override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
    UseCaseIO.repository(erasureRequests.statusForSubject(actor.userId, receiptId))

  private def deleteMyAccountOnce(
      actor: ActorContext,
      now: Instant,
      context: MutationWriteContext
  ): UseCaseIO[String] =
    if (context eq MutationWriteContext.directWrite)
      UseCaseIO.left(UseCaseError.Analytics(AnalyticsError.ErasureContextRequired))
    else
      authorization.resolve(actor, allowDeleted = true).flatMap {
        case user if user.role == UserRole.Admin =>
          UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation))
        case user if user.accountStatus == AccountStatus.Deleted =>
          UseCaseIO.repository(erasureRequests.enqueue(user.id, now, context))
        case user =>
          for {
            _ <- erasureRequests
              .workerReady(now)
              .leftMap(_ => UseCaseError.Analytics(AnalyticsError.ErasureWorkerUnavailable))
            receiptId <- UseCaseIO.repository(erasureRequests.enqueue(user.id, now, context))
            _ <- UseCaseIO.repository(accounts.deleteAccount(user.id, now, s"deleted-${user.id.value}", context))
          } yield receiptId
      }

  override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] =
    authorization
      .resolve(actor)
      .leftMap(_ => UseCaseError.Authentication(AuthenticationError.Unauthorized))
      .flatMap {
        case user if user.role == UserRole.Admin => UseCaseIO.repository(accounts.listAccounts(page))
        case _ => UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
      }

  private def protectedRequest(operation: String, name: String, request: IdempotencyRequest): IdempotencyRequest =
    request.copy(fingerprint =
      authenticationFingerprint.protect(operation, Idempotent.publicActorScope(name), request.fingerprint)
    )

  private def replayAccount(name: String, password: String)(
      reference: com.example.graphQL.cats.service.port.MutationEntityReference
  ): UseCaseIO[(User, AccountToken)] =
    if (reference.entityType != "user") UseCaseIO.left(UseCaseError.Repository(RepositoryError.Unavailable))
    else
      replayKnownAccount(name, password, reference)

  private def replayKnownAccount(
      name: String,
      password: String,
      reference: com.example.graphQL.cats.service.port.MutationEntityReference
  ): UseCaseIO[(User, AccountToken)] =
    UseCaseIO.repository(accounts.findByCanonicalName(canonicalName(name))).flatMap {
      case Some(credentials)
          if credentials.user.id.value.toString == reference.entityId &&
            credentials.user.accountStatus == AccountStatus.Active &&
            canonicalName(credentials.user.name) == canonicalName(name) =>
        UseCaseIO.liftIO(hasher.verify(credentials.passwordHash, password)).flatMap {
          case true  => UseCaseIO.liftIO(clock.realTimeInstant).flatMap(token(credentials.user, _))
          case false => UseCaseIO.left(UseCaseError.Account(AccountError.InvalidCredentials))
        }
      case _ =>
        UseCaseIO.liftIO(hasher.verifyUnknown(password)) *>
          UseCaseIO.left(UseCaseError.Account(AccountError.InvalidCredentials))
    }

  private def replayUser(
      actor: ActorContext,
      reference: com.example.graphQL.cats.service.port.MutationEntityReference
  ): UseCaseIO[User] =
    parseUserId(reference).flatMap { userId =>
      if (userId == actor.userId) me(actor)
      else UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    }

  private def replayDeletion(
      actor: ActorContext
  )(reference: com.example.graphQL.cats.service.port.MutationEntityReference): UseCaseIO[String] =
    if (reference.entityType == "analytics-erasure-receipt" && validReceiptId(reference.entityId))
      authorization.resolve(actor, allowDeleted = true).map(_ => reference.entityId)
    else UseCaseIO.left(UseCaseError.Repository(RepositoryError.Unavailable)) // corrupt stored reference

  private def parseUserId(
      reference: com.example.graphQL.cats.service.port.MutationEntityReference
  ): UseCaseIO[UserId] =
    parseIdentifier(reference.entityId)(UserId.apply)
      .fold(UseCaseIO.left(UseCaseError.Repository(RepositoryError.Unavailable)))(UseCaseIO.pure)

  private def accountReference(
      value: (User, AccountToken)
  ): com.example.graphQL.cats.service.port.MutationEntityReference =
    userReference(value._1)

  private def userReference(user: User): com.example.graphQL.cats.service.port.MutationEntityReference =
    userReference(user.id)

  private def userReference(userId: UserId): com.example.graphQL.cats.service.port.MutationEntityReference =
    com.example.graphQL.cats.service.port.MutationEntityReference("user", userId.value.toString)

  private def receiptReference(value: String): com.example.graphQL.cats.service.port.MutationEntityReference =
    com.example.graphQL.cats.service.port.MutationEntityReference("analytics-erasure-receipt", value)

  private def validReceiptId(value: String): Boolean =
    Parsing.parseUuid(value).exists(_.toString == value)

  private def token(user: User, now: Instant): UseCaseIO[(User, AccountToken)] =
    UseCaseIO.fromIO(
      tokenIssuer
        .issue(user, now)
        .handleErrorWith(error =>
          diagnostics
            .emit(LogEvent.AccessTokenIssuanceFailed, fields = LogFields.failure(error))
            .as(Left(AccessTokenIssuanceError.Unavailable))
        )
        .map(_.leftMap(_ => UseCaseError.Availability(AvailabilityError.ServiceNotReady)).map(user -> _))
    )

  private def validateRegistration(input: SignUpInput): ValidatedNel[DomainValidationError, Unit] =
    (validateCredentials(input.name, input.password), UserProfile.validateFor(input.role, input.profile)).mapN((_, _) =>
      ()
    )

  private def validateCredentials(name: String, password: String): ValidatedNel[DomainValidationError, Unit] =
    (validateName(name), validatePassword(password)).mapN((_, _) => ())

  private def validatePassword(password: String): ValidatedNel[DomainValidationError, Unit] =
    val byteLength = password.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
    if (byteLength < 12) DomainValidationError.BlankField("password").invalidNel
    else if (byteLength > FieldLimits.PasswordMaxBytes)
      DomainValidationError.ByteLengthExceeded("password", FieldLimits.PasswordMaxBytes, byteLength).invalidNel
    else ().validNel

  private def validateName(value: String): ValidatedNel[DomainValidationError, String] =
    val normalized = Normalizer.normalize(value.trim, Normalizer.Form.NFKC)
    if (normalized.isEmpty) DomainValidationError.BlankField("name").invalidNel
    else if (normalized.length > FieldLimits.ShortTextMaxChars)
      DomainValidationError.TextTooLong("name", FieldLimits.ShortTextMaxChars, normalized.length).invalidNel
    else normalized.validNel

  private def toUser(id: UserId, name: String, role: UserRole, profile: Option[UserProfile], now: Instant): User =
    User(id, None, Normalizer.normalize(name.trim, Normalizer.Form.NFKC), role, profile, now)

  private def canonicalName(value: String): String =
    AccountName.canonical(value)

  private def wakeCandidateAfterCommit: IO[Unit] =
    embeddingWork.wake.handleErrorWith(error =>
      diagnostics.emit(LogEvent.EmbeddingWakeFailed, fields = LogFields.failure(error))
    )
}
