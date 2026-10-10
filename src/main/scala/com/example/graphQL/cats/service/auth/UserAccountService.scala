package com.example.graphQL.cats.service.auth

import cats.data.EitherT
import cats.data.ValidatedNel
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{
  AnalyticsErasureRequestRepository,
  MutationWriteContext,
  UserAccountRepository,
  UserRepository,
  MutationEntityReference
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.*
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.shared.Parsing
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
        now <- EitherT.liftF(clock.realTimeInstant)
        userId <- EitherT.liftF(uuidGen.randomUUID.map(UserId.apply))
        result <- signUpOnce(input, now, userId, context)
      } yield result
    }

  private def signUpOnce(
      input: SignUpInput,
      now: Instant,
      userId: UserId,
      context: MutationWriteContext
  ): UseCaseIO[(User, AccountToken)] =
    if (input.role == UserRole.Admin) EitherT.leftT(UseCaseError.Account(AccountError.AdminSignupForbidden))
    else if (!UserProfile.matchesRole(input.role, input.profile))
      EitherT.leftT(UseCaseError.Account(AccountError.ProfileRoleMismatch))
    else
      (for {
        name <- EitherT.fromEither[IO](
          validateRegistration(input).toEither.leftMap(UseCaseError.ValidationFailed.apply)
        )
        _ <- UseCaseIO
          .repository(accounts.initialized)
          .subflatMap(initialized => Either.cond(initialized, (), UseCaseError.Account(AccountError.BootstrapRequired)))
        hash <- EitherT.liftF(hasher.hash(input.password))
        user = User(userId, None, name, input.role, input.profile, now)
        issued <- token(user, now)
        _ <- accounts.createAccount(user, hash, now, context).leftMap {
          case RepositoryError.Conflict => UseCaseError.Account(AccountError.NameTaken)
          case error                    => UseCaseError.Repository(error)
        }
      } yield issued).semiflatTap(_ => embeddingWork.wakeAfterCommit(diagnostics))

  override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute(
      "login",
      Idempotent.publicActorScope(input.name),
      protectedRequest("login", input.name, request),
      accountReference,
      replayAccount(input.name, input.password)
    ) { _ =>
      EitherT.liftF(clock.realTimeInstant).flatMap(now => loginOnce(input, now))
    }

  private def loginOnce(input: LoginInput, now: Instant): UseCaseIO[(User, AccountToken)] =
    UseCaseIO
      .repository(accounts.findByCanonicalName(canonicalName(input.name)))
      .flatMap(found =>
        verified(found.filter(_.user.accountStatus == AccountStatus.Active), input.password)(token(_, now))
      )

  /** Every refusal costs one password verification, so an unknown or inactive account is not distinguishable. */
  private def verified(credentials: Option[AccountCredentials], password: String)(
      issue: User => UseCaseIO[(User, AccountToken)]
  ): UseCaseIO[(User, AccountToken)] =
    credentials match {
      case Some(known) =>
        EitherT.liftF(hasher.verify(known.passwordHash, password)).flatMap {
          case true  => issue(known.user)
          case false => EitherT.leftT(UseCaseError.Account(AccountError.InvalidCredentials))
        }
      case None =>
        EitherT.liftF(hasher.verifyUnknown(password)) *>
          EitherT.leftT(UseCaseError.Account(AccountError.InvalidCredentials))
    }

  override def me(actor: ActorContext): UseCaseIO[User] =
    authorization.resolve(actor)

  override def updateMyProfile(
      request: IdempotencyRequest,
      actor: ActorContext,
      input: AccountProfileInput
  ): UseCaseIO[User] =
    idempotent.executeFor[User](actor, "updateMyProfile", request, userReference, replayUser(actor)) { context =>
      for {
        now <- EitherT.liftF(clock.realTimeInstant)
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
        EitherT.leftT(UseCaseError.Account(AccountError.ProfileUnsupportedForRole))
      else if (!UserProfile.matchesRole(user.role, Some(input.profile)))
        EitherT.leftT(UseCaseError.Account(AccountError.ProfileRoleMismatch))
      else
        EitherT.fromEither[IO](
          UserProfile.validateFor(user.role, Some(input.profile)).toEither.leftMap(UseCaseError.ValidationFailed.apply)
        ) *>
          UseCaseIO
            .repository(accounts.updateProfile(user.id, input.profile, now, context))
            .semiflatTap(_ => embeddingWork.wakeAfterCommit(diagnostics))
    }

  override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[String] =
    idempotent.executeFor[String](actor, "deleteMyAccount", request, receiptReference, replayDeletion(actor)) {
      context =>
        EitherT.liftF(clock.realTimeInstant).flatMap(now => deleteMyAccountOnce(actor, now, context))
    }

  override def accountDeletionStatus(actor: ActorContext, receiptId: String): UseCaseIO[AccountDeletionStatus] =
    UseCaseIO.repository(erasureRequests.statusForSubject(actor.userId, receiptId))

  private def deleteMyAccountOnce(
      actor: ActorContext,
      now: Instant,
      context: MutationWriteContext
  ): UseCaseIO[String] =
    if (context eq MutationWriteContext.directWrite)
      EitherT.leftT(UseCaseError.Analytics(AnalyticsError.ErasureContextRequired))
    else
      authorization.resolve(actor, allowDeleted = true).flatMap {
        case user if user.role == UserRole.Admin =>
          EitherT.leftT(UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation))
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
      // Deliberate anti-enumeration: every actor-lookup failure looks like Unauthorized, never Forbidden or Unavailable.
      .leftMap(_ => UseCaseError.Authentication(AuthenticationError.Unauthorized))
      .flatMap {
        case user if user.role == UserRole.Admin => UseCaseIO.repository(accounts.listAccounts(page))
        case _ => EitherT.leftT(UseCaseError.Authentication(AuthenticationError.Unauthorized))
      }

  private def protectedRequest(operation: String, name: String, request: IdempotencyRequest): IdempotencyRequest =
    request.copy(fingerprint =
      authenticationFingerprint.protect(operation, Idempotent.publicActorScope(name), request.fingerprint)
    )

  private def replayAccount(name: String, password: String)(
      reference: MutationEntityReference
  ): UseCaseIO[(User, AccountToken)] =
    if (reference.entityType != "user") Idempotent.corruptReference
    else
      replayKnownAccount(name, password, reference)

  private def replayKnownAccount(
      name: String,
      password: String,
      reference: MutationEntityReference
  ): UseCaseIO[(User, AccountToken)] =
    UseCaseIO
      .repository(accounts.findByCanonicalName(canonicalName(name)))
      .flatMap(found =>
        verified(
          found.filter(credentials =>
            credentials.user.id.value.toString == reference.entityId &&
              credentials.user.accountStatus == AccountStatus.Active &&
              canonicalName(credentials.user.name) == canonicalName(name)
          ),
          password
        )(user => EitherT.liftF(clock.realTimeInstant).flatMap(token(user, _)))
      )

  private def replayUser(actor: ActorContext): MutationEntityReference => UseCaseIO[User] =
    Idempotent.replayById("user", UserId.apply) { userId =>
      if (userId == actor.userId) me(actor)
      else EitherT.leftT(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    }

  private def replayDeletion(actor: ActorContext)(reference: MutationEntityReference): UseCaseIO[String] =
    if (reference.entityType == "analytics-erasure-receipt" && validReceiptId(reference.entityId))
      authorization.resolve(actor, allowDeleted = true).map(_ => reference.entityId)
    else Idempotent.corruptReference

  private def accountReference(value: (User, AccountToken)): MutationEntityReference = userReference(value._1)

  private def userReference(user: User): MutationEntityReference = MutationEntityReference.of("user", user.id.value)

  private def receiptReference(value: String): MutationEntityReference =
    MutationEntityReference("analytics-erasure-receipt", value)

  private def validReceiptId(value: String): Boolean =
    Parsing.parseUuid(value).exists(_.toString == value)

  private def token(user: User, now: Instant): UseCaseIO[(User, AccountToken)] =
    EitherT(
      tokenIssuer
        .issue(user, now)
        .handleErrorWith(error =>
          diagnostics
            .emit(LogEvent.AccessTokenIssuanceFailed, fields = LogFields.failure(error))
            .as(Left(AccessTokenIssuanceError.Unavailable))
        )
        .map(_.leftMap(_ => UseCaseError.Availability(AvailabilityError.ServiceNotReady)).map(user -> _))
    )

  private def validateRegistration(input: SignUpInput): ValidatedNel[DomainValidationError, String] =
    (Credentials.validate(input.name, input.password), UserProfile.validateFor(input.role, input.profile)).mapN(
      (name, _) => name
    )

  private def canonicalName(value: String): String =
    AccountName.canonical(value)
}
