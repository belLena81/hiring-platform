package com.example.graphQL.cats.service.auth

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{AnalyticsErasureRequestRepository, MutationWriteContext, RepositoryError, UserAccountRepository, UserRepository}
import com.example.graphQL.cats.service.*
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.protocol.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import java.text.Normalizer
import java.time.Instant
import java.util.UUID

final class UserAccountService(
    users: UserRepository,
    accounts: UserAccountRepository,
    hasher: PasswordHasher,
    tokenIssuer: AccessTokenIssuer,
    erasureRequests: AnalyticsErasureRequestRepository = AnalyticsErasureRequestRepository.unavailable,
    embeddingWork: EmbeddingWorkPublisher = EmbeddingWorkPublisher.noop,
    idempotent: Idempotent = Idempotent.noop,
    currentTime: IO[Instant] = IO.realTimeInstant,
    randomId: IO[UUID] = IO.randomUUID
) extends AccountUseCases {
  private val authorization = ActorAuthorization(users)

  override def signUp(request: IdempotencyRequest, input: SignUpInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute("signUp", Idempotent.publicActorScope(input.name), request, accountReference, replayAccount) { context =>
      for {
        now <- UseCaseIO.liftIO(currentTime)
        userId <- UseCaseIO.liftIO(randomId.map(UserId.apply))
        result <- UseCaseIO.fromIO(signUpOnce(input, now, userId, context))
      } yield result
    }

  private def signUpOnce(input: SignUpInput, now: Instant, userId: UserId, context: MutationWriteContext): IO[Either[UseCaseError, (User, AccountToken)]] =
    if (input.role == UserRole.Admin) IO.pure(Left(UseCaseError.Account(AccountError.AdminSignupForbidden)))
    else if (!UserProfile.matchesRole(input.role, input.profile))
      IO.pure(Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
    else validateRegistration(input).fold(
      errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
      _ => accounts.initialized.flatMap {
        case Left(error) => IO.pure(Left(UseCaseError.Repository(error)))
        case Right(false) => IO.pure(Left(UseCaseError.Account(AccountError.BootstrapRequired)))
        case Right(true) => hasher.hash(input.password).flatMap { hash =>
          val user = toUser(userId, input.name, input.role, input.profile, now)
          token(user, now).flatMap {
            case Left(error) => IO.pure(Left(error))
            case Right((_, accountToken)) => accounts.createAccount(user, hash, now, context).map {
              case Left(RepositoryError.Conflict) => Left(UseCaseError.Account(AccountError.NameTaken))
              case Left(error) => Left(UseCaseError.Repository(error))
              case Right(()) => Right(user -> accountToken)
            }
          }
        }
      }.flatTap(wakeCandidateAfterCommit)
    )

  override def bootstrapAdmin(request: IdempotencyRequest, input: BootstrapAdminInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute("bootstrapAdmin", Idempotent.publicActorScope(input.name), request, accountReference, replayAccount) { context =>
      for {
        now <- UseCaseIO.liftIO(currentTime)
        userId <- UseCaseIO.liftIO(randomId.map(UserId.apply))
        result <- UseCaseIO.fromIO(bootstrapAdminOnce(input, now, userId, context))
      } yield result
    }

  private def bootstrapAdminOnce(input: BootstrapAdminInput, now: Instant, userId: UserId, context: MutationWriteContext): IO[Either[UseCaseError, (User, AccountToken)]] =
    validateCredentials(input.name, input.password).fold(
      errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
      _ => hasher.hash(input.password).flatMap { hash =>
        val user = toUser(userId, input.name, UserRole.Admin, None, now).copy(adminSingleton = true)
        token(user, now).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right((_, accountToken)) => accounts.bootstrap(user, hash, context).map {
            case Left(RepositoryError.Conflict) => Left(UseCaseError.Account(AccountError.AlreadyBootstrapped))
            case Left(error) => Left(UseCaseError.Repository(error))
            case Right(()) => Right(user -> accountToken)
          }
        }
      }
    )

  override def login(request: IdempotencyRequest, input: LoginInput): UseCaseIO[(User, AccountToken)] =
    idempotent.execute("login", Idempotent.publicActorScope(input.name), request, accountReference, replayAccount) { _ =>
      UseCaseIO.liftIO(currentTime).flatMap(now => UseCaseIO.fromIO(loginOnce(input, now)))
    }

  private def loginOnce(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] = {
    val canonical = canonicalName(input.name)
    accounts.findByCanonicalName(canonical).flatMap {
      case Left(error) => IO.pure(Left(UseCaseError.Repository(error)))
      case Right(None) => hasher.verifyUnknown(input.password).as(Left(UseCaseError.Account(AccountError.InvalidCredentials)))
      case Right(Some(credentials)) if credentials.user.accountStatus != AccountStatus.Active =>
        hasher.verifyUnknown(input.password).as(Left(UseCaseError.Account(AccountError.InvalidCredentials)))
      case Right(Some(credentials)) => hasher.verify(credentials.passwordHash, input.password).flatMap {
        case false => IO.pure(Left(UseCaseError.Account(AccountError.InvalidCredentials)))
        case true => token(credentials.user, now)
      }
    }
  }

  override def issueToken(userId: UserId, now: Instant): UseCaseIO[(User, AccountToken)] =
    UseCaseIO.repository(users.find(userId))
      .subflatMap {
        case Some(user) if user.accountStatus == AccountStatus.Active => Right(user)
        case _ => Left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
      }
      .flatMap(user => UseCaseIO.fromIO(token(user, now)))

  override def me(actor: ActorContext): UseCaseIO[User] =
    authorization.resolve(actor)

  override def updateMyProfile(request: IdempotencyRequest, actor: ActorContext, input: AccountProfileInput): UseCaseIO[User] =
    idempotent.execute[User]("updateMyProfile", Idempotent.actorScope(actor), request, user => userReference(user), reference => replayUser(actor, reference)) { context =>
      for {
        now <- UseCaseIO.liftIO(currentTime)
        user <- UseCaseIO.fromIO(updateMyProfileOnce(actor, input, now, context))
      } yield user
    }

  private def updateMyProfileOnce(actor: ActorContext, input: AccountProfileInput, now: Instant, context: MutationWriteContext): IO[Either[UseCaseError, User]] =
    authorization.resolve(actor).value.flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(user) if user.role == UserRole.Admin => IO.pure(Left(UseCaseError.Account(AccountError.ProfileUnsupportedForRole)))
      case Right(user) =>
        if (!UserProfile.matchesRole(user.role, Some(input.profile))) IO.pure(Left(UseCaseError.Account(AccountError.ProfileRoleMismatch)))
        else UserProfile.validateFor(user.role, Some(input.profile)).fold(
          errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
          _ => accounts.updateProfile(user.id, input.profile, now, context).map(_.leftMap(UseCaseError.Repository.apply)).flatTap(wakeCandidateAfterCommit)
        )
    }

  override def deleteMyAccount(request: IdempotencyRequest, actor: ActorContext): UseCaseIO[Unit] =
    idempotent.execute[Unit]("deleteMyAccount", Idempotent.actorScope(actor), request, _ => userReference(actor.userId), replayDeletion(actor)) { context =>
      UseCaseIO.liftIO(currentTime).flatMap(now => UseCaseIO.fromIO(deleteMyAccountOnce(actor, now, context)))
    }

  private def deleteMyAccountOnce(actor: ActorContext, now: Instant, context: MutationWriteContext): IO[Either[UseCaseError, Unit]] =
    if (context eq MutationWriteContext.noop) IO.pure(Left(UseCaseError.Analytics(AnalyticsError.ErasureContextRequired)))
    else authorization.resolve(actor, allowDeleted = true).value.flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(user) if user.role == UserRole.Admin => IO.pure(Left(UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation)))
      case Right(user) if user.accountStatus == AccountStatus.Deleted => IO.pure(Right(()))
      case Right(user) =>
        erasureRequests.enqueue(user.id, now, context).flatMap {
          case Left(error) => IO.pure(Left(UseCaseError.Repository(error)))
          case Right(()) => accounts.deleteAccount(user.id, now, s"deleted-${user.id.value}", context).map(_.leftMap(UseCaseError.Repository.apply))
        }
    }

  override def listUsers(actor: ActorContext, page: UserPageRequest): UseCaseIO[List[User]] =
    UseCaseIO.fromIO(authorization.resolve(actor).value.flatMap {
      case Right(user) if user.role == UserRole.Admin =>
        accounts.listAccounts(page).map(_.widenUseCase)
      case _ => IO.pure(Left(UseCaseError.Authentication(AuthenticationError.Unauthorized)))
    })

  private def replayAccount(reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference): UseCaseIO[(User, AccountToken)] =
    parseUserId(reference).flatMap(userId => UseCaseIO.liftIO(currentTime).flatMap(issueToken(userId, _)))

  private def replayUser(actor: ActorContext, reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference): UseCaseIO[User] =
    parseUserId(reference).flatMap { userId =>
      if (userId == actor.userId) me(actor)
      else UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    }

  private def replayDeletion(actor: ActorContext)(reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference): UseCaseIO[Unit] =
    parseUserId(reference).flatMap { userId =>
      if (reference.entityType == "user" && userId == actor.userId)
        authorization.resolve(actor, allowDeleted = true).void
      else UseCaseIO.left(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    }

  private def parseUserId(reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference): UseCaseIO[UserId] =
    scala.util.Try(UserId(UUID.fromString(reference.entityId))).toEither.fold(
      _ => UseCaseIO.left(UseCaseError.Repository(RepositoryError.Unavailable)),
      UseCaseIO.pure
    )

  private def accountReference(value: (User, AccountToken)): com.example.graphQL.cats.repository.protocol.MutationEntityReference =
    userReference(value._1)

  private def userReference(user: User): com.example.graphQL.cats.repository.protocol.MutationEntityReference =
    userReference(user.id)

  private def userReference(userId: UserId): com.example.graphQL.cats.repository.protocol.MutationEntityReference =
    com.example.graphQL.cats.repository.protocol.MutationEntityReference("user", userId.value.toString)

  private def token(user: User, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
    tokenIssuer.issue(user, now)
      .handleError(_ => Left(AccessTokenIssuanceError.Unavailable))
      .map(_.leftMap(_ => UseCaseError.Availability(AvailabilityError.ServiceNotReady)).map(user -> _))

  private def validateRegistration(input: SignUpInput): ValidatedNel[DomainValidationError, Unit] =
    (validateCredentials(input.name, input.password), UserProfile.validateFor(input.role, input.profile)).mapN((_, _) => ())

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

  private def wakeCandidateAfterCommit[A](result: Either[UseCaseError, A]): IO[Unit] =
    result.fold(_ => IO.unit, _ => embeddingWork.wake.handleError(_ => ()))
}
