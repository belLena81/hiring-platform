package com.example.graphQL.cats.service.auth

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.{UserAccountRepository, UserRepository}
import com.example.graphQL.cats.service.*
import com.example.graphQL.cats.service.protocol.*
import java.text.Normalizer
import java.time.Instant

final class UserAccountService(
    users: UserRepository[IO],
    accounts: UserAccountRepository[IO],
    hasher: PasswordHasher[IO],
    tokenIssuer: AccessTokenIssuer[IO]
) extends AccountUseCases[IO] {
  private val authorization = ActorAuthorization(users)

  override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
    if (input.role == UserRole.Admin) IO.pure(Left(UseCaseError.account(AccountError.AdminSignupForbidden)))
    else if (!UserProfile.matchesRole(input.role, input.profile))
      IO.pure(Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
    else validateRegistration(input).fold(
      errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
      _ => accounts.initialized.flatMap {
        case false => IO.pure(Left(UseCaseError.account(AccountError.BootstrapRequired)))
        case true => hasher.hash(input.password).flatMap { hash =>
          val user = toUser(userId, input.name, input.role, input.profile, now)
          token(user, now).flatMap {
            case Left(error) => IO.pure(Left(error))
            case Right((_, accountToken)) => accounts.createAccount(user, hash, now).map {
              case Left(RepositoryError.Conflict) => Left(UseCaseError.account(AccountError.NameTaken))
              case Left(error) => Left(UseCaseError.repository(error))
              case Right(()) => Right(user -> accountToken)
            }
          }
        }
      }
    )

  override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
    validateCredentials(input.name, input.password).fold(
      errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
      _ => hasher.hash(input.password).flatMap { hash =>
        val user = toUser(userId, input.name, UserRole.Admin, None, now).copy(adminSingleton = true)
        token(user, now).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right((_, accountToken)) => accounts.bootstrap(user, hash).map {
            case Left(RepositoryError.Conflict) => Left(UseCaseError.account(AccountError.AlreadyBootstrapped))
            case Left(error) => Left(UseCaseError.repository(error))
            case Right(()) => Right(user -> accountToken)
          }
        }
      }
    )

  override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] = {
    val canonical = canonicalName(input.name)
    accounts.findByCanonicalName(canonical).flatMap {
      case None => hasher.verifyUnknown(input.password).as(Left(UseCaseError.account(AccountError.InvalidCredentials)))
      case Some(credentials) if credentials.user.accountStatus != AccountStatus.Active =>
        hasher.verifyUnknown(input.password).as(Left(UseCaseError.account(AccountError.InvalidCredentials)))
      case Some(credentials) => hasher.verify(credentials.passwordHash, input.password).flatMap {
        case false => IO.pure(Left(UseCaseError.account(AccountError.InvalidCredentials)))
        case true => token(credentials.user, now)
      }
    }
  }

  override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
    authorization.resolve(actor)

  override def updateMyProfile(actor: ActorContext, input: AccountProfileInput, now: Instant): IO[Either[UseCaseError, User]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(user) if user.role == UserRole.Admin => IO.pure(Left(UseCaseError.account(AccountError.ProfileUnsupportedForRole)))
      case Right(user) =>
        if (!UserProfile.matchesRole(user.role, Some(input.profile))) IO.pure(Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
        else UserProfile.validateFor(user.role, Some(input.profile)).fold(
          errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
          _ => accounts.updateProfile(user.id, input.profile, now).map(_.leftMap(UseCaseError.repository))
        )
    }

  override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] =
    authorization.resolve(actor, allowDeleted = true).flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(user) if user.role == UserRole.Admin => IO.pure(Left(UseCaseError.authentication(AuthenticationError.SingletonAdminViolation)))
      case Right(user) if user.accountStatus == AccountStatus.Deleted => IO.pure(Right(()))
      case Right(user) =>
        accounts.deleteAccount(user.id, now, s"deleted-${user.id.value}").map(_.leftMap(UseCaseError.repository))
    }

  override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] =
    authorization.resolve(actor).flatMap {
      case Right(user) if user.role == UserRole.Admin =>
        accounts.listAccounts(page).map(_.asRight[UseCaseError])
      case _ => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  private def token(user: User, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
    tokenIssuer.issue(user, now)
      .handleError(_ => Left(AccessTokenIssuanceError.Unavailable))
      .map(_.leftMap(_ => UseCaseError.availability(AvailabilityError.ServiceNotReady)).map(user -> _))

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
}
