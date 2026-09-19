package com.example.graphQL.cats.service.auth

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.config.JwtAuthConfig
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
    jwt: JwtAuthConfig
) extends AccountUseCases[IO] {
  private val dummyHash = "$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

  override def signUp(input: SignUpInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
    if (input.role == UserRole.Admin) IO.pure(Left(UseCaseError.account(AccountError.AdminSignupForbidden)))
    else if (!profileShapeValid(input.role, input.profile))
      IO.pure(Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
    else validateRegistration(input).fold(
      errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
      _ => accounts.initialized.flatMap {
        case false => IO.pure(Left(UseCaseError.account(AccountError.BootstrapRequired)))
        case true => hasher.hash(input.password).flatMap { hash =>
          val user = toUser(userId, input.name, input.role, input.profile, now)
          token(user, now).flatMap {
            case Left(error) => IO.pure(Left(error))
            case Right((_, accountToken)) => accounts.createAccount(user, hash).map {
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
      case None => hasher.verify(dummyHash, input.password).as(Left(UseCaseError.account(AccountError.InvalidCredentials)))
      case Some(credentials) if credentials.user.accountStatus != AccountStatus.Active =>
        hasher.verify(dummyHash, input.password).as(Left(UseCaseError.account(AccountError.InvalidCredentials)))
      case Some(credentials) => hasher.verify(credentials.passwordHash, input.password).flatMap {
        case false => IO.pure(Left(UseCaseError.account(AccountError.InvalidCredentials)))
        case true => token(credentials.user, now)
      }
    }
  }

  override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
    users.find(actor.userId).map(_.filter(_.accountStatus == AccountStatus.Active).toRight(UseCaseError.authentication(AuthenticationError.Unauthorized)))

  override def updateMyProfile(actor: ActorContext, input: AccountProfileInput): IO[Either[UseCaseError, User]] =
    users.find(actor.userId).flatMap {
      case None => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
      case Some(user) if user.accountStatus != AccountStatus.Active => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
      case Some(user) if user.role == UserRole.Admin => IO.pure(Left(UseCaseError.account(AccountError.ProfileUnsupportedForRole)))
      case Some(user) =>
        if (!profileShapeValid(user.role, Some(input.profile))) IO.pure(Left(UseCaseError.account(AccountError.ProfileRoleMismatch)))
        else validateProfile(user.role, Some(input.profile)).fold(
          errors => IO.pure(Left(UseCaseError.ValidationFailed(errors))),
          _ => accounts.updateProfile(user.id, input.profile).map(_.leftMap(UseCaseError.repository))
        )
    }

  override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] =
    users.find(actor.userId).flatMap {
      case Some(user) if user.role == UserRole.Admin && user.adminSingleton => IO.pure(Left(UseCaseError.authentication(AuthenticationError.SingletonAdminViolation)))
      case Some(user) if user.accountStatus == AccountStatus.Active =>
        accounts.deleteAccount(user.id, now, s"deleted-${user.id.value}").map(_.leftMap(UseCaseError.repository))
      case Some(_) => IO.pure(Right(()))
      case None => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] =
    users.find(actor.userId).flatMap {
      case Some(user) if user.role == UserRole.Admin && user.adminSingleton && user.accountStatus == AccountStatus.Active =>
        accounts.listAccounts(page).map(_.asRight[UseCaseError])
      case _ => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  private def token(user: User, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
    IO(JwtActorAuthenticator.issue(jwt, user.id, now))
      .map { case (value, expiresAt) => Right(user -> AccountToken(value, expiresAt)) }
      .handleError(_ => Left(UseCaseError.repository(RepositoryError.Unavailable)))

  private def validateRegistration(input: SignUpInput): ValidatedNel[DomainValidationError, Unit] =
    (validateName(input.name), validateCredentials(input.name, input.password), validateProfile(input.role, input.profile)).mapN((_, _, _) => ())

  private def validateCredentials(name: String, password: String): ValidatedNel[DomainValidationError, Unit] =
    (validateName(name), if (password.getBytes(java.nio.charset.StandardCharsets.UTF_8).length >= 12) ().validNel else DomainValidationError.BlankField("password").invalidNel).mapN((_, _) => ())

  private def validateName(value: String): ValidatedNel[DomainValidationError, String] =
    val normalized = Normalizer.normalize(value.trim, Normalizer.Form.NFKC)
    if (normalized.isEmpty) DomainValidationError.BlankField("name").invalidNel else normalized.validNel

  private def validateProfile(role: UserRole, profile: Option[UserProfile]): ValidatedNel[DomainValidationError, Unit] =
    (role, profile) match {
      case (UserRole.Candidate, Some(UserProfile.Candidate(profile))) => CandidateProfile.validate(profile.skills, profile.experienceSummary, profile.resumeRef).map(_ => ())
      case (UserRole.Recruiter, Some(UserProfile.Recruiter(profile))) => RecruiterProfile.validate(profile.organizationName, profile.jobTitle).map(_ => ())
      case _ => DomainValidationError.BlankField("profile").invalidNel
    }

  private def profileShapeValid(role: UserRole, profile: Option[UserProfile]): Boolean =
    (role, profile) match {
      case (UserRole.Candidate, Some(UserProfile.Candidate(_))) => true
      case (UserRole.Recruiter, Some(UserProfile.Recruiter(_))) => true
      case (UserRole.Admin, None) => true
      case _ => false
    }

  private def toUser(id: UserId, name: String, role: UserRole, profile: Option[UserProfile], now: Instant): User =
    User(id, None, Normalizer.normalize(name.trim, Normalizer.Form.NFKC), role, profile, now)

  private def canonicalName(value: String): String =
    AccountName.canonical(value)
}
