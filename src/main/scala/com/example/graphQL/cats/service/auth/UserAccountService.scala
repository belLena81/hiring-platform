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
    if (input.role == UserRole.Admin) IO.pure(Left(AccountError.AdminSignupForbidden: UseCaseError))
    else if (!profileShapeValid(input.role, AccountProfileInput(input.candidateProfile, input.recruiterProfile)))
      IO.pure(Left(AccountError.ProfileRoleMismatch: UseCaseError))
    else validateRegistration(input).fold(
      errors => IO.pure(Left(errors: UseCaseError)),
      _ => accounts.initialized.flatMap {
        case false => IO.pure(Left(AccountError.BootstrapRequired: UseCaseError))
        case true => hasher.hash(input.password).flatMap { hash =>
          val user = toUser(userId, input.name, input.role, input.candidateProfile, input.recruiterProfile, now)
          accounts.createAccount(user, hash).flatMap {
            case Left(RepositoryError.Conflict) => IO.pure(Left(AccountError.NameTaken: UseCaseError))
            case Left(error) => IO.pure(Left(error: UseCaseError))
            case Right(()) => token(user, now)
          }
        }
      }
    )

  override def bootstrapAdmin(input: BootstrapAdminInput, now: Instant, userId: UserId): IO[Either[UseCaseError, (User, AccountToken)]] =
    validateCredentials(input.name, input.password).fold(
      errors => IO.pure(Left(errors: UseCaseError)),
      _ => hasher.hash(input.password).flatMap { hash =>
        val user = toUser(userId, input.name, UserRole.Admin, None, None, now).copy(adminSingleton = true)
        accounts.bootstrap(user, hash).flatMap {
          case Left(RepositoryError.Conflict) => IO.pure(Left(AccountError.AlreadyBootstrapped: UseCaseError))
          case Left(error) => IO.pure(Left(error: UseCaseError))
          case Right(()) => token(user, now)
        }
      }
    )

  override def login(input: LoginInput, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] = {
    val canonical = canonicalName(input.name)
    accounts.findByCanonicalName(canonical).flatMap {
      case None => hasher.verify(dummyHash, input.password).as(Left(AccountError.InvalidCredentials: UseCaseError))
      case Some(credentials) if credentials.user.accountStatus != AccountStatus.Active =>
        hasher.verify(dummyHash, input.password).as(Left(AccountError.InvalidCredentials: UseCaseError))
      case Some(credentials) => hasher.verify(credentials.passwordHash, input.password).flatMap {
        case false => IO.pure(Left(AccountError.InvalidCredentials: UseCaseError))
        case true => token(credentials.user, now)
      }
    }
  }

  override def me(actor: ActorContext): IO[Either[UseCaseError, User]] =
    users.find(actor.userId).map(_.filter(_.accountStatus == AccountStatus.Active).toRight[UseCaseError](AuthenticationError.Unauthorized))

  override def updateMyProfile(actor: ActorContext, input: AccountProfileInput): IO[Either[UseCaseError, User]] =
    users.find(actor.userId).flatMap {
      case None => IO.pure(Left(AuthenticationError.Unauthorized: UseCaseError))
      case Some(user) if user.accountStatus != AccountStatus.Active => IO.pure(Left(AuthenticationError.Unauthorized: UseCaseError))
      case Some(user) =>
        if (!profileShapeValid(user.role, input)) IO.pure(Left(AccountError.ProfileRoleMismatch: UseCaseError))
        else validateProfile(user.role, input).fold(
          errors => IO.pure(Left(errors: UseCaseError)),
          _ => accounts.updateProfile(user.id, input.candidateProfile, input.recruiterProfile).map(_.leftMap(identity[UseCaseError]))
        )
    }

  override def deleteMyAccount(actor: ActorContext, now: Instant): IO[Either[UseCaseError, Unit]] =
    users.find(actor.userId).flatMap {
      case Some(user) if user.role == UserRole.Admin && user.adminSingleton => IO.pure(Left(AuthenticationError.SingletonAdminViolation: UseCaseError))
      case Some(user) if user.accountStatus == AccountStatus.Active =>
        accounts.deleteAccount(user.id, now, s"deleted-${user.id.value}").map(_.leftMap(identity[UseCaseError]))
      case Some(_) => IO.pure(Left(AccountError.AccountAlreadyDeleted: UseCaseError))
      case None => IO.pure(Left(AuthenticationError.Unauthorized: UseCaseError))
    }

  override def listUsers(actor: ActorContext, page: UserPageRequest): IO[Either[UseCaseError, List[User]]] =
    users.find(actor.userId).flatMap {
      case Some(user) if user.role == UserRole.Admin && user.adminSingleton && user.accountStatus == AccountStatus.Active =>
        accounts.listAccounts(page).map(_.asRight[UseCaseError])
      case _ => IO.pure(Left(AuthenticationError.Unauthorized: UseCaseError))
    }

  private def token(user: User, now: Instant): IO[Either[UseCaseError, (User, AccountToken)]] =
    IO.fromOption(JwtActorAuthenticator.issue(jwt, user.id, now))(new IllegalStateException("JWT issuance is disabled"))
      .map { case (value, expiresAt) => Right(user -> AccountToken(value, expiresAt)) }
      .handleError(_ => Left(RepositoryError.Unavailable: UseCaseError))

  private def validateRegistration(input: SignUpInput): ValidatedNel[DomainValidationError, Unit] =
    (validateName(input.name), validateCredentials(input.name, input.password), validateProfile(input.role, AccountProfileInput(input.candidateProfile, input.recruiterProfile))).mapN((_, _, _) => ())

  private def validateCredentials(name: String, password: String): ValidatedNel[DomainValidationError, Unit] =
    (validateName(name), if (password.getBytes(java.nio.charset.StandardCharsets.UTF_8).length >= 12) ().validNel else DomainValidationError.BlankField("password").invalidNel).mapN((_, _) => ())

  private def validateName(value: String): ValidatedNel[DomainValidationError, String] =
    val normalized = Normalizer.normalize(value.trim, Normalizer.Form.NFKC)
    if (normalized.isEmpty) DomainValidationError.BlankField("name").invalidNel else normalized.validNel

  private def validateProfile(role: UserRole, input: AccountProfileInput): ValidatedNel[DomainValidationError, Unit] =
    role match {
      case UserRole.Candidate => input.candidateProfile.fold(DomainValidationError.BlankField("candidateProfile").invalidNel)(profile => CandidateProfile.validate(profile.skills, profile.experienceSummary, profile.resumeRef).map(_ => ()))
      case UserRole.Recruiter => input.recruiterProfile.fold(DomainValidationError.BlankField("recruiterProfile").invalidNel)(profile => RecruiterProfile.validate(profile.organizationName, profile.jobTitle).map(_ => ()))
      case UserRole.Admin => ().validNel
    }

  private def profileShapeValid(role: UserRole, input: AccountProfileInput): Boolean =
    role match {
      case UserRole.Candidate => input.candidateProfile.nonEmpty && input.recruiterProfile.isEmpty
      case UserRole.Recruiter => input.candidateProfile.isEmpty && input.recruiterProfile.nonEmpty
      case UserRole.Admin => input.candidateProfile.isEmpty && input.recruiterProfile.isEmpty
    }

  private def toUser(id: UserId, name: String, role: UserRole, profile: Option[CandidateProfile], recruiterProfile: Option[RecruiterProfile], now: Instant): User =
    User(id, None, Normalizer.normalize(name.trim, Normalizer.Form.NFKC), role, profile, now, recruiterProfile = recruiterProfile)

  private def canonicalName(value: String): String =
    AccountName.canonical(value)
}
