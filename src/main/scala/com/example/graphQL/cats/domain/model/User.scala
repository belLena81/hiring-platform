package com.example.graphQL.cats.domain.model

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.{BlankField, EmptyCollection}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.time.Instant

enum UserRole {
  case Candidate, Recruiter, Admin
}

enum AccountStatus {
  case Active, Deleted
}

final case class CandidateProfile(
  skills: Set[String],
  experienceSummary: Option[String],
  resumeRef: Option[String]
)

object CandidateProfile {
  def validate(
      skills: Set[String],
      experienceSummary: Option[String],
      resumeRef: Option[String]
  ): ValidatedNel[DomainValidationError, CandidateProfile] =
    (
      validateNonEmptyValues("skills", skills),
      validateOptionalText("experienceSummary", experienceSummary, FieldLimits.LongTextMaxChars),
      validateOptionalText("resumeRef", resumeRef, FieldLimits.ResumeRefMaxChars)
    ).mapN(CandidateProfile.apply)
}

final case class RecruiterProfile(organizationName: String, jobTitle: Option[String])

object RecruiterProfile {
  def validate(
      organizationName: String,
      jobTitle: Option[String]
  ): ValidatedNel[DomainValidationError, RecruiterProfile] =
    (
      validateText("organizationName", organizationName),
      validateOptionalText("jobTitle", jobTitle)
    ).mapN(RecruiterProfile.apply)
}

enum UserProfile {
  case Candidate(value: CandidateProfile)
  case Recruiter(value: RecruiterProfile)
}

object UserProfile {
  def matchesRole(role: UserRole, profile: Option[UserProfile]): Boolean =
    (role, profile) match {
      case (UserRole.Candidate, Some(UserProfile.Candidate(_))) => true
      case (UserRole.Recruiter, Some(UserProfile.Recruiter(_))) => true
      case (UserRole.Admin, None) => true
      case _ => false
    }

  def validateFor(role: UserRole, profile: Option[UserProfile]): ValidatedNel[DomainValidationError, Unit] =
    (role, profile) match {
      case (UserRole.Candidate, Some(UserProfile.Candidate(value))) =>
        CandidateProfile.validate(value.skills, value.experienceSummary, value.resumeRef).void
      case (UserRole.Recruiter, Some(UserProfile.Recruiter(value))) =>
        RecruiterProfile.validate(value.organizationName, value.jobTitle).void
      case _ => DomainValidationError.BlankField("profile").invalidNel
    }
}

final case class User(
  id: UserId,
  email: Option[String],
  name: String,
  role: UserRole,
  profile: Option[UserProfile],
  createdAt: Instant,
  adminSingleton: Boolean = false,
  embedding: Option[EntityEmbedding] = None,
  accountStatus: AccountStatus = AccountStatus.Active,
  deletedAt: Option[Instant] = None
) {
  def candidateProfile: Option[CandidateProfile] =
    profile.collect { case UserProfile.Candidate(value) => value }

  def roleProfileIsValid: Boolean =
    accountStatus match {
      case AccountStatus.Deleted => profile.isEmpty
      case AccountStatus.Active =>
        role match {
          case UserRole.Admin => adminSingleton && profile.isEmpty
          case UserRole.Candidate | UserRole.Recruiter => !adminSingleton && UserProfile.matchesRole(role, profile)
        }
    }
}

object User {
  def validate(
      id: UserId,
      email: Option[String],
      name: String,
      role: UserRole,
      profile: Option[UserProfile],
      createdAt: Instant
  ): ValidatedNel[DomainValidationError, User] =
    (
      email.traverse(value => validateText("email", value)),
      validateText("name", name)
    ).mapN((validEmail, validName) => User(id, validEmail, validName, role, profile, createdAt))
}

object FieldLimits {
  val ShortTextMaxChars = 256
  val LongTextMaxChars = 8192
  val ResumeRefMaxChars = 2048
  val CollectionMaxValues = 100
  val PasswordMaxBytes = 1024
}

private[domain] def validateText(
    field: String,
    value: String,
    maximum: Int = FieldLimits.ShortTextMaxChars
): ValidatedNel[DomainValidationError, String] =
  value.trim match {
    case "" => BlankField(field).invalidNel
    case trimmed if trimmed.length > maximum => DomainValidationError.TextTooLong(field, maximum, trimmed.length).invalidNel
    case trimmed => trimmed.validNel
  }

private[domain] def validateOptionalText(
    field: String,
    value: Option[String],
    maximum: Int = FieldLimits.ShortTextMaxChars
): ValidatedNel[DomainValidationError, Option[String]] =
  value match {
    case Some(raw) => validateText(field, raw, maximum).map(Some(_))
    case None => none[String].validNel
  }

private[domain] def validateNonEmptyValues(
    field: String,
    values: Set[String]
): ValidatedNel[DomainValidationError, Set[String]] = {
  val trimmed = values.map(_.trim).filter(_.nonEmpty)
  if (trimmed.isEmpty) EmptyCollection(field).invalidNel
  else if (trimmed.size > FieldLimits.CollectionMaxValues)
    DomainValidationError.TooManyValues(field, FieldLimits.CollectionMaxValues, trimmed.size).invalidNel
  else trimmed.toList.traverse(validateText(field, _)).map(_.toSet)
}
