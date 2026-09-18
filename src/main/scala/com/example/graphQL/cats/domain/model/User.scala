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
      validateOptionalText("experienceSummary", experienceSummary),
      validateOptionalText("resumeRef", resumeRef)
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
  deletedAt: Option[Instant] = None,
  version: Long = 0L
) {
  def candidateProfile: Option[CandidateProfile] =
    profile.collect { case UserProfile.Candidate(value) => value }

  def roleProfileIsValid: Boolean =
    accountStatus match {
      case AccountStatus.Deleted => profile.isEmpty
      case AccountStatus.Active =>
        role match {
          case UserRole.Admin => adminSingleton && profile.isEmpty
          case UserRole.Candidate => !adminSingleton && profile.exists(_.isInstanceOf[UserProfile.Candidate])
          case UserRole.Recruiter => !adminSingleton && profile.exists(_.isInstanceOf[UserProfile.Recruiter])
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

private[domain] def validateText(field: String, value: String): ValidatedNel[DomainValidationError, String] =
  value.trim match {
    case "" => BlankField(field).invalidNel
    case trimmed => trimmed.validNel
  }

private[domain] def validateOptionalText(
    field: String,
    value: Option[String]
): ValidatedNel[DomainValidationError, Option[String]] =
  value match {
    case Some(raw) => validateText(field, raw).map(Some(_))
    case None => none[String].validNel
  }

private[domain] def validateNonEmptyValues(
    field: String,
    values: Set[String]
): ValidatedNel[DomainValidationError, Set[String]] = {
  val trimmed = values.map(_.trim).filter(_.nonEmpty)
  if (trimmed.isEmpty) EmptyCollection(field).invalidNel else trimmed.validNel
}
