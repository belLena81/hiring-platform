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

final case class User(
  id: UserId,
  email: String,
  name: String,
  role: UserRole,
  profile: Option[CandidateProfile],
  createdAt: Instant,
  adminSingleton: Boolean = false,
  embedding: Option[EntityEmbedding] = None
)

object User {
  def validate(
      id: UserId,
      email: String,
      name: String,
      role: UserRole,
      profile: Option[CandidateProfile],
      createdAt: Instant
  ): ValidatedNel[DomainValidationError, User] =
    (
      validateText("email", email),
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
