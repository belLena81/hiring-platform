package com.example.graphQL.cats.domain.model

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.EmptyCollection
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import java.time.Instant

enum JobStatus {
  case Draft, Open, Closed
}

final case class Location(country: String, city: String, remote: Boolean)

object Location {
  def validate(country: String, city: String, remote: Boolean): ValidatedNel[DomainValidationError, Location] =
    (
      validateText("country", country),
      validateText("city", city)
    ).mapN(Location(_, _, remote))
}

final case class Job(
  id: JobId,
  recruiterId: UserId,
  title: String,
  description: String,
  requirements: List[String],
  skills: Set[String],
  location: Location,
  status: JobStatus,
  createdAt: Instant,
  updatedAt: Instant
)

object Job {
  def validate(
      id: JobId,
      recruiterId: UserId,
      title: String,
      description: String,
      requirements: List[String],
      skills: Set[String],
      location: Location,
      status: JobStatus,
      createdAt: Instant,
      updatedAt: Instant
  ): ValidatedNel[DomainValidationError, Job] =
    (
      validateText("title", title),
      validateText("description", description),
      validateRequirements(requirements),
      validateNonEmptyValues("skills", skills)
    ).mapN { (validTitle, validDescription, validRequirements, validSkills) =>
      Job(id, recruiterId, validTitle, validDescription, validRequirements, validSkills, location, status, createdAt,
        updatedAt)
    }

  private def validateRequirements(requirements: List[String]): ValidatedNel[DomainValidationError, List[String]] = {
    val trimmed = requirements.map(_.trim).filter(_.nonEmpty)
    if (trimmed.isEmpty) EmptyCollection("requirements").invalidNel else trimmed.validNel
  }
}
