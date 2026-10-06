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

final case class GeoPoint(latitude: Double, longitude: Double) {
  def isValid: Boolean = latitude.isFinite && longitude.isFinite && latitude >= -90d && latitude <= 90d &&
    longitude >= -180d && longitude <= 180d
}

object GeoPoint {
  def validate(latitude: Double, longitude: Double): Either[DomainValidationError, GeoPoint] =
    Either.cond(
      GeoPoint(latitude, longitude).isValid,
      GeoPoint(latitude, longitude),
      DomainValidationError.InvalidCoordinates
    )
}

final case class Location(country: String, city: String, remote: Boolean, coordinates: Option[GeoPoint] = None)

object Location {
  def validate(
      country: String,
      city: String,
      remote: Boolean,
      coordinates: Option[GeoPoint] = None
  ): ValidatedNel[DomainValidationError, Location] =
    (
      validateText("country", country),
      validateText("city", city),
      coordinates.traverse(point =>
        Either.cond(point.isValid, point, DomainValidationError.InvalidCoordinates).toValidatedNel
      )
    ).mapN((validCountry, validCity, validCoordinates) => Location(validCountry, validCity, remote, validCoordinates))
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
    updatedAt: Instant,
    closedAt: Option[Instant] = None,
    embedding: Option[EntityEmbedding] = None
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
      updatedAt: Instant,
      closedAt: Option[Instant] = None,
      embedding: Option[EntityEmbedding] = None
  ): ValidatedNel[DomainValidationError, Job] =
    (
      validateText("title", title),
      validateText("description", description, FieldLimits.LongTextMaxChars),
      validateRequirements(requirements),
      validateNonEmptyValues("skills", skills),
      Location.validate(location.country, location.city, location.remote)
    ).mapN { (validTitle, validDescription, validRequirements, validSkills, validLocation) =>
      Job(
        id,
        recruiterId,
        validTitle,
        validDescription,
        validRequirements,
        validSkills,
        validLocation,
        status,
        createdAt,
        updatedAt,
        closedAt,
        embedding
      )
    }

  private def validateRequirements(requirements: List[String]): ValidatedNel[DomainValidationError, List[String]] = {
    val trimmed = requirements.map(_.trim).filter(_.nonEmpty)
    if (trimmed.isEmpty) EmptyCollection("requirements").invalidNel
    else if (trimmed.size > FieldLimits.CollectionMaxValues)
      DomainValidationError.TooManyValues("requirements", FieldLimits.CollectionMaxValues, trimmed.size).invalidNel
    else trimmed.traverse(validateText("requirements", _))
  }
}
