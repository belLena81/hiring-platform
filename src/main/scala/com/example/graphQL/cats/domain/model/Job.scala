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

/** The state of a [[Job]]. A closed job always carries the instant it was closed, so the pair (status, closedAt) cannot
  * disagree; [[JobStatus]] is its label (what a client filters, an event reports and a store compares).
  */
enum JobState {
  case Draft
  case Open
  case Closed(at: Instant)

  def status: JobStatus = this match {
    case Draft     => JobStatus.Draft
    case Open      => JobStatus.Open
    case Closed(_) => JobStatus.Closed
  }

  def closedAt: Option[Instant] = this match {
    case Closed(at)   => Some(at)
    case Draft | Open => None
  }
}

object JobState {

  /** The state a job created with the requested status has at `now`; a closed request is stamped with `now`. */
  def requested(status: JobStatus, now: Instant): JobState = status match {
    case JobStatus.Draft  => JobState.Draft
    case JobStatus.Open   => JobState.Open
    case JobStatus.Closed => JobState.Closed(now)
  }
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

final case class Location(country: String, city: Option[String], remote: Boolean, coordinates: Option[GeoPoint] = None)

object Location {
  def validate(
      country: String,
      city: Option[String],
      remote: Boolean,
      coordinates: Option[GeoPoint] = None
  ): ValidatedNel[DomainValidationError, Location] =
    (
      validateText("country", country),
      city.traverse(validateText("city", _)),
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
    state: JobState,
    createdAt: Instant,
    updatedAt: Instant,
    embedding: Option[EntityEmbedding] = None
) {
  def status: JobStatus = state.status

  def closedAt: Option[Instant] = state.closedAt
}

object Job {
  def validate(
      id: JobId,
      recruiterId: UserId,
      title: String,
      description: String,
      requirements: List[String],
      skills: Set[String],
      location: Location,
      state: JobState,
      createdAt: Instant,
      updatedAt: Instant,
      embedding: Option[EntityEmbedding] = None
  ): ValidatedNel[DomainValidationError, Job] =
    (
      validateText("title", title),
      validateText("description", description, FieldLimits.LongTextMaxChars),
      validateRequirements(requirements),
      validateNonEmptyValues("skills", skills),
      Location.validate(location.country, location.city, location.remote, location.coordinates)
    ).mapN { (validTitle, validDescription, validRequirements, validSkills, validLocation) =>
      Job(
        id,
        recruiterId,
        validTitle,
        validDescription,
        validRequirements,
        validSkills,
        validLocation,
        state,
        createdAt,
        updatedAt,
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
