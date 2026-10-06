package com.example.graphQL.cats.service.search

import cats.data.ValidatedNec
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{FieldLimits, GeoPoint}
import com.example.graphQL.cats.service.SearchError

/** Pure normalization shared by discovery requests, cursor binding and service calls. */
object JobDiscoveryValidation {
  def filter(value: JobSearchFilter): ValidatedNec[SearchError, JobSearchFilter] = {
    val city = value.city.map(_.trim)
    val skills = value.skills.map(_.trim).filter(_.nonEmpty)
    val validCity = city.forall(text => text.nonEmpty && text.length <= FieldLimits.ShortTextMaxChars)
    val validSkills = skills.size <= FieldLimits.CollectionMaxValues &&
      skills.forall(_.length <= FieldLimits.ShortTextMaxChars)
    (
      Either.cond(validCity, city, SearchError.InvalidFilter("city")).toValidatedNec,
      Either.cond(validSkills, skills, SearchError.InvalidFilter("skills")).toValidatedNec
    ).mapN((normalizedCity, normalizedSkills) => value.copy(city = normalizedCity, skills = normalizedSkills))
  }

  def validRadius(center: GeoPoint, radiusKm: Double): Boolean =
    center.isValid && radiusKm.isFinite && radiusKm > 0d && radiusKm <= NearbyJobsQuery.MaxRadiusKm

  def nearby(value: NearbyJobsQuery): ValidatedNec[SearchError, NearbyJobsQuery] =
    (
      Either
        .cond(validRadius(value.center, value.radiusKm), (), SearchError.InvalidFilter("radiusOrCenter"))
        .toValidatedNec,
      filter(value.filter)
    ).mapN((_, normalizedFilter) => value.copy(filter = normalizedFilter)).andThen { normalized =>
      normalized.after
        .traverse(cursor => NearbyJobCursorCodec.validate(cursor, normalized))
        .leftMap(_ => SearchError.InvalidFilter("cursor"))
        .toValidatedNec
        .as(normalized)
    }

  def facets(value: JobFacetQuery): ValidatedNec[SearchError, JobFacetQuery] =
    (
      Either.cond(value.radius.forall(_.isValid), (), SearchError.InvalidFilter("radius")).toValidatedNec,
      filter(value.filter)
    ).mapN((_, normalizedFilter) => value.copy(filter = normalizedFilter))
}
