package com.example.graphQL.cats.service.search

import cats.data.{Validated, ValidatedNec}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{FieldLimits, GeoPoint, SearchableText}
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
        .traverse(cursor =>
          Either
            .cond(cursor.hasValidDistance && cursor.isBoundTo(normalized), cursor, SearchError.InvalidFilter("cursor"))
        )
        .toValidatedNec
        .as(normalized)
    }

  /** Lexical query text is trimmed; blank is invalid (never absent) and the length is bounded before any I/O. */
  def queryText(value: Option[String]): ValidatedNec[SearchError, Option[String]] =
    value.traverse[[A] =>> ValidatedNec[SearchError, A], String] { raw =>
      val text = raw.trim
      if (text.isEmpty) Validated.invalidNec(SearchError.InvalidFilter("query"))
      else if (text.length > SearchableText.QueryMaxChars)
        Validated.invalidNec(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars))
      else Validated.validNec(text)
    }

  def facets(value: JobFacetQuery): ValidatedNec[SearchError, JobFacetQuery] =
    (
      Either.cond(value.radius.forall(_.isValid), (), SearchError.InvalidFilter("radius")).toValidatedNec,
      filter(value.filter),
      queryText(value.text)
    ).mapN((_, normalizedFilter, text) => value.copy(filter = normalizedFilter, text = text))
}
