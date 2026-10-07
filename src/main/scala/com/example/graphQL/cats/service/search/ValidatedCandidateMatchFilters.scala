package com.example.graphQL.cats.service.search

import cats.data.ValidatedNec
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{CandidateAvailabilityStatus, FieldLimits}
import com.example.graphQL.cats.service.SearchError
import java.util.Locale

/** Canonical criteria shared by retrieval and authoritative eligibility. Construction validates raw requests. */
final class ValidatedCandidateMatchFilters private (
    val requiredSkills: List[String],
    val countryCanonical: Option[String],
    val cityCanonical: Option[String],
    val availabilityStatus: Option[CandidateAvailabilityStatus]
) {
  private def values = (requiredSkills, countryCanonical, cityCanonical, availabilityStatus)

  override def equals(other: Any): Boolean = other match {
    case filters: ValidatedCandidateMatchFilters => values == filters.values
    case _                                       => false
  }

  override def hashCode(): Int = values.hashCode()
}

object ValidatedCandidateMatchFilters {
  val empty: ValidatedCandidateMatchFilters = new ValidatedCandidateMatchFilters(Nil, None, None, None)

  private[search] def canonical(value: String): String = value.trim.toLowerCase(Locale.ROOT)

  def from(value: CandidateMatchFilters): ValidatedNec[SearchError, ValidatedCandidateMatchFilters] = {
    val skills = value.requiredSkills.map(_.trim)
    val country = value.countryCanonical.map(canonical)
    val city = value.cityCanonical.map(canonical)
    val validSkills = value.requiredSkills.size <= FieldLimits.CollectionMaxValues &&
      skills.forall(skill => skill.nonEmpty && skill.length <= FieldLimits.ShortTextMaxChars)
    val validResidence = List(country, city).flatten.forall(text =>
      text.nonEmpty && text.length <= FieldLimits.ShortTextMaxChars
    ) && (city.isEmpty || country.nonEmpty)
    val availability = value.availabilityStatus.traverse(text =>
      CandidateAvailabilityStatus.values
        .find(_.toString == text)
        .toValidNec(SearchError.InvalidFilter("availabilityStatus"))
    )
    (
      Either
        .cond(validSkills, skills.map(canonical).distinct.sorted, SearchError.InvalidFilter("requiredSkills"))
        .toValidatedNec,
      Either.cond(validResidence, (country, city), SearchError.InvalidFilter("residence")).toValidatedNec,
      availability
    ).mapN { case (normalizedSkills, (normalizedCountry, normalizedCity), normalizedAvailability) =>
      new ValidatedCandidateMatchFilters(normalizedSkills, normalizedCountry, normalizedCity, normalizedAvailability)
    }
  }
}
