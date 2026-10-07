package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{AccountStatus, UserRole}
import java.time.Instant

/** One pure eligibility decision rendered independently for operational and indexed selection. */
enum SearchEligibilityField {
  case JobStatus, EmbeddingModel, JobCity, JobSkill, JobCreatedAt
  case CandidateRole, CandidateAccountStatus, CandidateSkill, SearchConsent
  case ResidenceCountry, ResidenceCity, Availability
}

enum SearchEligibilityValue {
  case Text(value: String)
  case Flag(value: Boolean)
}

enum SearchEligibilityCriteria {
  case Equal(field: SearchEligibilityField, value: SearchEligibilityValue)
  case AtLeast(field: SearchEligibilityField, value: Instant)
  case Missing(field: SearchEligibilityField)
  case All(criteria: List[SearchEligibilityCriteria])
  case AnyOf(criteria: List[SearchEligibilityCriteria])
}

object SearchEligibilityCriteria {
  import SearchEligibilityField.*
  import SearchEligibilityValue.*

  def jobs(model: String, filter: JobSearchFilter): SearchEligibilityCriteria = All(
    List(
      Equal(JobStatus, Text(com.example.graphQL.cats.domain.model.JobStatus.Open.toString)),
      Equal(EmbeddingModel, Text(model))
    ) ++
      filter.city.toList.map(city => Equal(JobCity, Text(city))) ++
      filter.skills.toList.sorted.map(skill => Equal(JobSkill, Text(skill))) ++
      filter.createdAfter.toList.map(AtLeast(JobCreatedAt, _))
  )

  def candidates(query: VectorSearchQuery, includeEmbeddingModel: Boolean): SearchEligibilityCriteria = {
    val filters = query.candidateFilters
    val requested = filters.countryCanonical.toList.map(value => Equal(ResidenceCountry, Text(value))) ++
      filters.cityCanonical.toList.map(value => Equal(ResidenceCity, Text(value))) ++
      filters.availabilityStatus.toList.map(value => Equal(Availability, Text(value.toString)))
    val privacy = Option
      .when(requested.nonEmpty)(
        AnyOf(
          List(
            Equal(SearchConsent, Flag(false)),
            Missing(SearchConsent),
            All(Equal(SearchConsent, Flag(true)) :: requested)
          )
        )
      )
      .toList
    All(
      List(
        Equal(CandidateRole, Text(UserRole.Candidate.toString)),
        Equal(CandidateAccountStatus, Text(AccountStatus.Active.toString))
      ) ++
        Option.when(includeEmbeddingModel)(Equal(EmbeddingModel, Text(query.model))).toList ++
        filters.requiredSkills.map(skill => Equal(CandidateSkill, Text(skill))) ++ privacy
    )
  }
}
