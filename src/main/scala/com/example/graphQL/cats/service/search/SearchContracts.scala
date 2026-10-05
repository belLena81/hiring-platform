package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, Job, SearchMode}
import com.example.graphQL.cats.domain.pagination.PageSize
import java.time.Instant
import java.util.UUID

final case class JobSearchFilter(
    city: Option[String],
    skills: Set[String],
    createdAfter: Option[Instant]
)

final case class VectorSearchQuery(
    vector: List[Float],
    lexicalQuery: Option[String],
    filter: JobSearchFilter,
    /** Requested response page; repositories may return more hits up to their configured branch-result cap. */
    first: PageSize,
    mode: SearchMode,
    model: String,
    searchId: UUID,
    candidateQueryVector: Option[List[Float]] = None,
    candidateFilters: CandidateMatchFilters = CandidateMatchFilters.empty
)

final case class CandidateMatchFilters(
    requiredSkills: List[String],
    countryCanonical: Option[String],
    cityCanonical: Option[String],
    availabilityStatus: Option[String]
)

object CandidateMatchFilters {
  val empty: CandidateMatchFilters = CandidateMatchFilters(Nil, None, None, None)
}

final case class RankedJob(
    job: Job,
    score: Double,
    mode: SearchMode,
    meta: EmbeddingMeta,
    searchId: UUID,
    matchedSkills: List[String] = Nil,
    retrievalScore: Option[Double] = None
)
final case class CandidateSearchHit(
    id: com.example.graphQL.cats.domain.model.Identifiers.UserId,
    name: String,
    skills: Set[String],
    experienceSummary: Option[String]
)

final case class RankedCandidate(
    candidate: CandidateSearchHit,
    score: Double,
    mode: SearchMode,
    meta: EmbeddingMeta,
    searchId: UUID,
    matchedSkills: List[String] = Nil,
    retrievalScore: Option[Double] = None
)

object SkillMatching {
  def matched(candidateSkills: Set[String], targetSkills: Set[String]): List[String] = {
    val candidateCanonical = candidateSkills.map(_.trim.toLowerCase(java.util.Locale.ROOT))
    targetSkills.toList
      .filter(skill => candidateCanonical.contains(skill.trim.toLowerCase(java.util.Locale.ROOT)))
      .sortBy(skill => (skill.trim.toLowerCase(java.util.Locale.ROOT), skill))
  }
}
