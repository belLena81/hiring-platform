package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.shared.crypto.SourceHash

/** Authoritative selection data deliberately excludes embedding vectors and candidate resume/email. */
final case class JobSearchEligibility(job: Job, metadata: Option[EmbeddingMeta])
object JobSearchEligibility {
  def fromJob(job: Job): JobSearchEligibility =
    JobSearchEligibility(job.copy(embedding = None), job.embedding.map(_.meta))
}
final case class CandidateSearchEligibility(
    id: UserId,
    role: UserRole,
    accountStatus: AccountStatus,
    profile: Option[CandidateProfile],
    metadata: Option[EmbeddingMeta]
)
object CandidateSearchEligibility {
  def fromUser(user: User): CandidateSearchEligibility =
    CandidateSearchEligibility(
      user.id,
      user.role,
      user.accountStatus,
      user.candidateProfile.map(_.copy(resumeRef = None)),
      user.embedding.map(_.meta)
    )
}

/** Parsed once at the input boundary; eligibility decisions use the closed availability enum. */
final case class CandidateEligibilityFilters(
    filters: CandidateMatchFilters,
    availability: Option[CandidateAvailabilityStatus]
)

object SearchEligibilityPolicy {
  def job(current: JobSearchEligibility, retrieved: EmbeddingMeta, model: String, filter: JobSearchFilter): Boolean =
    current.job.status == JobStatus.Open && current.metadata.exists(meta =>
      meta == retrieved && meta.model == model && meta.sourceHash == SourceHash.sha256(SearchableText.job(current.job))
    ) && filter.city.forall(_ == current.job.location.city) && filter.skills.subsetOf(current.job.skills) &&
      filter.createdAfter.forall(after => !current.job.createdAt.isBefore(after))

  def candidate(
      current: CandidateSearchEligibility,
      retrieved: EmbeddingMeta,
      model: String,
      requested: CandidateEligibilityFilters
  ): Boolean =
    current.role == UserRole.Candidate && current.accountStatus == AccountStatus.Active && current.profile.exists {
      profile =>
        val fresh = current.metadata.exists(meta =>
          meta == retrieved && meta.model == model &&
            meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
        )
        val publicMatch = requested.filters.requiredSkills.forall(required =>
          profile.skills.exists(_.trim.equalsIgnoreCase(required.trim))
        )
        val privateMatch = !profile.recruiterSearchOptIn || (requested.filters.countryCanonical
          .forall(value => profile.currentResidence.exists(_.country.trim.equalsIgnoreCase(value.trim))) &&
          requested.filters.cityCanonical
            .forall(value => profile.currentResidence.flatMap(_.city).exists(_.trim.equalsIgnoreCase(value.trim))) &&
          requested.availability.forall(value => profile.availabilityStatus.contains(value)))
        fresh && publicMatch && privateMatch
    }
}
