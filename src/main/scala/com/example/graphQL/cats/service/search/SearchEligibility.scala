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
      requested: ValidatedCandidateMatchFilters
  ): Boolean =
    current.role == UserRole.Candidate && current.accountStatus == AccountStatus.Active && current.profile.exists {
      profile =>
        val fresh = current.metadata.exists(meta =>
          meta == retrieved && meta.model == model &&
            meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
        )
        val skills = profile.skills.map(ValidatedCandidateMatchFilters.canonical)
        val publicMatch = requested.requiredSkills.forall(skills.contains)
        val privateMatch = !profile.recruiterSearchOptIn || (requested.countryCanonical
          .forall(value =>
            profile.currentResidence
              .exists(residence => ValidatedCandidateMatchFilters.canonical(residence.country) == value)
          ) &&
          requested.cityCanonical
            .forall(value =>
              profile.currentResidence
                .flatMap(_.city)
                .exists(city => ValidatedCandidateMatchFilters.canonical(city) == value)
            ) &&
          requested.availabilityStatus.forall(value => profile.availabilityStatus.contains(value)))
        fresh && publicMatch && privateMatch
    }
}
