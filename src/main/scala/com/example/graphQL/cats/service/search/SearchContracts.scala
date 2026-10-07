package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, GeoPoint, Job, SearchMode}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import java.time.Instant
import java.util.UUID

final case class JobSearchFilter(
    city: Option[String],
    skills: Set[String],
    createdAfter: Option[Instant]
)

final case class NearbyJobsQuery(
    center: GeoPoint,
    radiusKm: Double,
    filter: JobSearchFilter,
    after: Option[NearbyJobCursor] = None
) {
  def isValid: Boolean = JobDiscoveryValidation.nearby(this).isValid
  def fingerprint: String = NearbyJobCursorCodec.fingerprint(this)
}
final case class NearbyJobCursor(distanceKm: Double, jobId: JobId, queryFingerprint: String)
final case class NearbyJobsPage(query: NearbyJobsQuery, first: Int, after: Option[NearbyJobCursor])
final case class NearbyRadius(center: GeoPoint, radiusKm: Double) {
  def isValid: Boolean = JobDiscoveryValidation.validRadius(center, radiusKm)
}
final case class JobFacetQuery(filter: JobSearchFilter, radius: Option[NearbyRadius]) {
  def isValid: Boolean = JobDiscoveryValidation.facets(this).isValid
}

object NearbyJobsQuery {
  val MaxRadiusKm: Double = 500d

  def encodeCursor(distanceKm: Double, jobId: JobId, query: NearbyJobsQuery): String =
    NearbyJobCursorCodec.encode(distanceKm, jobId, query)

  def decodeCursor(value: String, query: NearbyJobsQuery): Either[NearbyCursorError, NearbyJobCursor] =
    NearbyJobCursorCodec.decode(value, query)
}

final case class NearbyJob(job: Job, distanceKm: Double)
final case class JobFacetBucket(value: String, count: Long)
final case class JobDiscoveryFacets(
    skills: List[JobFacetBucket],
    countries: List[JobFacetBucket],
    cities: List[JobFacetBucket],
    remote: List[JobFacetBucket],
    truncated: Boolean
)

object JobDiscoveryFacets {
  val MaxBucketsPerDimension: Int = 20

  def fromJobs(jobs: List[Job]): JobDiscoveryFacets = {
    def buckets(values: Iterable[String]): List[JobFacetBucket] =
      values.toList
        .groupMapReduce(identity)(_ => 1L)(_ + _)
        .toList
        .sortBy { case (value, count) => (-count, value) }
        .map { case (value, count) => JobFacetBucket(value, count) }
    val skillBuckets = buckets(jobs.flatMap(_.skills))
    val countryBuckets = buckets(jobs.map(_.location.country).filter(_.nonEmpty))
    val cityBuckets = buckets(jobs.map(_.location.city).filter(_.nonEmpty))
    val remoteBuckets = buckets(jobs.map(job => job.location.remote.toString))
    JobDiscoveryFacets(
      skillBuckets.take(MaxBucketsPerDimension),
      countryBuckets.take(MaxBucketsPerDimension),
      cityBuckets.take(MaxBucketsPerDimension),
      remoteBuckets.take(MaxBucketsPerDimension),
      List(skillBuckets, countryBuckets, cityBuckets, remoteBuckets).exists(_.size > MaxBucketsPerDimension)
    )
  }
}

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
    candidateFilters: ValidatedCandidateMatchFilters = ValidatedCandidateMatchFilters.empty
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

/** Retrieval supplies indexed evidence; public entities are hydrated once from authorized operational truth. */
final case class SearchRetrievalHit[Id](
    id: Id,
    score: Double,
    mode: SearchMode,
    meta: EmbeddingMeta,
    searchId: UUID,
    retrievalScore: Option[Double] = None
)
type JobRetrievalHit = SearchRetrievalHit[JobId]
type CandidateRetrievalHit = SearchRetrievalHit[UserId]

final case class RankedJob(
    job: Job,
    score: Double,
    mode: SearchMode,
    meta: EmbeddingMeta,
    searchId: UUID,
    matchedSkills: List[String] = Nil,
    retrievalScore: Option[Double] = None
) {
  def retrieval: JobRetrievalHit = SearchRetrievalHit(job.id, score, mode, meta, searchId, retrievalScore)
}
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
) {
  def retrieval: CandidateRetrievalHit = SearchRetrievalHit(candidate.id, score, mode, meta, searchId, retrievalScore)
}

object SkillMatching {
  def matched(candidateSkills: Set[String], targetSkills: Set[String]): List[String] = {
    val candidateCanonical = candidateSkills.map(_.trim.toLowerCase(java.util.Locale.ROOT))
    targetSkills.toList
      .filter(skill => candidateCanonical.contains(skill.trim.toLowerCase(java.util.Locale.ROOT)))
      .sortBy(skill => (skill.trim.toLowerCase(java.util.Locale.ROOT), skill))
  }
}
