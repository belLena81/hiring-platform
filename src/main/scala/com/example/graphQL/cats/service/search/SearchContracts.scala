package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, GeoPoint, Job, SearchMode}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.shared.crypto.CriteriaDigest
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
  def fingerprint: String = NearbyJobCursor.fingerprint(this)
}
final case class NearbyJobCursor(distanceKm: Double, jobId: JobId, queryFingerprint: String) {
  def hasValidDistance: Boolean = distanceKm.isFinite && distanceKm >= 0d
  def isBoundTo(query: NearbyJobsQuery): Boolean = queryFingerprint == query.fingerprint
}

object NearbyJobCursor {

  /** Canonical digest of the criteria and ordering a nearby cursor is bound to. */
  private[search] def fingerprint(query: NearbyJobsQuery): String =
    CriteriaDigest.sha256(
      query.center.latitude,
      query.center.longitude,
      query.radiusKm,
      query.filter.city.map(_.trim),
      query.filter.skills.toList.map(_.trim).filter(_.nonEmpty).distinct.sorted,
      query.filter.createdAfter.map(_.toString)
    )
}
final case class NearbyRadius(center: GeoPoint, radiusKm: Double) {
  def isValid: Boolean = JobDiscoveryValidation.validRadius(center, radiusKm)
}

/** Facet request; `text` is the optional lexical query whose top matches restrict the counted jobs. */
final case class JobFacetQuery(filter: JobSearchFilter, radius: Option[NearbyRadius], text: Option[String] = None) {
  def isValid: Boolean = JobDiscoveryValidation.facets(this).isValid
}

/** Which matches the counts cover: every eligible match, or only the top `limit` lexical matches (never a total). */
enum FacetCoverage {
  case AllMatches
  case TopMatches(limit: Int)
}

/** A lexical hit as projected by the index: job identity and its relevance score. */
final case class ScoredJobId(id: JobId, score: Double)

/** Job IDs matched by a lexical query, with the coverage of the match set. IDs are never returned to callers. */
final case class LexicalJobMatches(ids: Set[JobId], coverage: FacetCoverage)

/** Validated lexical match limit (1..5000); the Mongo adapter reads limit + 1 hits to detect truncation. */
opaque type LexicalMatchLimit = Int
object LexicalMatchLimit {
  val Min: Int = 1
  val Max: Int = 5000
  val Default: LexicalMatchLimit = 1000
  def from(value: Int): Option[LexicalMatchLimit] = Option.when(value >= Min && value <= Max)(value)
  extension (limit: LexicalMatchLimit) def value: Int = limit
}

object LexicalJobMatches {

  /** Reduces a window of at most `limit + 1` hits. Ordered by score descending then job ID ascending (UUID text), the
    * last element is dropped when the window overflows, so the (limit + 1)th match is never counted.
    */
  def fromWindow(window: List[ScoredJobId], limit: Int): LexicalJobMatches = {
    val ordered = window.distinctBy(_.id).sortBy(hit => (-hit.score, hit.id.value.toString))
    if (ordered.size > limit) LexicalJobMatches(ordered.take(limit).map(_.id).toSet, FacetCoverage.TopMatches(limit))
    else LexicalJobMatches(ordered.map(_.id).toSet, FacetCoverage.AllMatches)
  }
}

object NearbyJobsQuery {
  val MaxRadiusKm: Double = 500d
}

final case class NearbyJob(job: Job, distanceKm: Double)
final case class JobFacetBucket(value: String, count: Long)
final case class JobDiscoveryFacets(
    skills: List[JobFacetBucket],
    countries: List[JobFacetBucket],
    cities: List[JobFacetBucket],
    remote: List[JobFacetBucket],
    truncated: Boolean,
    coverage: FacetCoverage = FacetCoverage.AllMatches
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
    val cityBuckets = buckets(jobs.flatMap(_.location.city).filter(_.nonEmpty))
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

/** Lexical-only retrieval: text relevance plus live eligibility filters, with no vector and no embedding model. */
final case class LexicalSearchQuery(
    text: String,
    filter: JobSearchFilter,
    /** Requested response page; repositories may return more hits up to their configured branch-result cap. */
    first: PageSize,
    searchId: UUID,
    candidateFilters: ValidatedCandidateMatchFilters = ValidatedCandidateMatchFilters.empty
)

/** A lexical hit carries no embedding evidence: lexical eligibility never depends on embedding freshness. */
final case class LexicalRetrievalHit[Id](id: Id, score: Double, searchId: UUID)
type LexicalJobHit = LexicalRetrievalHit[JobId]
type LexicalCandidateHit = LexicalRetrievalHit[UserId]

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

/** Why a search answered from the lexical fallback; always `ProviderUnavailable` or `VectorSearchUnavailable`. */
final case class DegradedSearch(reason: com.example.graphQL.cats.service.SearchError)

/** Ranked hits plus, when the lexical fallback answered, the reason the primary retrieval could not. */
final case class SearchResults[A](hits: List[A], degraded: Option[DegradedSearch])

/** A ranked job as returned to callers; `model` is absent for lexical fallback hits. */
final case class SearchedJob(
    job: Job,
    score: Double,
    mode: SearchMode,
    model: Option[String],
    searchId: UUID,
    matchedSkills: List[String] = Nil,
    retrievalScore: Option[Double] = None
)

object SearchedJob {
  def from(ranked: RankedJob): SearchedJob =
    SearchedJob(
      ranked.job,
      ranked.score,
      ranked.mode,
      Some(ranked.meta.model),
      ranked.searchId,
      ranked.matchedSkills,
      ranked.retrievalScore
    )
}

/** A ranked candidate as returned to recruiters; `model` is absent for lexical fallback hits. */
final case class SearchedCandidate(
    candidate: CandidateSearchHit,
    score: Double,
    mode: SearchMode,
    model: Option[String],
    searchId: UUID,
    matchedSkills: List[String] = Nil,
    retrievalScore: Option[Double] = None
)

object SearchedCandidate {
  def from(ranked: RankedCandidate): SearchedCandidate =
    SearchedCandidate(
      ranked.candidate,
      ranked.score,
      ranked.mode,
      Some(ranked.meta.model),
      ranked.searchId,
      ranked.matchedSkills,
      ranked.retrievalScore
    )
}

object SkillMatching {
  def matched(candidateSkills: Set[String], targetSkills: Set[String]): List[String] = {
    val candidateCanonical = candidateSkills.map(_.trim.toLowerCase(java.util.Locale.ROOT))
    targetSkills.toList
      .filter(skill => candidateCanonical.contains(skill.trim.toLowerCase(java.util.Locale.ROOT)))
      .sortBy(skill => (skill.trim.toLowerCase(java.util.Locale.ROOT), skill))
  }
}
