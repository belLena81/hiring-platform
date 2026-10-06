package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, GeoPoint, Job, SearchMode}
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
  def isValid: Boolean = center.isValid && radiusKm.isFinite && radiusKm > 0d && radiusKm <= NearbyJobsQuery.MaxRadiusKm
  def fingerprint: String = com.example.graphQL.cats.shared.crypto.SourceHash.sha256(
    io.circe.Json
      .arr(
        io.circe.Json.fromDoubleOrNull(center.latitude),
        io.circe.Json.fromDoubleOrNull(center.longitude),
        io.circe.Json.fromDoubleOrNull(radiusKm),
        filter.city.map(value => io.circe.Json.fromString(value.trim)).getOrElse(io.circe.Json.Null),
        io.circe.Json.fromValues(filter.skills.toList.map(_.trim).distinct.sorted.map(io.circe.Json.fromString)),
        filter.createdAfter.map(value => io.circe.Json.fromString(value.toString)).getOrElse(io.circe.Json.Null),
        io.circe.Json.fromString("distanceKm,id")
      )
      .noSpaces
  )
}
final case class NearbyJobCursor(distanceKm: Double, jobId: String, queryFingerprint: String)
final case class NearbyJobsPage(query: NearbyJobsQuery, first: Int, after: Option[NearbyJobCursor])
final case class NearbyRadius(center: GeoPoint, radiusKm: Double) {
  def isValid: Boolean = center.isValid && radiusKm.isFinite && radiusKm > 0d && radiusKm <= NearbyJobsQuery.MaxRadiusKm
}
final case class JobFacetQuery(filter: JobSearchFilter, radius: Option[NearbyRadius]) {
  def isValid: Boolean = radius.forall(_.isValid)
}

object NearbyJobsQuery {
  val MaxRadiusKm: Double = 500d

  def encodeCursor(distanceKm: Double, jobId: String, query: NearbyJobsQuery): String = {
    val payload = s"$distanceKm|$jobId|${query.fingerprint}"
    java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  }

  def decodeCursor(value: String, query: NearbyJobsQuery): Either[String, NearbyJobCursor] =
    scala.util
      .Try(new String(java.util.Base64.getUrlDecoder.decode(value), java.nio.charset.StandardCharsets.UTF_8))
      .toEither
      .left
      .map(_ => "Invalid nearby cursor")
      .flatMap { decoded =>
        decoded.split("\\|", -1).toList match {
          case distanceText :: jobId :: fingerprint :: Nil =>
            for {
              distance <- scala.util.Try(distanceText.toDouble).toEither.left.map(_ => "Invalid nearby cursor")
              _ <- Either.cond(distance.isFinite && distance >= 0d, (), "Invalid nearby cursor")
              _ <- scala.util.Try(UUID.fromString(jobId)).toEither.left.map(_ => "Invalid nearby cursor")
              _ <- Either.cond(fingerprint == query.fingerprint, (), "Nearby cursor does not match criteria")
            } yield NearbyJobCursor(distance, jobId, fingerprint)
          case _ => Left("Invalid nearby cursor")
        }
      }
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
    val dimensions = List(
      buckets(jobs.flatMap(_.skills)),
      buckets(jobs.map(_.location.country).filter(_.nonEmpty)),
      buckets(jobs.map(_.location.city).filter(_.nonEmpty)),
      buckets(jobs.map(job => job.location.remote.toString))
    )
    JobDiscoveryFacets(
      dimensions(0).take(MaxBucketsPerDimension),
      dimensions(1).take(MaxBucketsPerDimension),
      dimensions(2).take(MaxBucketsPerDimension),
      dimensions(3).take(MaxBucketsPerDimension),
      dimensions.exists(_.size > MaxBucketsPerDimension)
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
