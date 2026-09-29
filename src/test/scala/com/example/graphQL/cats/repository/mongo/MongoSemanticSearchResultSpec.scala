package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.search.{JobSearchFilter, RankedCandidate, VectorSearchQuery}
import com.example.graphQL.cats.service.ServiceFixtures;
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.model.{
  CandidateProfile,
  EmbeddingMeta,
  EntityEmbedding,
  SearchMode,
  SearchableText,
  UserProfile
}
import munit.FunSuite
import org.bson.Document

import java.util.UUID

final class MongoSemanticSearchResultSpec extends FunSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000801")
  private val pageSize = PageSize.fromInt(10).toOption.getOrElse(fail("invalid page size"))
  private val query = VectorSearchQuery(
    List(0.1f, 0.2f),
    None,
    JobSearchFilter(None, Set.empty, None),
    pageSize,
    SearchMode.VECTOR,
    "voyage-4-lite",
    searchId
  )

  test("fresh job vector hit is returned with score and metadata") {
    val job = ServiceFixtures.openJob.copy(embedding = Some(jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite")))
    val ranked = MongoSemanticSearchResult.rankedJob(
      scored(MongoHiringCodecs.job(job)).append("retrievalScore", java.lang.Double.valueOf(0.88d)),
      query
    )

    assertEquals(ranked.map(_.map(_.job.id)), Right(Some(job.id)))
    assertEquals(ranked.map(_.map(_.score)), Right(Some(0.91d)))
    assertEquals(ranked.map(_.map(_.retrievalScore)), Right(Some(Some(0.88d))))
    assertEquals(ranked.map(_.map(_.meta.sourceHash)), Right(Some(SourceHash.sha256(SearchableText.job(job)))))
  }

  test("malformed native-fusion job results are classified as invalid stored data") {
    val malformed = new Document("_id", "unexpected").append("unsupported", true).append("score", "invalid")

    assertEquals(
      MongoSemanticSearchResult.rankedJobs(List(malformed), query),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
  }

  test("stale job vector hit is omitted") {
    val stale = ServiceFixtures.openJob.copy(embedding =
      Some(
        jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite")
          .copy(meta = jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite").meta.copy(sourceHash = "stale"))
      )
    )

    assertEquals(MongoSemanticSearchResult.rankedJob(scored(MongoHiringCodecs.job(stale)), query), Right(None))
  }

  test("fresh job vector hits with missing or nonnumeric scores are invalid stored data") {
    val job = ServiceFixtures.openJob.copy(embedding = Some(jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite")))
    val document = MongoHiringCodecs.job(job)

    assertEquals(
      MongoSemanticSearchResult.rankedJob(document, query),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
    assertEquals(
      MongoSemanticSearchResult.rankedJob(document.append("score", "invalid"), query),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
  }

  test("fresh candidate vector hit is returned and stale candidate hit is omitted") {
    val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume-ref"))
    val fresh = ServiceFixtures.candidate.copy(
      profile = Some(UserProfile.Candidate(profile)),
      embedding = Some(candidateEmbedding(profile, "voyage-4-lite"))
    )
    val stale = fresh.copy(embedding =
      Some(
        candidateEmbedding(profile, "voyage-4-lite")
          .copy(meta = candidateEmbedding(profile, "voyage-4-lite").meta.copy(sourceHash = "stale"))
      )
    )

    assert(
      MongoSemanticSearchResult
        .rankedCandidate(
          scored(MongoHiringCodecs.user(fresh)).append("retrievalScore", java.lang.Double.valueOf(0.87d)),
          query
        )
        .exists(_.exists {
          case RankedCandidate(candidate, 0.91d, SearchMode.VECTOR, meta, `searchId`, _, Some(0.87d)) =>
            candidate.id == fresh.id && candidate.skills == profile.skills &&
            meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
          case _ => false
        })
    )
    assertEquals(
      MongoSemanticSearchResult.rankedCandidate(
        MongoHiringCodecs.user(fresh).append("score", "invalid"),
        query
      ),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
    assertEquals(
      MongoSemanticSearchResult.rankedCandidate(MongoHiringCodecs.user(fresh), query),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
    assertEquals(MongoSemanticSearchResult.rankedCandidate(scored(MongoHiringCodecs.user(stale)), query), Right(None))
  }

  test("malformed native-fusion candidate results are classified as invalid stored data") {
    assertEquals(
      MongoSemanticSearchResult.rankedCandidates(List(new Document("_id", "invalid")), query),
      Left(com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
    )
  }

  private def jobEmbedding(
      job: com.example.graphQL.cats.domain.model.Job,
      model: String
  ): EntityEmbedding =
    EntityEmbedding(
      List(0.1f, 0.2f),
      EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(job)), ServiceFixtures.now)
    )

  private def candidateEmbedding(profile: CandidateProfile, model: String): EntityEmbedding =
    EntityEmbedding(
      List(0.1f, 0.2f),
      EmbeddingMeta(model, SourceHash.sha256(SearchableText.candidate(profile)), ServiceFixtures.now)
    )

  private def scored(document: Document): Document =
    document.append("score", java.lang.Double.valueOf(0.91d))
}
