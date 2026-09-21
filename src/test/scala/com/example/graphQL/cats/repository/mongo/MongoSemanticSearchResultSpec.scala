package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.shared.pagination.PageSize
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, VectorSearchQuery}
import com.example.graphQL.cats.service.ServiceFixtures;
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.model.{
  CandidateProfile, EmbeddingMeta, EntityEmbedding, SearchMode, SearchableText, UserProfile
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
    val ranked = MongoSemanticSearchResult.rankedJob(scored(MongoHiringCodecs.job(job)), query)

    assertEquals(ranked.map(_.map(_.job.id)), Right(Some(job.id)))
    assertEquals(ranked.map(_.map(_.score)), Right(Some(0.91d)))
    assertEquals(ranked.map(_.map(_.meta.sourceHash)), Right(Some(SourceHash.sha256(SearchableText.job(job)))))
  }

  test("stale job vector hit is omitted") {
    val stale = ServiceFixtures.openJob.copy(embedding = Some(jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite")
      .copy(meta = jobEmbedding(ServiceFixtures.openJob, "voyage-4-lite").meta.copy(sourceHash = "stale"))))

    assertEquals(MongoSemanticSearchResult.rankedJob(scored(MongoHiringCodecs.job(stale)), query), Right(None))
  }

  test("fresh candidate vector hit is returned and stale candidate hit is omitted") {
    val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume-ref"))
    val fresh = ServiceFixtures.candidate.copy(profile = Some(UserProfile.Candidate(profile)),
      embedding = Some(candidateEmbedding(profile, "voyage-4-lite")))
    val stale = fresh.copy(embedding = Some(candidateEmbedding(profile, "voyage-4-lite")
      .copy(meta = candidateEmbedding(profile, "voyage-4-lite").meta.copy(sourceHash = "stale"))))

    assert(MongoSemanticSearchResult.rankedCandidate(scored(MongoHiringCodecs.user(fresh)), query).exists(_.exists {
      case RankedCandidate(candidate, 0.91d, SearchMode.VECTOR, meta, `searchId`) =>
        candidate.id == fresh.id && meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))
      case _ => false
    }))
    assertEquals(MongoSemanticSearchResult.rankedCandidate(scored(MongoHiringCodecs.user(stale)), query), Right(None))
  }

  private def jobEmbedding(
      job: com.example.graphQL.cats.domain.model.Job,
      model: String
  ): EntityEmbedding =
    EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(job)), ServiceFixtures.now))

  private def candidateEmbedding(profile: CandidateProfile, model: String): EntityEmbedding =
    EntityEmbedding(List(0.1f, 0.2f),
      EmbeddingMeta(model, SourceHash.sha256(SearchableText.candidate(profile)), ServiceFixtures.now))

  private def scored(document: Document): Document =
    document.append("score", java.lang.Double.valueOf(0.91d))
}
