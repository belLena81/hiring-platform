package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{CandidateProfile, JobStatus}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.search.{
  CandidateMatchFilters,
  CandidateSearchEligibility,
  JobSearchEligibility,
  JobSearchFilter,
  LexicalRetrievalHit,
  SearchEligibilityCriteria,
  SearchEligibilityPolicy,
  ValidatedCandidateMatchFilters
}
import java.util.UUID
import munit.FunSuite
import org.bson.Document

/** Lexical fallback retrieval and eligibility need no embedding state (AIR-05, AIR-07). */
final class MongoLexicalFallbackSpec extends FunSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val filters = ValidatedCandidateMatchFilters
    .from(CandidateMatchFilters(List("Scala"), Some("Cyprus"), None, None))
    .fold(errors => fail(errors.toString), identity)

  test("AIR-07 candidate lexical criteria keep role, status, skills and consent but not the embedding model") {
    val rendered = MongoSearchEligibilityRendering
      .indexed(SearchEligibilityCriteria.candidates(filters, None))
      .toJson
    assert(!rendered.contains("embeddingMeta.model"), rendered)
    List("role", "accountStatus", "profile.recruiterSearchOptIn").foreach(path =>
      assert(rendered.contains(s""""path": "$path""""), s"$path in $rendered")
    )
    val vector = MongoSearchEligibilityRendering
      .indexed(SearchEligibilityCriteria.candidates(filters, Some("voyage")))
      .toJson
    assert(vector.contains("embeddingMeta.model"), vector)
  }

  test("AIR-07 a lexical hit is identity and score only and never needs embedding metadata") {
    val id = UUID.fromString("00000000-0000-0000-0000-0000000000a1")
    val hit = MongoSemanticSearchResult.lexicalHit(new Document("_id", id.toString).append("score", 2.5d), searchId)(
      JobId.apply
    )
    assertEquals(hit, Right(LexicalRetrievalHit(JobId(id), 2.5d, searchId)))
    assertEquals(
      MongoSemanticSearchResult.lexicalHit(new Document("_id", id.toString), searchId)(UserId.apply),
      Left(RepositoryError.InvalidStoredData)
    )
    assertEquals(
      MongoSemanticSearchResult.lexicalHit(new Document("_id", "nope").append("score", 1d), searchId)(UserId.apply),
      Left(RepositoryError.InvalidStoredData)
    )
  }

  test("AIR-05 lexical eligibility ignores embedding freshness but keeps the live open and filter checks") {
    val noEmbedding = JobSearchEligibility(openJob, None)
    assert(SearchEligibilityPolicy.jobLexical(noEmbedding, JobSearchFilter(Some("Kyiv"), Set("Scala"), None)))
    assert(!SearchEligibilityPolicy.jobLexical(noEmbedding, JobSearchFilter(Some("Lviv"), Set.empty, None)))
    assert(
      !SearchEligibilityPolicy.jobLexical(
        JobSearchEligibility(openJob.copy(status = JobStatus.Closed), None),
        JobSearchFilter(None, Set.empty, None)
      )
    )
    val withoutEmbedding = CandidateSearchEligibility.fromUser(candidate)
    assert(
      SearchEligibilityPolicy.candidateLexical(withoutEmbedding, ValidatedCandidateMatchFilters.empty)
    )
    assert(
      !SearchEligibilityPolicy.candidateLexical(
        withoutEmbedding.copy(profile = None),
        ValidatedCandidateMatchFilters.empty
      )
    )
    assert(
      !SearchEligibilityPolicy.candidateLexical(
        withoutEmbedding.copy(profile = Some(CandidateProfile(Set("Java"), None, None))),
        filters
      )
    )
  }

  test(
    "AIR-05 candidate lexical retrieval applies live eligibility before the limit, so stale index entries cannot shorten the page"
  ) {
    val repository = new MongoSemanticSearchRepository(
      null,
      "jobs_vector",
      "candidates_vector",
      "jobs_lexical",
      "candidates_lexical",
      numCandidates = 60,
      diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
    )
    val query = com.example.graphQL.cats.service.search.LexicalSearchQuery(
      "scala",
      JobSearchFilter(None, Set.empty, None),
      com.example.graphQL.cats.domain.pagination.PageSize.next(
        com.example.graphQL.cats.domain.pagination.PageSize.fromInt(5).toOption.get
      ),
      searchId,
      filters
    )
    val stages = repository.lexicalCandidateStages(query).map(_.keySet.iterator.next)
    assertEquals(stages.take(3), List("$search", "$match", "$limit"))
    val live = repository.lexicalCandidateStages(query)(1).get("$match").toString
    List("role", "accountStatus", "profile.skillsCanonical").foreach(path => assert(live.contains(path), live))
  }
}
