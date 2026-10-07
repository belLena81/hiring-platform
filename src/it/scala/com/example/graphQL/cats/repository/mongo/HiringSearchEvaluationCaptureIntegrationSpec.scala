package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import org.bson.Document
import java.util.UUID

/** Pure adapter/source checks; this suite never connects to Atlas or calls an embedding provider. */
class HiringSearchEvaluationCaptureIntegrationSpec extends munit.FunSuite {
  private val page = PageSize.fromInt(7).fold(errors => fail(errors.toString), identity)
  private val query = VectorSearchQuery(
    HiringSearchEvaluationCorpus.vector(Set("scala", "cats")),
    Some("scala cats"),
    JobSearchFilter(None, Set.empty, None),
    page,
    SearchMode.HYBRID,
    HiringSearchEvaluationCorpus.Model,
    UUID.fromString("00000000-0000-0000-0003-000000000001")
  )

  test("Atlas requires explicit disposable opt-in and nonce database identity") {
    assert(HiringSearchEvaluationAtlasRunner.parse(Nil).isLeft)
    assert(
      HiringSearchEvaluationAtlasRunner
        .parse(List("--output", ".local/data/search", "--source-revision", "revision", "--concurrency", "1"))
        .isLeft
    )
    val valid = List(
      "--authorize-disposable-atlas",
      "--output",
      ".local/data/search",
      "--source-revision",
      "revision",
      "--concurrency",
      "8"
    )
    assert(HiringSearchEvaluationAtlasRunner.parse(valid).isRight)
    assert(HiringSearchEvaluationAtlasRunner.parse(valid.updated(2, "/tmp/output")).isLeft)
    assert(HiringSearchEvaluationAtlasRunner.parse(valid.updated(2, ".local/data/../../escape")).isLeft)
    assert(HiringSearchEvaluationAtlasRunner.parse(valid.updated(6, "16")).isLeft)
    val first =
      HiringSearchEvaluationAtlasRunner.ownedDatabaseName(UUID.fromString("00000000-0000-0000-0000-000000000001"))
    val second =
      HiringSearchEvaluationAtlasRunner.ownedDatabaseName(UUID.fromString("00000000-0000-0000-0000-000000000002"))
    assert(first.startsWith("search_evaluation_curated_"))
    assertNotEquals(first, second)
  }

  test("curated stale retrieval metadata fails authoritative job and candidate eligibility") {
    def scored(document: Document): Document = new Document(document).append(MongoFields.Score, 1.0d)
    val jobs = HiringSearchEvaluationCorpus.jobs
    assertEquals(jobs.size, 8)
    jobs.foreach { job =>
      val result = MongoSemanticSearchResult.jobHit(scored(HiringSearchEvaluationCorpus.jobDocument(job)), query)
      if (job.id.value.toString == SearchEvaluationFixtures.jobId(8)) {
        assertEquals(
          result.map(
            _.exists(hit =>
              com.example.graphQL.cats.service.search.SearchEligibilityPolicy.job(
                com.example.graphQL.cats.service.search.JobSearchEligibility.fromJob(job),
                hit.meta,
                query.model,
                query.filter
              )
            )
          ),
          Right(false)
        )
        assert(job.embedding.exists(_.meta.sourceHash != SourceHash.sha256(SearchableText.job(job))))
      } else assert(result.toOption.flatten.nonEmpty)
    }
    HiringSearchEvaluationCorpus.candidates.foreach { user =>
      val result =
        MongoSemanticSearchResult.candidateHit(scored(HiringSearchEvaluationCorpus.candidateDocument(user)), query)
      if (user.id.value.toString == SearchEvaluationFixtures.candidateId(8))
        assertEquals(
          result.map(
            _.exists(hit =>
              com.example.graphQL.cats.service.search.SearchEligibilityPolicy.candidate(
                com.example.graphQL.cats.service.search.CandidateSearchEligibility.fromUser(user),
                hit.meta,
                query.model,
                com.example.graphQL.cats.service.search.ValidatedCandidateMatchFilters.empty
              )
            )
          ),
          Right(false)
        )
      else assert(result.toOption.flatten.nonEmpty)
    }
    val wrongModel = query.copy(model = "unrelated-model")
    assertEquals(
      MongoSemanticSearchResult.jobHit(scored(HiringSearchEvaluationCorpus.jobDocument(jobs.head)), wrongModel),
      Right(None)
    )
  }

  test("production authoritative eligibility applies declared filters separately from role relevance") {
    SearchEvaluationFixtures.queries.filter(_.useCase != SearchEvaluationUseCase.Recommendations).foreach { fixture =>
      val eligible = if (fixture.useCase == SearchEvaluationUseCase.RecruiterMatching) {
        val filters = HiringSearchEvaluationCorpus.candidateFilter(fixture)
        val validated = ValidatedCandidateMatchFilters.from(filters).fold(errors => fail(errors.toString), identity)
        HiringSearchEvaluationCorpus.candidates.flatMap { user =>
          val source = HiringSearchEvaluationCorpus.candidateDocument(user)
          val selection = MongoSearchEligibilityCodecs.candidate(source)
          selection.toOption
            .filter(value =>
              value.metadata.exists(meta =>
                SearchEligibilityPolicy.candidate(value, meta, HiringSearchEvaluationCorpus.Model, validated)
              )
            )
            .map(_.id.value.toString)
        }
      } else
        HiringSearchEvaluationCorpus.jobs
          .filterNot(_.id.value.toString == SearchEvaluationFixtures.jobId(7))
          .filter { job =>
            job.embedding.exists(value =>
              SearchEligibilityPolicy.job(
                JobSearchEligibility.fromJob(job),
                value.meta,
                HiringSearchEvaluationCorpus.Model,
                HiringSearchEvaluationCorpus.jobFilter(fixture)
              )
            )
          }
          .map(_.id.value.toString)
      assertEquals(eligible, fixture.eligibleIds)
      if (fixture.filterGroup == SearchEvaluationFilterGroup.Broad) assert(eligible.toSet != fixture.relevantIds)
    }
  }

  test("consent false and absent remain public-match eligible; opted-in missing attributes reject private filters") {
    val fixture = SearchEvaluationFixtures.queries
      .find(_.queryId == "RecruiterMatching-3")
      .getOrElse(fail("Missing selective fixture"))
    val filters = HiringSearchEvaluationCorpus.candidateFilter(fixture).copy(requiredSkills = Nil)
    val validated = ValidatedCandidateMatchFilters.from(filters).fold(errors => fail(errors.toString), identity)
    val ids = HiringSearchEvaluationCorpus.candidates.flatMap { user =>
      MongoSearchEligibilityCodecs
        .candidate(HiringSearchEvaluationCorpus.candidateDocument(user))
        .toOption
        .filter(value =>
          value.metadata
            .exists(meta =>
              SearchEligibilityPolicy.candidate(value, meta, HiringSearchEvaluationCorpus.Model, validated)
            )
        )
        .map(_.id.value.toString)
    }
    assert(ids.contains(SearchEvaluationFixtures.candidateId(3)))
    assert(ids.contains(SearchEvaluationFixtures.candidateId(4)))
    assert(!ids.contains(SearchEvaluationFixtures.candidateId(5)))
    assert(!ids.contains(SearchEvaluationFixtures.candidateId(6)))
    assert(!ids.contains(SearchEvaluationFixtures.candidateId(7)))
    assert(!ids.contains(SearchEvaluationFixtures.candidateId(8)))
    val absent = HiringSearchEvaluationCorpus
      .candidateDocument(HiringSearchEvaluationCorpus.candidates(3))
      .get(MongoFields.Profile, classOf[Document])
    assert(!absent.containsKey("recruiterSearchOptIn"))
  }
}
