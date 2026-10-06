package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, SearchMode}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.ServiceFixtures
import munit.FunSuite

import java.util.UUID

final class HybridRankFusionSpec extends FunSuite {
  private val meta = EmbeddingMeta("voyage-4-lite", "source", ServiceFixtures.now)

  private def candidate(id: Int, name: String = "Candidate"): RankedCandidate = RankedCandidate(
    CandidateSearchHit(UserId(new UUID(0L, id.toLong)), name, Set("Scala"), None),
    0.5,
    SearchMode.VECTOR,
    meta,
    new UUID(1L, 1L)
  )

  test("documents present in both retrieval branches receive the combined reciprocal-rank score") {
    val shared = ServiceFixtures.openJob
    val vectorOnly = shared.copy(id = JobId(UUID.fromString("00000000-0000-0000-0000-000000000901")))
    val lexicalOnly = shared.copy(id = JobId(UUID.fromString("00000000-0000-0000-0000-000000000902")))
    val vector = List(
      RankedJob(shared, 0.9, SearchMode.HYBRID, meta, UUID.randomUUID()),
      RankedJob(vectorOnly, 0.8, SearchMode.HYBRID, meta, UUID.randomUUID())
    )
    val lexical = List(
      RankedJob(shared, 0.4, SearchMode.HYBRID, meta, UUID.randomUUID()),
      RankedJob(lexicalOnly, 0.3, SearchMode.HYBRID, meta, UUID.randomUUID())
    )

    val result = HybridRankFusion.jobs(vector, lexical, limit = 3)

    assertEquals(result.map(_.job.id), List(shared.id, vectorOnly.id, lexicalOnly.id))
    assertEquals(result.head.score, 2.0 / 61.0)
    assertEquals(result.head.mode, SearchMode.HYBRID)
  }

  test("fusion truncates to the requested page size") {
    val jobs = (0 until 3).toList.map { index =>
      val id = JobId(UUID.fromString(f"00000000-0000-0000-0000-0000000009${index}%02d"))
      RankedJob(ServiceFixtures.openJob.copy(id = id), 1.0, SearchMode.HYBRID, meta, UUID.randomUUID())
    }

    assertEquals(HybridRankFusion.jobs(jobs, Nil, limit = 2).size, 2)
  }

  test("candidate fusion combines three ranks deterministically and matched skills preserve job spelling") {
    def candidate(id: Int): RankedCandidate = RankedCandidate(
      CandidateSearchHit(
        UserId(UUID.fromString(f"00000000-0000-0000-0000-0000000009${id}%02d")),
        s"Candidate $id",
        Set("scala"),
        None
      ),
      0.5,
      SearchMode.VECTOR,
      meta,
      UUID.randomUUID()
    )
    val first = candidate(1)
    val second = candidate(2)
    val fused = HybridRankFusion.candidates(List(first, second), List(second, first), List(first), limit = 2)

    assertEquals(fused.map(_.candidate.id), List(first.candidate.id, second.candidate.id))
    assert(math.abs(fused.head.score - (2.0 / 61.0 + 1.0 / 62.0)) < 1e-12)
    assertEquals(
      SkillMatching.matched(Set(" scala ", "JAVA"), Set("Scala", "JavaScript", "java")),
      List("java", "Scala")
    )
  }

  test("candidate score ties preserve job then query then lexical branch precedence") {
    val jobHit = candidate(3)
    val queryHit = candidate(2)
    val lexicalHit = candidate(1)
    val result = HybridRankFusion.candidates(List(jobHit), List(queryHit), List(lexicalHit), limit = 3)
    assertEquals(result.map(_.candidate.id), List(jobHit.candidate.id, queryHit.candidate.id, lexicalHit.candidate.id))
    assertEquals(result.map(_.score), List.fill(3)(1.0 / 61.0))
    assertEquals(
      HybridRankFusion.candidates(Nil, List(queryHit), List(lexicalHit), limit = 2).map(_.candidate.id),
      List(queryHit.candidate.id, lexicalHit.candidate.id)
    )
  }

  test("candidate fusion preserves score addition order and the first branch's candidate representation") {
    val shared = candidate(1, "Job vector name")
    val result = HybridRankFusion.candidates(
      List(shared),
      List(candidate(2), shared.copy(candidate = shared.candidate.copy(name = "Query vector name"))),
      List(candidate(3), candidate(4), shared.copy(candidate = shared.candidate.copy(name = "Lexical name"))),
      limit = 1
    )
    assertEquals(result.map(_.candidate), List(shared.candidate))
    assertEquals(result.map(_.score), List((1.0 / 61.0 + 1.0 / 62.0) + 1.0 / 63.0))
    val lexicalOnly = shared.copy(candidate = shared.candidate.copy(name = "Lexical only"))
    assertEquals(
      HybridRankFusion.candidates(Nil, Nil, List(lexicalOnly), limit = 1),
      List(lexicalOnly.copy(score = 1.0 / 61.0))
    )
  }

  test("candidate fusion preserves empty branches and bounded output") {
    assertEquals(HybridRankFusion.candidates(Nil, Nil, Nil, limit = 3), Nil)
    val hits = List(candidate(1), candidate(2), candidate(3))
    assertEquals(HybridRankFusion.candidates(hits, Nil, Nil, limit = 0), Nil)
    assertEquals(
      HybridRankFusion.candidates(hits, Nil, Nil, limit = 2).map(_.candidate.id),
      hits.take(2).map(_.candidate.id)
    )
    assertEquals(
      HybridRankFusion.candidates(Nil, hits, Nil, limit = 2).map(_.candidate.id),
      hits.take(2).map(_.candidate.id)
    )
  }
}
