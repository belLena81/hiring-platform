package com.example.graphQL.cats.shared.search

import com.example.graphQL.cats.domain.model.{EmbeddingMeta, SearchMode}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.ServiceFixtures
import munit.FunSuite

import java.util.UUID

final class HybridRankFusionSpec extends FunSuite {
  private val meta = EmbeddingMeta("voyage-4-lite", "source", ServiceFixtures.now)

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
}
