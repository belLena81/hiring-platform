package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.{SearchFilterSnapshot, SearchSession, SearchSessionResult}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

/** The stored `filter` string of a SearchSession for each of the three written shapes; literals must never change. */
class MongoSearchSessionFilterWireContractSpec extends FunSuite {
  private val now = Instant.parse("2026-09-19T10:15:30Z")
  private val jobId = new UUID(0L, 0x204L)

  private def stored(kind: SearchKind, filter: SearchFilterSnapshot): String = {
    val session = SearchSession(
      new UUID(0L, 0x205L),
      UserId(new UUID(0L, 0x202L)),
      kind,
      None,
      filter,
      None,
      List(SearchSessionResult(jobId.toString, 1, 0.5)),
      now,
      now.plusSeconds(60)
    )
    val document = MongoHiringCodecs.searchSession(session)
    assertEquals(MongoHiringCodecs.readSearchSession(document).toOption.map(_.filter), Some(filter))
    document.getString("filter")
  }

  test("job criteria filter is stored as the exact compact string") {
    val filter = SearchFilterSnapshot.JobCriteria(Some("Nicosia"), List("Cats", "Scala"), Some(now))
    assertEquals(
      stored(SearchKind.Jobs, filter),
      """{"city":"Nicosia","skills":["Cats","Scala"],"createdAfter":"2026-09-19T10:15:30Z"}"""
    )
  }

  test("recommended jobs filter is stored as an empty object") {
    assertEquals(stored(SearchKind.RecommendedJobs, SearchFilterSnapshot.Unfiltered), "{}")
  }

  test("candidate matches filter is stored with the job id only") {
    assertEquals(
      stored(SearchKind.CandidateMatches, SearchFilterSnapshot.ForJob(JobId(jobId))),
      """{"jobId":"00000000-0000-0000-0000-000000000204"}"""
    )
  }
}
