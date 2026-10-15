package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.infrastructure.events.SearchFilterWireCodec
import com.example.graphQL.cats.service.events.SearchFilterSnapshot
import com.example.graphQL.cats.service.search.JobSearchFilter
import java.time.Instant
import java.util.UUID
import munit.FunSuite

/** The three SearchSession filter shapes written by the search resolvers; literals must never change. */
class SearchSessionFilterWireContractSpec extends FunSuite {
  test("job criteria filter keeps key order city, skills, createdAfter with sorted skills") {
    val filter = JobSearchFilter(Some("Nicosia"), Set("Scala", "Cats"), Some(Instant.parse("2026-09-19T10:15:30Z")))
    assertEquals(
      SearchFilterWireCodec.encodeString(HiringGraphQLResolverSupport.filterSnapshot(filter)),
      """{"city":"Nicosia","skills":["Cats","Scala"],"createdAfter":"2026-09-19T10:15:30Z"}"""
    )
  }

  test("job criteria filter writes nulls and an empty skill list for absent criteria") {
    assertEquals(
      SearchFilterWireCodec.encodeString(
        HiringGraphQLResolverSupport.filterSnapshot(JobSearchFilter(None, Set.empty, None))
      ),
      """{"city":null,"skills":[],"createdAfter":null}"""
    )
  }

  test("recommended jobs write an empty object and candidate matches write the job id") {
    assertEquals(SearchFilterWireCodec.encodeString(SearchFilterSnapshot.Unfiltered), "{}")
    val jobId = new UUID(0L, 0x204L)
    assertEquals(
      SearchFilterWireCodec.encodeString(SearchFilterSnapshot.ForJob(JobId(jobId))),
      """{"jobId":"00000000-0000-0000-0000-000000000204"}"""
    )
  }
}
