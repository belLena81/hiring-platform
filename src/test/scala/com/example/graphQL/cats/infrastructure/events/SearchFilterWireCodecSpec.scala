package com.example.graphQL.cats.infrastructure.events

import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.events.SearchFilterSnapshot
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class SearchFilterWireCodecSpec extends FunSuite {
  private val jobId = JobId(new UUID(0L, 0x204L))
  private val snapshots = List(
    SearchFilterSnapshot
      .JobCriteria(Some("Nicosia"), List("Cats", "Scala"), Some(Instant.parse("2026-09-19T10:15:30Z"))),
    SearchFilterSnapshot.JobCriteria(None, Nil, None),
    SearchFilterSnapshot.Unfiltered,
    SearchFilterSnapshot.ForJob(jobId)
  )

  test("each of the three stored filter shapes round-trips through its snapshot") {
    snapshots.foreach(snapshot =>
      assertEquals(SearchFilterWireCodec.decodeString(SearchFilterWireCodec.encodeString(snapshot)), Right(snapshot))
    )
  }

  test("decoding accepts only the three written key sets") {
    val rejected = List(
      """{"city":"Nicosia"}""",
      """{"city":"Nicosia","skills":[],"createdAfter":null,"extra":1}""",
      """{"jobId":"00000000-0000-0000-0000-000000000204","city":null}""",
      """{"jobId":"not-a-uuid"}""",
      """{"city":1,"skills":[],"createdAfter":null}""",
      """{"city":null,"skills":["Scala",1],"createdAfter":null}""",
      """{"city":null,"skills":[],"createdAfter":"yesterday"}""",
      """[]""",
      """not json"""
    )
    rejected.foreach(raw => assert(SearchFilterWireCodec.decodeString(raw).isLeft, raw))
  }
}
