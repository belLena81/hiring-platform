package com.example.graphQL.cats.service.port

import munit.FunSuite

final class EmbeddingWorkKeySpec extends FunSuite {
  test("AC-08.3 a work row identity round-trips to the kind and entity it was built from") {
    val keys = List(
      EmbeddingWorkKey(EmbeddingWorkKind.Job, "00000000-0000-0000-0000-000000000001"),
      EmbeddingWorkKey(EmbeddingWorkKind.CandidateProfile, "00000000-0000-0000-0000-000000000002")
    )
    keys.foreach(key => assertEquals(EmbeddingWorkKey.fromId(key.value), Some(key)))
  }

  test("AC-08.3 an identity without a known kind or an entity is not a key") {
    List("", "Job", "Job:", ":entity", "Unknown:entity", "job:entity").foreach(id =>
      assertEquals(EmbeddingWorkKey.fromId(id), None, id)
    )
  }

  test("AC-08.3 an entity that itself contains the separator keeps it") {
    assertEquals(
      EmbeddingWorkKey.fromId("Job:a:b"),
      Some(EmbeddingWorkKey(EmbeddingWorkKind.Job, "a:b"))
    )
  }
}
