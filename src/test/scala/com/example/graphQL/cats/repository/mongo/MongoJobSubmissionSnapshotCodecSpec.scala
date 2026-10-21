package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.{JobStatus, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import munit.FunSuite
import org.bson.Document
import java.util.UUID

final class MongoJobSubmissionSnapshotCodecSpec extends FunSuite {
  private val id = UUID.fromString("00000000-0000-0000-0000-000000000007")
  private def valid = new Document("_id", id.toString).append("status", "Open")

  test("decode only submission identity and status") {
    assertEquals(
      MongoJobSubmissionSnapshotCodec.read(valid).toEither,
      Right(JobSubmissionSnapshot(JobId(id), JobStatus.Open))
    )
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("version", 7L)).isInvalid)
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("status", "Closed")).isValid)
  }

  test("fail closed for missing fields and invalid UUID/status") {
    List("_id", "status").foreach { field =>
      val document = valid
      val _ = document.remove(field)
      assert(MongoJobSubmissionSnapshotCodec.read(document).isInvalid)
    }
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("_id", "1-1-1-1-1")).isInvalid)
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("status", "Unknown")).isInvalid)
  }

  test("reject accidental aggregate hydration at the projection boundary") {
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("embedding", List(1f))).isInvalid)
  }
}
