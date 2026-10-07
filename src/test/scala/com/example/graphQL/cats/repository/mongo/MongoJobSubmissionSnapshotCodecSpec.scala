package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.{JobStatus, JobSubmissionSnapshot}
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import munit.FunSuite
import org.bson.Document
import java.util.UUID

final class MongoJobSubmissionSnapshotCodecSpec extends FunSuite {
  private val id = UUID.fromString("00000000-0000-0000-0000-000000000007")
  private def valid = new Document("_id", id.toString).append("status", "Open").append("version", 7L)

  test("decode only submission identity, status and nonnegative revision") {
    assertEquals(
      MongoJobSubmissionSnapshotCodec.read(valid).toEither,
      Right(JobSubmissionSnapshot(JobId(id), JobStatus.Open, 7L))
    )
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("version", Int.box(7))).isInvalid)
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("status", "Closed")).isValid)
  }

  test("fail closed for missing fields, invalid UUID/status and malformed revisions") {
    List("_id", "status", "version").foreach { field =>
      val document = valid
      val _ = document.remove(field)
      assert(MongoJobSubmissionSnapshotCodec.read(document).isInvalid)
    }
    List[AnyRef](Long.box(-1L), Double.box(7d), "7", null).foreach { value =>
      assert(MongoJobSubmissionSnapshotCodec.read(valid.append("version", value)).isInvalid)
    }
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("_id", "1-1-1-1-1")).isInvalid)
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("status", "Unknown")).isInvalid)
  }

  test("reject accidental aggregate hydration at the projection boundary") {
    assert(MongoJobSubmissionSnapshotCodec.read(valid.append("embedding", List(1f))).isInvalid)
  }
}
