package com.example.hiring.analytics.cli

import com.example.hiring.analytics.domain.PartitionOffsetRange
import org.bson.Document

class PostAuthorizationHmacControlEventSpec extends munit.FunSuite {
  private val expected = PartitionOffsetRange
    .from("hiring.hmac.rotation.test.0123456789abcdef", 0, 7L, 8L)
    .toEither
    .fold(errors => fail(errors.toString), identity)
  private def row = new Document("topic", "hiring.hmac.rotation.test.0123456789abcdef")
    .append("partition", Int.box(0))
    .append("startOffset", Long.box(9L))
    .append("endOffsetExclusive", Long.box(10L))

  test("retained synthetic restart control preserves its exact single-offset coordinate") {
    val decoded = PostAuthorizationHmacControlEventMain.coordinate(row, expected)
    assertEquals(decoded.map(_.startOffset: Long), Right(9L))
    assertEquals(decoded.map(_.endOffsetExclusive: Long), Right(10L))
  }

  test("missing malformed cross-source or multi-offset restart evidence fails closed") {
    val cases = Vector(
      row.append("topic", "other.topic"),
      row.append("partition", Int.box(1)),
      row.append("partition", Long.box(0L)),
      row.append("startOffset", Int.box(9)),
      row.append("startOffset", Long.box(-1L)),
      row.append("endOffsetExclusive", Long.box(9L)),
      row.append("endOffsetExclusive", Long.box(11L)),
      row.append("endOffsetExclusive", Long.box(Long.MinValue)),
      row.append("topic", null),
      row.append("partition", null),
      row.append("startOffset", null),
      row.append("endOffsetExclusive", null)
    )
    cases.foreach(value => assert(PostAuthorizationHmacControlEventMain.coordinate(value, expected).isLeft))
  }
}
