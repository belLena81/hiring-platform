package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLateFactReplayOutcome
import munit.FunSuite
import org.bson.Document
import scala.jdk.CollectionConverters.*

final class StreamingAdmissionProofSpec extends FunSuite {
  private def unavailable(code: String) = new Document(
    "errors",
    Vector(
      new Document("extensions", new Document("code", code))
    ).asJava
  ).append("data", new Document("analyticsReport", null))

  test("actual report observations distinguish published, temporarily hidden, and authorization failure") {
    val report = new Document("skillPostingActivity", Vector.empty[Document].asJava)
    assertEquals(
      StreamingAdmissionProof.report(new Document("data", new Document("analyticsReport", report))),
      Right(Some(report))
    )
    assertEquals(StreamingAdmissionProof.report(unavailable("ANALYTICS_UNAVAILABLE")), Right(None))
    assertEquals(StreamingAdmissionProof.report(unavailable("UNAUTHORIZED")), Left("REPORT_OBSERVATION_REJECTED"))
    assertEquals(StreamingAdmissionProof.report(unavailable("FORBIDDEN")), Left("REPORT_OBSERVATION_REJECTED"))
  }

  test("missing malformed or mixed hidden report states cannot become an accepted observation") {
    assertEquals(StreamingAdmissionProof.report(new Document()), Left("REPORT_SHAPE_INVALID"))
    assertEquals(
      StreamingAdmissionProof.report(new Document("errors", "secret-invalid-shape")),
      Left("REPORT_SHAPE_INVALID")
    )
    val mixed = unavailable("ANALYTICS_UNAVAILABLE")
    mixed.put(
      "data",
      new Document("analyticsReport", new Document("skillPostingActivity", Vector.empty[Document].asJava))
    )
    assertEquals(StreamingAdmissionProof.report(mixed), Left("REPORT_OBSERVATION_REJECTED"))
    val duplicate = unavailable("ANALYTICS_UNAVAILABLE")
    val errors = duplicate.getList("errors", classOf[Document])
    duplicate.put("errors", Vector(errors.get(0), errors.get(0)).asJava)
    assertEquals(StreamingAdmissionProof.report(duplicate), Left("REPORT_OBSERVATION_REJECTED"))
  }

  test("exact delivered evidence binds topic partition bounds and record count") {
    val offsets = (20L until 32L).toVector
    assert(StreamingAdmissionProof.exactDelivered("fixture", 0, 20L, 31L, 12L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("foreign", 0, 20L, 31L, 12L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 1, 20L, 31L, 12L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, 19L, 31L, 12L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, 20L, 32L, 12L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, 20L, 31L, 13L, "fixture", offsets))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, 0L, 0L, 0L, "fixture", Vector.empty))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, 1L, 1L, 2L, "fixture", Vector(1L, 1L)))
    assert(!StreamingAdmissionProof.exactDelivered("fixture", 0, -1L, -1L, 1L, "fixture", Vector(-1L)))
  }

  test("suppression evidence follows native store schemas") {
    assertEquals(StreamingAdmissionProof.evidenceColumns("silver"), Vector("eventId"))
    assertEquals(StreamingAdmissionProof.evidenceColumns("late"), Vector("eventId", "topic", "partition", "offset"))
    assertEquals(StreamingAdmissionProof.evidenceColumns("bronze"), Vector("topic", "partition", "offset"))
    assertEquals(StreamingAdmissionProof.evidenceColumns("quarantine"), Vector("topic", "partition", "offset"))
    intercept[IllegalArgumentException](StreamingAdmissionProof.evidenceColumns("foreign"))
  }

  test("safe diagnostic contains only allowlisted mode category and own source line") {
    val error = new IllegalStateException("credential=secret payload=private")
    error.setStackTrace(
      Array(
        new StackTraceElement(
          "com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain$",
          "secretMethod",
          "HiringAnalyticsStreamingScenariosMain.scala",
          321
        )
      )
    )
    assertEquals(
      StreamingAdmissionProof.failure(List("prepare"), error),
      "STREAMING_LIVE_SCENARIO_FAILED mode=prepare class=IllegalStateException ownLine=321"
    )
    assert(!StreamingAdmissionProof.failure(List("credential=secret"), error).contains("secret"))
    error.setStackTrace(Array(new StackTraceElement("foreign.Secret", "secret", "Secret.scala", 55)))
    assertEquals(
      StreamingAdmissionProof.failure(List("suppressed-only"), error),
      "STREAMING_LIVE_SCENARIO_FAILED mode=suppressed-only class=IllegalStateException ownLine=0"
    )
  }

  test("replay result diagnostic distinguishes all native outcomes without accepting publication") {
    Vector(
      AnalyticsLateFactReplayOutcome.Published -> "RIGHT_PUBLISHED",
      AnalyticsLateFactReplayOutcome.AlreadyPublished -> "RIGHT_ALREADY_PUBLISHED",
      AnalyticsLateFactReplayOutcome.ErasurePending -> "RIGHT_ERASURE_PENDING"
    ).foreach { case (outcome, category) =>
      assertEquals(
        StreamingAdmissionProof.replayResult(Right(outcome)),
        s"STREAMING_REPLAY_DELETION_RACE_RESULT category=$category"
      )
    }
  }

  test("replay result diagnostic classifies typed failures without exposing their values or causes") {
    val secret = new IllegalStateException("credential=secret payload=private")
    Vector[(Throwable, String)](
      AnalyticsError.LakehouseLockTimeout -> "LEFT_LOCK_TIMEOUT",
      AnalyticsError.LakehouseFailure(secret) -> "LEFT_LAKEHOUSE_FAILURE",
      AnalyticsError.SourceReadFailure(secret) -> "LEFT_SOURCE_READ_FAILURE",
      AnalyticsError.InvalidConfiguration("credential=secret") -> "LEFT_INVALID_CONFIGURATION",
      AnalyticsError.LateFactReplayRequestConflict -> "LEFT_REPLAY_CONFLICT",
      AnalyticsError.LateFactReplayRejected -> "LEFT_REPLAY_REJECTED",
      AnalyticsError.GuardedErasurePublicationRejected -> "LEFT_PUBLICATION_REJECTED",
      AnalyticsError.EmptyRequestedRange("private-topic", 7, 123L) -> "LEFT_OTHER_ANALYTICS"
    ).foreach { case (error, category) =>
      assertEquals(
        StreamingAdmissionProof.replayResult(Left(error)),
        s"STREAMING_REPLAY_DELETION_RACE_RESULT category=$category"
      )
    }
  }

  test("unexpected replay errors cannot expose messages runtime classes or cyclic causes") {
    val error = new RuntimeException("credential=secret payload=private") {
      override def getMessage: String = throw new AssertionError("message inspected")
      override def getCause: Throwable = this
      override def toString: String = throw new AssertionError("error rendered")
    }
    assertEquals(
      StreamingAdmissionProof.replayResult(Left(error)),
      "STREAMING_REPLAY_DELETION_RACE_RESULT category=LEFT_UNEXPECTED"
    )
  }

}
