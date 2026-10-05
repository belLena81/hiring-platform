package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLateFactReplayOutcome
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*
import org.bson.Document
import scala.jdk.CollectionConverters.*

final class StreamingAdmissionProofSpec extends CatsEffectSuite {
  test("cancellable producer acquisition stops at the preparation deadline and releases partial ownership once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        actions <- Ref.of[IO, Int](0)
        partial = Resource.make(IO.unit)(_ => releases.update(_ + 1))
        owned = partial.flatMap(_ => Resource.eval(IO.never[Unit]))
        result <- HiringAnalyticsStreamingScenariosMain
          .ownedSuppressionProof(owned, actions.update(_ + 1))((_, _) => IO.unit)
          .attempt
        elapsed <- IO.monotonic
        count <- releases.get
        invoked <- actions.get
      } yield {
        assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
        assertEquals(count, 1)
        assertEquals(invoked, 0)
        assertEquals(elapsed, 120.seconds)
      }
    }
  }

  test("suppression preparation timeout releases its acquired owner exactly once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        owned = Resource.make(IO.unit)(_ => releases.update(_ + 1))
        result <- HiringAnalyticsStreamingScenariosMain
          .ownedSuppressionProof(owned, IO.never[Unit])((_, _) => IO.unit)
          .attempt
        elapsed <- IO.monotonic
        count <- releases.get
      } yield {
        assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
        assertEquals(elapsed, 120.seconds)
        assertEquals(count, 1)
      }
    }
  }

  test("late producer acquisition rejects before deletion preparation and releases once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        actions <- Ref.of[IO, Int](0)
        owned = Resource.make(IO.sleep(121.seconds))(_ => releases.update(_ + 1))
        result <- HiringAnalyticsStreamingScenariosMain
          .ownedSuppressionProof(owned, actions.update(_ + 1))((_, _) => IO.unit)
          .attempt
        count <- releases.get
        invoked <- actions.get
      } yield {
        assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
        assertEquals(invoked, 0)
        assertEquals(count, 1)
      }
    }
  }

  test("successful suppression preparation retains ownership through verification and releases once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        owned = Resource.make(IO.pure("prepared"))(_ => releases.update(_ + 1))
        _ <- HiringAnalyticsStreamingScenariosMain.ownedSuppressionProof(owned, IO.pure("baseline")) {
          (value, baseline) =>
            releases.get.flatMap(count =>
              IO {
                assertEquals(value, "prepared")
                assertEquals(baseline, "baseline")
                assertEquals(count, 0)
              }
            )
        }
        after <- releases.get
      } yield assertEquals(after, 1)
    }
  }

  test("suppression operation timeout releases its producer exactly once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        owned = Resource.make(IO.unit)(_ => releases.update(_ + 1))
        result <- HiringAnalyticsStreamingScenariosMain
          .ownedSuppressionProof(owned, IO.unit)((_, _) => IO.never[Unit])
          .attempt
        elapsed <- IO.monotonic
        count <- releases.get
      } yield {
        assert(result.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
        assertEquals(elapsed, 300.seconds)
        assertEquals(count, 1)
      }
    }
  }

  test("cancellation at the prepared operation boundary joins producer cleanup once") {
    TestControl.executeEmbed {
      for {
        releases <- Ref.of[IO, Int](0)
        verifying <- Deferred[IO, Unit]
        owned = Resource.make(IO.unit)(_ => releases.update(_ + 1))
        proof <- HiringAnalyticsStreamingScenariosMain
          .ownedSuppressionProof(owned, IO.unit)((_, _) => verifying.complete(()).void *> IO.never[Unit])
          .start
        _ <- verifying.get
        _ <- proof.cancel
        count <- releases.get
      } yield assertEquals(count, 1)
    }
  }

  test("native worker ownership start ticks preserve the supervisor string contract") {
    val native = Document.parse("""{"pid":1234,"uid":1000,"startTicks":"345678901"}""")
    assertEquals(StreamingAdmissionProof.workerStartTicks(native), Some(345678901L))
    assertEquals(
      StreamingAdmissionProof.workerStartTicks(new Document("startTicks", Long.MaxValue.toString)),
      Some(Long.MaxValue)
    )
    val invalid: Vector[AnyRef] = Vector(
      java.lang.Long.valueOf(345678901L),
      "0",
      "-1",
      "+1",
      "01",
      " 1",
      "1 ",
      "1.0",
      "1e3",
      "9223372036854775808",
      "999999999999999999999999",
      "secret-invalid"
    )
    invalid.foreach(value =>
      assertEquals(StreamingAdmissionProof.workerStartTicks(new Document("startTicks", value)), None)
    )
    assertEquals(StreamingAdmissionProof.workerStartTicks(new Document()), None)
    assertEquals(StreamingAdmissionProof.workerStartTicks(new Document("startTicks", null)), None)
  }

  test("worker overlap requires postcommit revision advancement under one stable mutex owner") {
    import StreamingAdmissionProof.*
    val before = OwnedRevision("owned", java.time.Instant.parse("2026-10-05T10:00:00Z"), 1L, true)
    val after = before.copy(revision = 2L)
    assert(workerOwnedAfterCommit(Some(before), Some(after)))
    val rejected = Vector(
      before,
      after.copy(ownerToken = "other"),
      after.copy(acquiredAt = before.acquiredAt.plusSeconds(1)),
      after.copy(afterCommit = false),
      after.copy(ownerToken = ""),
      after.copy(revision = -1L)
    )
    rejected.foreach(value => assert(!workerOwnedAfterCommit(Some(before), Some(value))))
    assert(!workerOwnedAfterCommit(Some(before.copy(afterCommit = false)), Some(after)))
    assert(!workerOwnedAfterCommit(Some(before.copy(revision = -1L)), Some(after)))
    assert(!workerOwnedAfterCommit(None, Some(after)))
    assert(!workerOwnedAfterCommit(Some(before), None))
  }

  test("fresh maintenance success preserves the five minute native maximum") {
    def line(
        outcome: String = "Succeeded",
        maximum: String = "300000",
        elapsed: String = "0",
        at: String = "2026-10-05T10:00:00Z",
        deferrals: String = "0"
    ) =
      s"INFO STREAMING_MAINTENANCE outcome=$outcome elapsedSinceSuccessMillis=$elapsed maximumElapsedSinceSuccessMillis=$maximum consecutiveDeferrals=$deferrals lastSuccess=$at"
    assert(StreamingAdmissionProof.boundedMaintenanceSuccess(line()))
    Vector(
      line(outcome = "Started"),
      line(maximum = "300001"),
      line(maximum = "-1"),
      line(elapsed = "300001"),
      line(at = "none"),
      line(deferrals = "1"),
      line(maximum = "999999999999999999999999999")
    ).foreach(value => assert(!StreamingAdmissionProof.boundedMaintenanceSuccess(value)))
  }

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
