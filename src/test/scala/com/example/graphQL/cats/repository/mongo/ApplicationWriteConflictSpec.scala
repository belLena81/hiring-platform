package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogLevel, RepositoryError}
import munit.CatsEffectSuite

import java.util.UUID

/** The conflict source is diagnostics only: the repository error stays `Conflict`, and the logged fields carry the
  * source, the operation name and opaque UUIDs, nothing else.
  */
final class ApplicationWriteConflictSpec extends CatsEffectSuite {
  private val entity = UUID.fromString("00000000-0000-0000-0000-000000000001")
  private val actor = UUID.fromString("00000000-0000-0000-0000-000000000002")

  private def capture: IO[(Diagnostics[IO], IO[List[(LogEvent, Map[LogField, String])]])] =
    Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil).map { captured =>
      val diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          captured.update((event, fields) :: _)
      }
      (diagnostics, captured.get)
    }

  test("the source labels are the documented, distinct categories") {
    assertEquals(
      ConflictSource.values.toList.map(_.label),
      List(
        "receipt-fingerprint",
        "receipt-duplicate-key",
        "receipt-completion-no-match",
        "fence-duplicate-key",
        "fence-write-conflict",
        "status-write-conflict",
        "status-guard-no-match",
        "history-duplicate-key",
        "transaction-exhausted"
      )
    )
  }

  test("the service-layer allow-list accepts exactly the documented sources") {
    assertEquals(
      ConflictSource.values.map(_.label).toSet,
      com.example.graphQL.cats.service.LogFields.ConflictSourceLabels
    )
  }

  test("the event is a WARN with a stable category and the fields carry only source, operation and UUIDs") {
    assertEquals(LogEvent.ApplicationWriteConflict.category, "APPLICATION_WRITE_CONFLICT")
    assertEquals(LogEvent.ApplicationWriteConflict.level, LogLevel.Warn)
    val fields =
      ApplicationWriteConflict.fields(
        ConflictSource.FenceDuplicateKey,
        "applications.statusFence",
        Some(entity),
        Some(actor)
      )
    assertEquals(
      fields,
      Map(
        LogField.ConflictSource -> "fence-duplicate-key",
        LogField.SpanName -> "applications.statusFence",
        LogField.EntityId -> entity.toString,
        LogField.ActorId -> actor.toString
      )
    )
    assert(LogField.EntityId.sensitive && LogField.ActorId.sensitive, "UUIDs are masked unless unmasking is configured")
  }

  test("a refused step is recorded with its source and still returns Conflict") {
    for {
      (diagnostics, logged) <- capture
      result <- ApplicationWriteConflict
        .onConflict(diagnostics, ConflictSource.StatusGuardNoMatch, "applications.statusGuard", Some(entity))(
          RepositoryF.raise[IO, Unit](RepositoryError.Conflict)
        )
        .value
      entries <- logged
    } yield {
      assertEquals(result, Left(RepositoryError.Conflict))
      assertEquals(entries.map(_._1), List(LogEvent.ApplicationWriteConflict))
      assertEquals(entries.head._2.get(LogField.ConflictSource), Some("status-guard-no-match"))
    }
  }

  test("other outcomes are not recorded and pass through unchanged") {
    for {
      (diagnostics, logged) <- capture
      revoked <- ApplicationWriteConflict
        .onConflict(diagnostics, ConflictSource.StatusGuardNoMatch, "applications.statusGuard")(
          RepositoryF.raise[IO, Unit](RepositoryError.AuthorityRevoked)
        )
        .value
      ok <- ApplicationWriteConflict
        .onConflict(diagnostics, ConflictSource.StatusGuardNoMatch, "applications.statusGuard")(
          RepositoryF.pure[IO, Int](1)
        )
        .value
      entries <- logged
    } yield {
      assertEquals(revoked, Left(RepositoryError.AuthorityRevoked))
      assertEquals(ok, Right(1))
      assertEquals(entries, Nil)
    }
  }

  test("a non-transient failure is not recorded as a write conflict and is re-raised") {
    for {
      (diagnostics, logged) <- capture
      result <- ApplicationWriteConflict
        .onTransient(diagnostics, ConflictSource.FenceWriteConflict, "applications.statusFence")(
          RepositoryF.lift[IO, Unit](IO.raiseError(new IllegalStateException("boom")))
        )
        .value
        .attempt
      entries <- logged
    } yield {
      assert(result.isLeft)
      assertEquals(entries, Nil)
    }
  }

  test("a transient failure is recorded once and re-raised unchanged for the runner to retry") {
    val transient = new com.mongodb.MongoException("conflict")
    transient.addLabel("TransientTransactionError")
    for {
      (diagnostics, logged) <- capture
      result <- ApplicationWriteConflict
        .onTransient(diagnostics, ConflictSource.FenceWriteConflict, "applications.statusFence")(
          RepositoryF.lift[IO, Unit](IO.raiseError(transient))
        )
        .value
        .attempt
      entries <- logged
    } yield {
      assertEquals(result.left.toOption.map(_ eq transient), Some(true))
      assertEquals(entries.flatMap(_._2.get(LogField.ConflictSource)), List("fence-write-conflict"))
    }
  }
}
