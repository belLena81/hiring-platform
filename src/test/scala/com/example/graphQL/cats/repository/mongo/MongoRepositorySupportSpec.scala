package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.{Deferred, Outcome, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType
}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import io.circe.Json
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID

final class MongoRepositorySupportSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-22T12:00:00Z")
  private val actor = UserId(new UUID(0L, 1L))

  private def jobEvent(number: Long): OperationalEventEnvelope =
    OperationalEventEnvelope(
      new UUID(0L, number),
      OperationalEventType.JOB_CREATED,
      now,
      OperationalAggregateType.Job,
      new UUID(1L, number).toString,
      actor,
      Json.obj("jobId" -> Json.fromString(new UUID(1L, number).toString))
    )

  test("repository guard logs safe failure metadata before returning its typed error") {
    for {
      captured <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(
            event: LogEvent,
            requestId: Option[String],
            fields: => Map[LogField, String]
        ): IO[Unit] = captured.update((event, fields) :: _)
      }
      result <- MongoRepositorySupport
        .repositoryGuard[Unit](diagnostics, "job.find")(
          RepositoryIO.lift(IO.raiseError(new IllegalStateException("sensitive mongo detail")))
        )(_ => Left(RepositoryError.Unavailable))
        .value
      entries <- captured.get
    } yield {
      assertEquals(result, Left(RepositoryError.Unavailable))
      assertEquals(entries.size, 1)
      assertEquals(entries.head._1, LogEvent.MongoRepositoryFailed)
      assertEquals(entries.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assertEquals(entries.head._2.get(LogField.SpanName), Some("job.find"))
      assert(!entries.head._2.values.exists(_.contains("sensitive mongo detail")))
    }
  }

  test("a diagnostics sink failure does not replace the repository error") {
    val diagnostics = new Diagnostics {
      override def event(
          event: LogEvent,
          requestId: Option[String],
          fields: => Map[LogField, String]
      ): IO[Unit] = IO.raiseError(new RuntimeException("sink unavailable"))
    }

    MongoRepositorySupport
      .repositoryGuard[Unit](diagnostics, "job.find")(
        RepositoryIO.lift(IO.raiseError(new IllegalArgumentException("driver failure")))
      )(_ => Left(RepositoryError.MissingWriteResult))
      .value
      .map(result => assertEquals(result, Left(RepositoryError.MissingWriteResult)))
  }

  test("RepositoryIO wraps the expected repository failure channel") {
    RepositoryIO.fromEither[Unit](Left(RepositoryError.InvalidStoredData)).value.map { result =>
      assertEquals(result, Left(RepositoryError.InvalidStoredData))
    }
  }

  test("a missing write result has its own repository error") {
    assertEquals(MongoRepositorySupport.writeResult(None), Left(RepositoryError.MissingWriteResult))
    assertEquals(MongoRepositorySupport.writeResult(Some(1)), Right(1))
  }

  test("invalid middle event stops later outbox inserts") {
    val first = jobEvent(1L)
    val invalid = jobEvent(2L).copy(
      eventType = OperationalEventType.APPLICATION_CREATED,
      aggregateType = OperationalAggregateType.Application
    )
    val later = jobEvent(3L)
    for {
      written <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- MongoOperationalEventInsertion
        .insertSequence(List(first, invalid, later), now)(document =>
          RepositoryIO.lift(written.update(_ :+ document.getString(MongoFields.Id)))
        )
        .value
      ids <- written.get
    } yield {
      assertEquals(result, Left(RepositoryError.InvalidStoredData))
      assertEquals(ids, Vector(first.eventId.toString))
    }
  }

  test("failed middle outbox insert stops later inserts") {
    val second = jobEvent(2L)
    val events = List(jobEvent(1L), second, jobEvent(3L))
    for {
      written <- Ref.of[IO, Vector[String]](Vector.empty)
      result <- MongoOperationalEventInsertion
        .insertSequence(events, now)(document =>
          RepositoryIO.lift(written.update(_ :+ document.getString(MongoFields.Id))).subflatMap { _ =>
            if (document.getString(MongoFields.Id) == second.eventId.toString)
              Left(RepositoryError.MissingWriteResult)
            else Right(())
          }
        )
        .value
      ids <- written.get
    } yield {
      assertEquals(result, Left(RepositoryError.MissingWriteResult))
      assertEquals(ids, events.take(2).map(_.eventId.toString).toVector)
    }
  }

  test("outbox insertion preserves successful event order and skips empty input") {
    val events = List(jobEvent(3L), jobEvent(1L), jobEvent(2L))
    for {
      written <- Ref.of[IO, Vector[String]](Vector.empty)
      empty <- MongoOperationalEventInsertion
        .insertSequence(Nil, now)(_ =>
          RepositoryIO.lift(IO.raiseError(new IllegalStateException("empty input must not write")))
        )
        .value
      result <- MongoOperationalEventInsertion
        .insertSequence(events, now)(document =>
          RepositoryIO.lift(written.update(_ :+ document.getString(MongoFields.Id)))
        )
        .value
      ids <- written.get
    } yield {
      assertEquals(empty, Right(()))
      assertEquals(result, Right(()))
      assertEquals(ids, events.map(_.eventId.toString).toVector)
    }
  }

  test("repository guard preserves typed failures without reporting a driver failure") {
    for {
      reports <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          reports.update(_ + 1)
      }
      result <- MongoRepositorySupport
        .repositoryGuard(diagnostics, "typedFailure")(
          RepositoryIO.fromEither[Unit](Left(RepositoryError.InvalidStoredData))
        )(_ => Left(RepositoryError.Unavailable))
        .value
      count <- reports.get
    } yield {
      assertEquals(result, Left(RepositoryError.InvalidStoredData))
      assertEquals(count, 0)
    }
  }

  test("repository guard preserves cancellation and runs the underlying finalizer") {
    for {
      started <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      reports <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          reports.update(_ + 1)
      }
      fiber <- MongoRepositorySupport
        .repositoryGuard(diagnostics, "cancelledWrite")(
          RepositoryIO.lift((started.complete(()) *> IO.never[Unit]).guarantee(released.set(true)))
        )(_ => Left(RepositoryError.Unavailable))
        .value
        .start
      _ <- started.get
      _ <- fiber.cancel
      outcome <- fiber.join
      didRelease <- released.get
      count <- reports.get
    } yield {
      assert(outcome match { case Outcome.Canceled() => true; case _ => false })
      assert(didRelease)
      assertEquals(count, 0)
    }
  }

}
