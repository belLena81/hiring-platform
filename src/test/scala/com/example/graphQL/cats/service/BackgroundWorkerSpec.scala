package com.example.graphQL.cats.service

import cats.effect.{Deferred, IO, Ref}
import munit.CatsEffectSuite

final class BackgroundWorkerSpec extends CatsEffectSuite {
  private final class Recording(events: Ref[IO, Vector[(LogEvent, Map[LogField, String])]]) extends Diagnostics {
    override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
      events.update(_ :+ (event -> fields))
  }

  test("an errored worker is reported once with its name and sanitized failure fields") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      failed <- Deferred[IO, Unit]
      worker = IO
        .raiseError[Unit](new IllegalStateException("private worker detail"))
        .guarantee(failed.complete(()).void)
      _ <- BackgroundWorker.resource("outbox-publisher", new Recording(events))(worker).use(_ => failed.get)
      recorded <- events.get
    } yield {
      assertEquals(recorded.map(_._1), Vector(LogEvent.RuntimeFailed))
      assertEquals(recorded.head._2.get(LogField.Worker), Some("outbox-publisher"))
      assertEquals(recorded.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assert(!recorded.head._2.values.exists(_.contains("private worker detail")))
    }
  }

  test("release cancels a running worker and reports nothing") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      started <- Deferred[IO, Unit]
      cancelled <- Deferred[IO, Unit]
      worker = (started.complete(()).void *> IO.never[Unit]).onCancel(cancelled.complete(()).void)
      _ <- BackgroundWorker.resource("search-session-handoff-1", new Recording(events))(worker).use(_ => started.get)
      _ <- cancelled.get
      recorded <- events.get
    } yield assertEquals(recorded, Vector.empty)
  }

  test("an orderly completion is silent and the worker name is a public diagnostic field") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      done <- Deferred[IO, Unit]
      _ <- BackgroundWorker.resource("mongo-setup", new Recording(events))(done.complete(()).void).use(_ => done.get)
      recorded <- events.get
    } yield {
      assertEquals(recorded, Vector.empty)
      assert(LogFields.validPublic(LogField.Worker, "mongo-setup"))
      assert(!LogFields.validPublic(LogField.Worker, "mongo setup"))
    }
  }
}
