package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Ref, Resource}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import munit.CatsEffectSuite

final class MongoTransactionCleanupSpec extends CatsEffectSuite {
  private def fixture = for {
    active <- Ref.of[IO, Boolean](false)
    aborts <- Ref.of[IO, Int](0)
    cleanup = MongoTransactionRunner.abortActiveTransaction(
      Diagnostics.noop,
      active.get,
      aborts.update(_ + 1) *> active.set(false)
    )
    transaction = Resource.make(active.set(true))(_ => cleanup)
  } yield (active, aborts, cleanup, transaction)

  test("successful commit clears transaction state before resource cleanup") {
    fixture.flatMap { case (active, aborts, _, transaction) =>
      transaction.use(_ => active.set(false)) *> aborts.get.map(assertEquals(_, 0))
    }
  }

  test("explicit rejection abort and resource cleanup abort once") {
    fixture.flatMap { case (_, aborts, cleanup, transaction) =>
      transaction.use(_ => cleanup.as(Left("rejected"))) *> aborts.get.map(assertEquals(_, 1))
    }
  }

  test("operation failure aborts the active transaction once") {
    fixture.flatMap { case (_, aborts, cleanup, transaction) =>
      val failure = new IllegalStateException("operation failure")
      for {
        result <- transaction.use(_ => IO.raiseError[Unit](failure).onError { case _ => cleanup }).attempt
        count <- aborts.get
      } yield {
        assertEquals(result, Left(failure))
        assertEquals(count, 1)
      }
    }
  }

  test("cancellation releases the active transaction once") {
    fixture.flatMap { case (active, aborts, _, transaction) =>
      for {
        entered <- Deferred[IO, Unit]
        fiber <- transaction.use(_ => entered.complete(()) *> IO.never[Unit]).start
        _ <- entered.get
        _ <- fiber.cancel
        count <- aborts.get
        stillActive <- active.get
      } yield {
        assertEquals(count, 1)
        assertEquals(stillActive, false)
      }
    }
  }

  test("reusing cleanup observes current transaction state each time") {
    fixture.flatMap { case (active, aborts, cleanup, _) =>
      cleanup *> active.set(true) *> cleanup *> cleanup *> active.set(true) *> cleanup *>
        aborts.get.map(assertEquals(_, 2))
    }
  }

  test("state inspection failure stays inside the sanitized abort guard") {
    for {
      entries <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          entries.update((event, fields) :: _)
      }
      aborts <- Ref.of[IO, Int](0)
      _ <- MongoTransactionRunner.abortActiveTransaction(
        diagnostics,
        IO.raiseError(new IllegalStateException("sensitive transaction detail")),
        aborts.update(_ + 1)
      )
      count <- aborts.get
      captured <- entries.get
    } yield {
      assertEquals(count, 0)
      assertEquals(captured.size, 1)
      assertEquals(captured.head._1, LogEvent.MongoRepositoryFailed)
      assertEquals(captured.head._2.get(LogField.SpanName), Some("transaction.abort"))
      assert(!captured.head._2.values.exists(_.contains("sensitive transaction detail")))
    }
  }
}
