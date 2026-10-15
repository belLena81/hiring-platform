package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.mongodb.MongoException
import munit.CatsEffectSuite

final class MongoRepositoryGuardSpec extends CatsEffectSuite {
  private def transient: MongoException = {
    val error = new MongoException("write conflict")
    error.addLabel("TransientTransactionError")
    error
  }

  private def recording: IO[(Diagnostics[IO], IO[Int])] =
    Ref.of[IO, Int](0).map { count =>
      val diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          count.update(_ + 1)
      }
      diagnostics -> count.get
    }

  test("guard re-raises a transient-labelled MongoException unmapped and unlogged") {
    recording.flatMap { (diagnostics, logged) =>
      val error = transient
      for {
        raised <- MongoRepositorySupport.guard(diagnostics, "op")(IO.raiseError[Int](error))(_ => -1).attempt
        entries <- logged
      } yield {
        assertEquals(raised.left.toOption.map(_ eq error), Some(true))
        assertEquals(entries, 0)
      }
    }
  }

  test("guard maps and logs any other failure, and passes a success through") {
    recording.flatMap { (diagnostics, logged) =>
      for {
        mapped <- MongoRepositorySupport.guard(diagnostics, "op")(IO.raiseError[Int](new MongoException("other")))(_ =>
          -1
        )
        plain <- MongoRepositorySupport.guard(diagnostics, "op")(IO.pure(7))(_ => -1)
        entries <- logged
      } yield {
        assertEquals((mapped, plain), (-1, 7))
        assertEquals(entries, 1)
      }
    }
  }

  test("guardCleanup swallows every failure, transient included, and logs it") {
    recording.flatMap { (diagnostics, logged) =>
      for {
        _ <- MongoRepositorySupport.guardCleanup(diagnostics, "cleanup")(IO.raiseError(transient))
        _ <- MongoRepositorySupport.guardCleanup(diagnostics, "cleanup")(IO.raiseError(new IllegalStateException("x")))
        entries <- logged
      } yield assertEquals(entries, 2)
    }
  }
}
