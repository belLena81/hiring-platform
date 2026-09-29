package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import munit.CatsEffectSuite

final class MongoRepositorySupportSpec extends CatsEffectSuite {
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
          IO.raiseError(new IllegalStateException("sensitive mongo detail"))
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
        IO.raiseError(new IllegalArgumentException("driver failure"))
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
}
