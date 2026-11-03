package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, TransactionOperation}
import com.mongodb.{MongoCommandException, ServerAddress}
import munit.CatsEffectSuite
import org.bson.BsonDocument

/** An exhausted transaction names its operation. Every `MongoTransactionRunner.run` caller must pass a
  * `TransactionOperation`: the parameter has no default, so an unnamed caller does not compile.
  */
final class TransactionExhaustionReportSpec extends CatsEffectSuite {
  private val exhausted = new MongoCommandException(new BsonDocument(), new ServerAddress())

  private def emitted(operation: TransactionOperation): IO[Vector[(LogEvent, Map[LogField, String])]] =
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics[IO] {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          events.update(_ :+ (event -> fields))
      }
      _ <- MongoRepositorySupport.reportExhausted(diagnostics, exhausted, operation)
      result <- events.get
    } yield result

  test("an exhausted transaction line names its operation and emits no application write conflict line") {
    emitted(TransactionOperation.OutboxClaimBatch).map { events =>
      assertEquals(events.map(_._1), Vector(LogEvent.MongoRepositoryFailed))
      val fields = events.head._2
      assertEquals(fields.get(LogField.SpanName), Some("transaction.exhausted"))
      assertEquals(fields.get(LogField.ConflictSource), Some("transaction-exhausted"))
      assertEquals(fields.get(LogField.TransactionOperation), Some("outbox.claimBatch"))
    }
  }

  test("only application and receipt operations also emit the application write conflict line") {
    TransactionOperation.values.toList.foldLeft(IO.unit) { (done, operation) =>
      done *> emitted(operation).map { events =>
        val applicationWrite = operation == TransactionOperation.ApplicationWrite ||
          operation == TransactionOperation.MutationReceiptWrite
        assertEquals(events.map(_._1).contains(LogEvent.ApplicationWriteConflict), applicationWrite, clue(operation))
        assertEquals(events.last._2.get(LogField.TransactionOperation), Some(operation.label))
        if (applicationWrite) assertEquals(events.head._2.get(LogField.SpanName), Some(operation.label))
      }
    }
  }
}
