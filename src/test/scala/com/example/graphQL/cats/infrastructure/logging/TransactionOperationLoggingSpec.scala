package com.example.graphQL.cats.infrastructure.logging

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.{LogEvent, LogField, LogFields, TransactionOperation}
import io.circe.parser.parse
import munit.CatsEffectSuite

/** The name of an exhausted transaction passes the real diagnostics sink: a registered operation is rendered, anything
  * else is filtered. The registry is the allow-list, so a new caller cannot log an unlisted name.
  */
final class TransactionOperationLoggingSpec extends CatsEffectSuite {
  private def text(line: String, key: String): Option[String] =
    parse(line).toOption.flatMap(_.hcursor.downField("details").get[String](key).toOption)

  test("the sink renders every registered transaction operation and filters an unregistered one") {
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink(line => emitted.update(_ :+ line))
      operations = TransactionOperation.values.toList
      _ <- operations.foldLeft(IO.unit)((done, operation) =>
        done *> diagnostics.event(
          LogEvent.MongoRepositoryFailed,
          fields = Map(LogField.TransactionOperation -> operation.label)
        )
      )
      _ <- diagnostics.event(
        LogEvent.MongoRepositoryFailed,
        fields = Map(LogField.TransactionOperation -> "outbox.somethingNew")
      )
      lines <- emitted.get
    } yield {
      assertEquals(lines.size, operations.size + 1)
      operations.zip(lines).foreach { case (operation, line) =>
        assertEquals(text(line, "transactionOperation"), Some(operation.label))
      }
      assertEquals(text(lines.last, "transactionOperation"), Some("[FILTERED]"))
    }
  }

  test("operation labels are unique, dotted business names and the allow-list is exactly the registry") {
    val labels = TransactionOperation.values.toList.map(_.label)
    assertEquals(labels.distinct, labels)
    assertEquals(LogFields.TransactionOperationLabels, labels.toSet)
    labels.foreach(label => assert(label.matches("[a-z][A-Za-z]*\\.[a-z][A-Za-z]*"), clue(label)))
  }
}
