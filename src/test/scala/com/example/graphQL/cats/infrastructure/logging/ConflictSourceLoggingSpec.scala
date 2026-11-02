package com.example.graphQL.cats.infrastructure.logging

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.{LogEvent, LogField, LogFields}
import io.circe.parser.parse
import munit.CatsEffectSuite

import java.util.UUID

/** The write-conflict source passes the real diagnostics sink: a documented label is rendered, anything else is
  * filtered, and the UUID fields stay masked.
  */
final class ConflictSourceLoggingSpec extends CatsEffectSuite {
  test("the sink renders every documented source and filters an undocumented one") {
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink(line => emitted.update(_ :+ line))
      sources = LogFields.ConflictSourceLabels.toList.sorted
      _ <- sources.foldLeft(IO.unit)((done, label) =>
        done *> diagnostics.event(
          LogEvent.ApplicationWriteConflict,
          fields = Map(
            LogField.ConflictSource -> label,
            LogField.SpanName -> "applications.statusFence",
            LogField.EntityId -> UUID.randomUUID().toString
          )
        )
      )
      _ <- diagnostics.event(
        LogEvent.ApplicationWriteConflict,
        fields = Map(LogField.ConflictSource -> "feedback text that must not be logged")
      )
      lines <- emitted.get
    } yield {
      val details = lines.map(line => parse(line).toOption.flatMap(_.hcursor.downField("details").focus))
      def text(index: Int, key: String): Option[String] = details(index).flatMap(_.hcursor.get[String](key).toOption)
      assertEquals(lines.size, sources.size + 1)
      sources.zipWithIndex.foreach { case (label, index) =>
        assertEquals(text(index, "conflictSource"), Some(label))
        assertEquals(text(index, "spanName"), Some("applications.statusFence"))
        assertEquals(text(index, "entityId"), Some("[REDACTED]"))
      }
      assertEquals(text(sources.size, "conflictSource"), Some("[FILTERED]"))
      assert(!lines.exists(_.contains("feedback text")))
    }
  }
}
