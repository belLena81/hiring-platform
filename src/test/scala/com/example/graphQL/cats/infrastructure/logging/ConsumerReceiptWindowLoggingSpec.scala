package com.example.graphQL.cats.infrastructure.logging

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.{LogEvent, LogField}
import io.circe.parser.parse
import munit.CatsEffectSuite

/** DDC-02.2: the refusal passes the real diagnostics sink with its numbers and reason; non-numeric values stay
  * filtered.
  */
final class ConsumerReceiptWindowLoggingSpec extends CatsEffectSuite {
  private def render(fields: Map[LogField, String]): IO[Map[String, String]] =
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- SafeDiagnostics
        .withSink(line => emitted.update(_ :+ line))
        .event(LogEvent.ConsumerReceiptWindowRefused, fields = fields)
      lines <- emitted.get
    } yield parse(lines.head).toOption
      .flatMap(_.hcursor.downField("details").as[Map[String, String]].toOption)
      .getOrElse(Map.empty)

  test("DDC-02.2 the sink renders the three durations (including -1) and the refusal label") {
    render(
      Map(
        LogField.RetentionMs -> "-1",
        LogField.SegmentMs -> "86400000",
        LogField.ReceiptTtlMs -> "691200000",
        LogField.WindowRefusal -> "RetentionUnbounded"
      )
    ).map { details =>
      assertEquals(details.get("retentionMs"), Some("-1"))
      assertEquals(details.get("segmentMs"), Some("86400000"))
      assertEquals(details.get("receiptTtlMs"), Some("691200000"))
      assertEquals(details.get("windowRefusal"), Some("RetentionUnbounded"))
    }
  }

  test("DDC-02.2 non-numeric durations and unknown refusal labels are still filtered") {
    render(
      Map(
        LogField.RetentionMs -> "seven days",
        LogField.SegmentMs -> "-2",
        LogField.ReceiptTtlMs -> "1; DROP",
        LogField.WindowRefusal -> "secret text"
      )
    ).map { details =>
      assertEquals(
        details.view.filterKeys(Set("retentionMs", "segmentMs", "receiptTtlMs", "windowRefusal")).values.toSet,
        Set("[FILTERED]")
      )
    }
  }
}
