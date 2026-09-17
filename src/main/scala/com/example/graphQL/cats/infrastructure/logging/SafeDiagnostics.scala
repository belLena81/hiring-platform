package com.example.graphQL.cats.infrastructure.logging

import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import io.circe.Json
import java.nio.charset.StandardCharsets
import org.slf4j.{LoggerFactory, MarkerFactory}

object SafeDiagnostics {
  private val loggerName = "hiring.foundation"

  def configure(level: String, maskSensitive: Boolean = true): IO[Diagnostics] =
    IO.delay(apply(level, maskSensitive)).flatTap { diagnostics =>
      if (maskSensitive) IO.unit
      else Diagnostics.emit(diagnostics, LogEvent.LocalUnmasked)
    }

  def apply(level: String, maskSensitive: Boolean = true): Diagnostics = {
    val logger = LoggerFactory.getLogger(loggerName)
    withEventSink(level, maskSensitive, (event, message) => IO.blocking {
      val marker = MarkerFactory.getMarker(event.marker)
      event.severity match {
        case "ERROR" => logger.error(marker, message)
        case "WARN" => logger.warn(marker, message)
        case _ => logger.info(marker, message)
      }
    })
  }

  private[logging] def withSink(level: String, sink: String => IO[Unit], maskSensitive: Boolean = true): Diagnostics =
    withEventSink(level, maskSensitive, (_, message) => sink(message))

  private def bounded(value: String, limit: Int): String = {
    val count = value.codePointCount(0, value.length)
    val clipped = count > limit
    val end = value.offsetByCodePoints(0, if (clipped) limit - 1 else count)
    val normalized = value.substring(0, end).map { character =>
      if (character.isControl || Character.getType(character) == Character.FORMAT || character == '\u2028' || character == '\u2029') ' '
      else character
    }
    if (clipped) normalized + "…" else normalized
  }

  private def withEventSink(level: String, maskSensitive: Boolean, sink: (LogEvent, String) => IO[Unit]): Diagnostics = new Diagnostics {
    private val threshold = level match {
      case "TRACE" | "DEBUG" => 0
      case "INFO" => 0
      case "WARN" => 1
      case _ => 2
    }

    def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] = IO.defer {
      val safetyWarning = event == LogEvent.LocalUnmasked
      if (event.rank < threshold && !safetyWarning) IO.unit
      else IO.realTimeInstant.flatMap { timestamp =>
        val safeId = requestId.filter(_.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
        val details = fields.toList.sortBy(_._1.ordinal).take(12).map { case (field, value) =>
            val rendered =
              if (field.sensitive && maskSensitive) "[REDACTED]"
              else if (field.sensitive || LogFields.validPublic(field, value)) bounded(value, 128)
              else "[FILTERED]"
            field.key -> Json.fromString(rendered)
          }
        val record = Json.obj(
          "timestamp" -> Json.fromString(timestamp.toString),
          "severity" -> Json.fromString(event.severity),
          "category" -> Json.fromString(event.category),
          "requestId" -> safeId.fold(Json.Null)(Json.fromString),
          "marker" -> Json.fromString(event.marker),
          "component" -> Json.fromString(event.component),
          "message" -> Json.fromString(event.message),
          "masking" -> Json.fromString(if (maskSensitive) "enabled" else "disabled-local"),
          "details" -> Json.obj(details*)
        )
        val encoded = record.noSpaces
        val line = if (encoded.getBytes(StandardCharsets.UTF_8).length <= 8192) encoded
          else record.mapObject(_.add("details", Json.obj())).noSpaces
        sink(event, line)
      }
    }.handleError(_ => ())
  }
}
