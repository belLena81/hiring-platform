package com.example.graphQL.cats.infrastructure.logging

import cats.effect.IO
import com.example.graphQL.cats.application.{Diagnostics, LogEvent}
import io.circe.Json
import org.slf4j.LoggerFactory

object SafeDiagnostics {
  private val loggerName = "hiring.foundation"

  def configure(level: String): IO[Diagnostics] = IO.delay(apply(level))

  def apply(level: String): Diagnostics = {
    val logger = LoggerFactory.getLogger(loggerName)
    withSink(level, message => IO.delay(logger.info(message)))
  }

  private[logging] def withSink(level: String, sink: String => IO[Unit]): Diagnostics = new Diagnostics {
    private val threshold = level match {
      case "INFO" => 0
      case "WARN" => 1
      case _ => 2
    }

    def event(event: LogEvent, requestId: Option[String]): IO[Unit] = {
      val (severity, rank) = event match {
        case LogEvent.ConfigInvalid | LogEvent.StartupFailed | LogEvent.RuntimeFailed => ("ERROR", 2)
        case LogEvent.MongoUnavailable | LogEvent.MongoAuthFailed | LogEvent.RequestRejected => ("WARN", 1)
        case LogEvent.Started | LogEvent.Shutdown => ("INFO", 0)
      }
      if (rank < threshold) IO.unit
      else IO.realTimeInstant.flatMap { timestamp =>
        val safeId = requestId.filter(_.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
        sink(Json.obj(
          "timestamp" -> Json.fromString(timestamp.toString),
          "severity" -> Json.fromString(severity),
          "category" -> Json.fromString(event.category),
          "requestId" -> safeId.fold(Json.Null)(Json.fromString)
        ).noSpaces)
      }
    }
  }
}
