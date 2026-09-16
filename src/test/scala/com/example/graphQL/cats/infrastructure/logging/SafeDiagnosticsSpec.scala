package com.example.graphQL.cats.infrastructure.logging

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.application.LogEvent
import com.example.graphQL.cats.config.AppConfig
import io.circe.parser.parse
import java.time.Instant
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory

class SafeDiagnosticsSpec extends CatsEffectSuite {
  test("P1-AC09 emitted logs are UTC JSON with safe categories and correlation") {
    val requestId = "fe211944-7015-4e73-8dc1-000000000001"
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line))
      _ <- LogEvent.values.toList.traverse_(event => diagnostics.event(event, Some(requestId)))
      lines <- emitted.get
    } yield {
      assertEquals(lines.size, LogEvent.values.length)
      lines.zip(LogEvent.values).foreach { case (line, event) =>
        val json = parse(line).toOption.getOrElse(fail("Invalid log JSON"))
        assertEquals(json.hcursor.get[String]("category"), Right(event.category))
        assertEquals(json.hcursor.get[String]("requestId"), Right(requestId))
        val timestamp = json.hcursor.get[String]("timestamp").toOption.getOrElse(fail("Missing timestamp"))
        assert(timestamp.endsWith("Z"))
        assert(Instant.parse(timestamp).isBefore(Instant.now().plusSeconds(1)))
        assertEquals(json.asObject.map(_.keys.toSet), Some(Set("timestamp", "severity", "category", "requestId")))
      }
    }
  }

  test("P1-AC09 URI and validation failures cannot inject secrets into diagnostics") {
    val secret = "mongodb://user:synthetic-secret@host:invalid\nforged-log"
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line))
      invalid <- IO(AppConfig.fromEnvironment(Map("MONGODB_URI" -> secret)))
      _ <- diagnostics.event(LogEvent.ConfigInvalid, Some(secret))
      _ <- diagnostics.event(LogEvent.RequestRejected, Some("validation failed: " + secret))
      _ <- diagnostics.event(LogEvent.MongoAuthFailed)
      lines <- emitted.get
    } yield {
      assert(invalid.isLeft)
      assert(!invalid.toString.contains("synthetic-secret"))
      assertEquals(lines.size, 3)
      lines.foreach { line =>
        assert(!line.contains("synthetic-secret"))
        assert(!line.contains("forged-log"))
        assertEquals(parse(line).map(_.hcursor.downField("requestId").focus), Right(Some(io.circe.Json.Null)))
      }
    }
  }

  test("P1-AC09 application thresholds suppress lower severity events") {
    List("INFO" -> 8, "WARN" -> 6, "ERROR" -> 3).traverse_ { case (level, count) =>
      for {
        emitted <- Ref.of[IO, Vector[String]](Vector.empty)
        diagnostics = SafeDiagnostics.withSink(level, line => emitted.update(_ :+ line))
        _ <- LogEvent.values.toList.traverse_(event => diagnostics.event(event))
        lines <- emitted.get
      } yield assertEquals(lines.size, count)
    }
  }

  test("P1-AC09 backend suppresses raw framework and driver emitters at every app level") {
    List("INFO", "WARN", "ERROR").traverse_ { level =>
      SafeDiagnostics.configure(level).flatMap { _ => IO {
        List("ROOT", "org.mongodb.driver", "org.http4s", "org.typelevel", "com.mongodb.ConnectionString").foreach { name =>
          val logger = LoggerFactory.getLogger(name)
          assert(!logger.isErrorEnabled, clues(name, level))
          assert(!logger.isWarnEnabled, clues(name, level))
          assert(!logger.isInfoEnabled, clues(name, level))
        }
        assert(LoggerFactory.getLogger("hiring.foundation").isInfoEnabled)
      }}
    }
  }
}
