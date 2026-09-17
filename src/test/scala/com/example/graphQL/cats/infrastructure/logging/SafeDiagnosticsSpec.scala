package com.example.graphQL.cats.infrastructure.logging

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.application.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.config.AppConfig
import io.circe.parser.parse
import java.time.Instant
import munit.CatsEffectSuite
import org.slf4j.LoggerFactory
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class SafeDiagnosticsSpec extends CatsEffectSuite {
  test("LOG-01 actual SLF4J events carry the matching severity and marker") {
    val logger = LoggerFactory.getLogger("hiring.foundation").asInstanceOf[ch.qos.logback.classic.Logger]
    val requestId = "fe211944-7015-4e73-8dc1-000000000099"
    Resource.make(IO {
      val appender = new ch.qos.logback.core.read.ListAppender[ch.qos.logback.classic.spi.ILoggingEvent]()
      appender.start()
      logger.addAppender(appender)
      appender
    })(appender => IO {
      val _ = logger.detachAppender(appender)
      appender.stop()
    }).use { appender =>
      val events = List(LogEvent.Started, LogEvent.RequestRejected, LogEvent.RuntimeFailed)
      events.traverse_(event => SafeDiagnostics("INFO").event(event, Some(requestId))) *> IO {
        val captured = appender.list.asScala.toList.filter(_.getFormattedMessage.contains(requestId))
        assertEquals(captured.size, events.size)
        captured.zip(events).foreach { case (record, event) =>
          assertEquals(record.getLevel.toString, event.severity)
          assertEquals(record.getMarkerList.asScala.map(_.getName).toList, List(event.marker))
        }
      }
    }
  }

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
        assertEquals(json.hcursor.get[String]("marker"), Right(event.marker))
        assertEquals(json.hcursor.get[String]("component"), Right(event.component))
        assertEquals(json.hcursor.get[String]("message"), Right(event.message))
        assertEquals(json.hcursor.get[String]("severity"), Right(event.severity))
        assertEquals(json.hcursor.get[String]("masking"), Right("enabled"))
        val timestamp = json.hcursor.get[String]("timestamp").toOption.getOrElse(fail("Missing timestamp"))
        assert(timestamp.endsWith("Z"))
        assert(Instant.parse(timestamp).isBefore(Instant.now().plusSeconds(1)))
        assertEquals(json.asObject.map(_.keys.toSet), Some(Set("timestamp", "severity", "category", "requestId",
          "marker", "component", "message", "masking", "details")))
      }
    }
  }

  test("P1-AC09 URI and validation failures cannot inject secrets into diagnostics") {
    val secret = "mongodb://user:synthetic-secret@host:invalid\nforged-log"
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line))
      invalid <- IO(AppConfig.fromConfig(
        s"""http {
           |  host = "127.0.0.1"
           |  port = 8080
           |}
           |mongo {
           |  uri = "$secret"
           |  database = "hiring"
           |}
           |logging {
           |  level = "INFO"
           |  mask-sensitive = true
           |}
           |""".stripMargin,
        Map.empty
      ))
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
    List("TRACE" -> 13, "DEBUG" -> 13, "INFO" -> 13, "WARN" -> 8, "ERROR" -> 4).traverse_ { case (level, count) =>
      for {
        emitted <- Ref.of[IO, Vector[String]](Vector.empty)
        diagnostics = SafeDiagnostics.withSink(level, line => emitted.update(_ :+ line))
        _ <- LogEvent.values.toList.traverse_(event => diagnostics.event(event))
        lines <- emitted.get
      } yield assertEquals(lines.size, count)
    }
  }

  test("LOG-03/04 sensitive metadata is masked by default and explicitly visible locally") {
    List(true, false).traverse_ { masked =>
      for {
        emitted <- Ref.of[IO, Vector[String]](Vector.empty)
        diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line), maskSensitive = masked)
        _ <- diagnostics.event(LogEvent.MongoProbeFailed, fields = Map(
          LogField.MongoHosts -> "local-db:27017", LogField.MongoDatabase -> "local-hiring",
          LogField.Reason -> "DATABASE_TIMEOUT", LogField.DurationMs -> "2001"))
        lines <- emitted.get
      } yield {
        val details = parse(lines.head).toOption.getOrElse(fail("Invalid JSON")).hcursor.downField("details")
        assertEquals(details.get[String]("mongoHosts"), Right(if (masked) "[REDACTED]" else "local-db:27017"))
        assertEquals(details.get[String]("mongoDatabase"), Right(if (masked) "[REDACTED]" else "local-hiring"))
        assertEquals(details.get[String]("reason"), Right("DATABASE_TIMEOUT"))
        assertEquals(details.get[String]("durationMs"), Right("2001"))
      }
    }
  }

  test("LOG-04 unknown public evidence is filtered and exceptions reveal no message, path or method") {
    val secret = "synthetic-private-secret"
    val failure = new IllegalStateException(secret)
    failure.setStackTrace(Array(new StackTraceElement("com.example.graphQL.cats.Main", secret, s"/$secret.scala", 25)))
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line), maskSensitive = false)
      _ <- diagnostics.event(LogEvent.RuntimeFailed, fields = LogFields.failure(failure))
      _ <- diagnostics.event(LogEvent.RequestRejected, fields = Map(
        LogField.Reason -> secret, LogField.Route -> s"/$secret", LogField.Method -> secret,
        LogField.ErrorType -> secret, LogField.ErrorLocation -> s"/$secret.scala:25", LogField.ConfigKey -> secret))
      lines <- emitted.get
    } yield {
      assert(lines.forall(!_.contains(secret)))
      val first = parse(lines.head).toOption.getOrElse(fail("Invalid JSON")).hcursor.downField("details")
      assertEquals(first.get[String]("errorType"), Right("java.lang.IllegalStateException"))
      assertEquals(first.get[String]("errorLocation"), Right("Main.scala:25"))
      assert(lines.last.contains("[FILTERED]"))
    }
  }

  test("LOG-01 records have bounded details and cannot inject additional log lines") {
    for {
      emitted <- Ref.of[IO, Vector[String]](Vector.empty)
      diagnostics = SafeDiagnostics.withSink("INFO", line => emitted.update(_ :+ line), maskSensitive = false)
      _ <- diagnostics.event(LogEvent.Started, fields = LogField.values.map(_ -> ("\n\r\u2028\u202e💡" * 3000)).toMap)
      _ <- diagnostics.event(LogEvent.GraphQLCompleted, fields = Map(LogField.OperationName -> ("💡" * 500)))
      lines <- emitted.get
    } yield {
      lines.foreach { line =>
        assert(line.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 8192)
        assert(!line.contains('\n') && !line.contains('\r') && !line.contains('\u2028') && !line.contains('\u202e'))
        val details = parse(line).toOption.getOrElse(fail("Invalid JSON")).hcursor.downField("details").focus
          .flatMap(_.asObject).getOrElse(fail("Missing details"))
        assert(details.size <= 12)
      }
      assertEquals(parse(lines.last).flatMap(_.hcursor.downField("details").get[String]("operationName")),
        Right("💡" * 127 + "…"))
    }
  }

  test("LOG-04 synchronous and effectful diagnostic failures are swallowed without swallowing cancellation") {
    val secret = new IllegalStateException("synthetic-sink-secret")
    val throwing = new Diagnostics {
      def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] = throw secret
    }
    for {
      _ <- Diagnostics.emit(throwing, LogEvent.Started)
      _ <- SafeDiagnostics.withSink("INFO", _ => IO.raiseError(secret)).event(LogEvent.Started)
      _ <- SafeDiagnostics.withSink("INFO", _ => throw secret).event(LogEvent.Started)
      entered <- Deferred[IO, Unit]
      finalized <- Deferred[IO, Unit]
      waiting = SafeDiagnostics.withSink("INFO", _ => (entered.complete(()) *> IO.never[Unit]).onCancel(finalized.complete(()).void))
      _ <- Diagnostics.emit(waiting, LogEvent.Started).start.bracket { fiber =>
        for {
          _ <- entered.get.timeout(1.second)
          _ <- fiber.cancel
          outcome <- fiber.join
          _ <- finalized.get.timeout(1.second)
        } yield assert(outcome.isCanceled)
      }(_.cancel)
    } yield ()
  }

  test("P1-AC09 backend suppresses raw framework and driver emitters at every app level") {
    List("TRACE", "DEBUG", "INFO", "WARN", "ERROR").traverse_ { level =>
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
