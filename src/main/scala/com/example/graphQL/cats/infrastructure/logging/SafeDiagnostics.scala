package com.example.graphQL.cats.infrastructure.logging

import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields, LogLevel}
import io.circe.Json
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermission
import org.slf4j.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.trace.Tracer
import scala.jdk.CollectionConverters.*

/** The application classifies records; Logback is the sole severity policy. */
object SafeDiagnostics {
  private val loggerName = "hiring.foundation"
  private val DefaultLogDirectory = Paths.get("_logs")

  def configure(maskSensitive: Boolean = true, tracer: Tracer[IO] = Tracer.noop[IO]): IO[Diagnostics] =
    IO.blocking {
      Files.createDirectories(DefaultLogDirectory)
      val diagnostics = apply(maskSensitive, tracer)
      if (!maskSensitive) {
        restrictToCurrentUser(DefaultLogDirectory)
        activeLogFiles.foreach(restrictToCurrentUser)
        requireSafeUnmaskedDestination()
      }
      diagnostics
    }.flatTap { diagnostics =>
      if (maskSensitive) IO.unit else Diagnostics.emit(diagnostics, LogEvent.LocalUnmasked)
    }

  def apply(maskSensitive: Boolean = true, tracer: Tracer[IO] = Tracer.noop[IO]): Diagnostics = {
    val structuredLogger = Slf4jLogger.getLoggerFromName[IO](loggerName)
    withEventSink(maskSensitive, levelEnabled(structuredLogger, _), (event, message) =>
      event.level match {
        case LogLevel.Trace => structuredLogger.trace(Map("category" -> event.category))(message)
        case LogLevel.Debug => structuredLogger.debug(Map("category" -> event.category))(message)
        case LogLevel.Info => structuredLogger.info(Map("category" -> event.category))(message)
        case LogLevel.Warn => structuredLogger.warn(Map("category" -> event.category))(message)
        case LogLevel.Error => structuredLogger.error(Map("category" -> event.category))(message)
      }, tracer)
  }

  private[logging] def withSink(sink: String => IO[Unit], maskSensitive: Boolean = true): Diagnostics =
    withEventSink(maskSensitive, _ => IO.pure(true), (_, message) => sink(message))

  private def levelEnabled(logger: org.typelevel.log4cats.SelfAwareStructuredLogger[IO], level: LogLevel): IO[Boolean] = level match {
    case LogLevel.Trace => logger.isTraceEnabled
    case LogLevel.Debug => logger.isDebugEnabled
    case LogLevel.Info => logger.isInfoEnabled
    case LogLevel.Warn => logger.isWarnEnabled
    case LogLevel.Error => logger.isErrorEnabled
  }

  private def requireSafeUnmaskedDestination(): Unit = {
    val files = activeLogFiles
    val unsafe = files.isEmpty || files.exists { file =>
      val directory = file.getParent
      !Files.isRegularFile(file) || !Files.isDirectory(directory) || !Files.isWritable(directory) || !privatePath(directory) || !privatePath(file)
    }
    if (unsafe) throw new IllegalStateException("Unmasked diagnostics require a private pre-provisioned file directory")
  }

  private def activeLogFiles: List[Path] = {
    val logger = LoggerFactory.getLogger(loggerName)
    Option(logger.getClass.getMethod("iteratorForAppenders").invoke(logger))
      .collect { case iterator: java.util.Iterator[?] => iterator.asScala.toList }
      .getOrElse(Nil)
      .flatMap { appender =>
        Option(appender.getClass.getMethod("getFile").invoke(appender)).collect {
          case file: String if file.nonEmpty => Paths.get(file).toAbsolutePath.normalize
        }
      }
  }

  private def restrictToCurrentUser(path: Path): Unit =
    try {
      val permissions =
        if (Files.isDirectory(path)) Set(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE
        )
        else Set(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE
        )
      Files.setPosixFilePermissions(path, permissions.asJava): Unit
    }
    catch {
      case _: UnsupportedOperationException => ()
    }

  private def privatePath(path: Path): Boolean =
    try {
      val permissions = Files.getPosixFilePermissions(path)
      !permissions.asScala.exists(permission => permission.toString.startsWith("GROUP_") || permission.toString.startsWith("OTHERS_"))
    } catch {
      case _: UnsupportedOperationException => true
    }

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

  private def withEventSink(
      maskSensitive: Boolean,
      isEnabled: LogLevel => IO[Boolean],
      sink: (LogEvent, String) => IO[Unit],
      traceProvider: Tracer[IO] = Tracer.noop[IO]
  ): Diagnostics = new Diagnostics {
    override val tracer: Tracer[IO] = traceProvider
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] = IO.defer {
      isEnabled(event.level).flatMap { enabled =>
      if (!enabled) IO.unit
      else IO.realTimeInstant.flatMap { timestamp =>
        val safeId = requestId.filter(isUuid)
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
          "masking" -> Json.fromString(if (maskSensitive) "enabled" else "disabled"),
          "details" -> Json.obj(details*)
        )
        val encoded = record.noSpaces
        val line = if (encoded.getBytes(StandardCharsets.UTF_8).length <= 8192) encoded
          else record.mapObject(_.add("details", Json.obj())).noSpaces
        sink(event, line)
      }
      }
    }.handleError(_ => ())
  }

  private def isUuid(value: String): Boolean =
    value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
}
