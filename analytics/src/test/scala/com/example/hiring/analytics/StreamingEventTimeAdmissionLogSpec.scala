package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.domain.StreamingBatchId
import com.example.hiring.analytics.service.streaming.StreamingEventTimeAdmissionLog
import munit.CatsEffectSuite
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.{LoggerConfig, Property}

import scala.concurrent.duration.*

class StreamingEventTimeAdmissionLogSpec extends CatsEffectSuite {
  private val batch = StreamingBatchId.from(42L).toOption.get

  test("DDR-04.2 the admission line reports closed-day and future counts with the configured lateness") {
    val line = StreamingEventTimeAdmissionLog(12L, 3L, 48.hours, batch, 7L, 5L).line
    assertEquals(
      line,
      "STREAMING_EVENT_TIME_ADMISSION closedDay=12 future=3 allowedLatenessMillis=172800000 batch=42 generation=7 revision=5"
    )
  }

  test("DDR-04.2 DDR-04.3 a batch without late facts reports zero counts and still names its generation and revision") {
    assertEquals(
      StreamingEventTimeAdmissionLog(0L, 0L, 24.hours, batch, 0L, 1L).line,
      "STREAMING_EVENT_TIME_ADMISSION closedDay=0 future=0 allowedLatenessMillis=86400000 batch=42 generation=0 revision=1"
    )
  }

  test("DDR-04.2 DDR-04.3 emit writes exactly the rendered line to the admission logger") {
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val captured = new java.util.concurrent.CopyOnWriteArrayList[String]()
    val appender = new AbstractAppender("ddr-04-capture", null, null, true, Property.EMPTY_ARRAY) {
      override def append(event: LogEvent): Unit = { captured.add(event.getMessage.getFormattedMessage); () }
    }
    appender.start()
    val config = context.getConfiguration
    // The test configuration logs at ERROR; give the admission logger its own INFO configuration for the capture.
    val loggerConfig = new LoggerConfig(StreamingEventTimeAdmissionLog.LoggerName, Level.INFO, false)
    val entry = StreamingEventTimeAdmissionLog(2L, 1L, 24.hours, batch, 3L, 4L)
    IO {
      config.addAppender(appender)
      loggerConfig.addAppender(appender, null, null)
      config.addLogger(StreamingEventTimeAdmissionLog.LoggerName, loggerConfig)
      context.updateLoggers()
    }.flatMap(_ => StreamingEventTimeAdmissionLog.emit[IO](entry))
      .guarantee(IO {
        config.removeLogger(StreamingEventTimeAdmissionLog.LoggerName)
        config.getAppenders.remove("ddr-04-capture")
        appender.stop()
        context.updateLoggers()
      })
      .map(_ => assertEquals(captured.toArray.toList, List(entry.line)))
  }
}
