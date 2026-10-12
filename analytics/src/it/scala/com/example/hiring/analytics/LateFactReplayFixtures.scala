package com.example.hiring.analytics

import cats.effect.IO
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.Harness
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.domain.*
import org.apache.spark.sql.Row

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.*

/** Twelve retained late facts and the explicit replay request that selects them. */
private[analytics] object LateFactReplayFixtures {
  def seed(harness: Harness): IO[(AnalyticsLateFactReplayRequest, Instant, Instant)] = {
    val observed = Instant.now().minusSeconds(25L * 86400L).truncatedTo(ChronoUnit.MICROS)
    val expiry = observed.plusSeconds(30L * 86400L)
    val request = AnalyticsLateFactReplayRequest
      .from("selected-old-day", (0 until 12).toVector.map(n => (harness.topic, n % 3, (n / 3).toLong)))
      .toOption
      .get
    harness.lock
      .resource(harness.paths.root)
      .use { _ =>
        harness.maintenance.validateHmacConfigurationLocked *> harness
          .execution {
            val rows = (0 until 12).map { n =>
              val event = s"late-event-$n"
              val token = AnalyticsTestSubjectPseudonymizer.tokenValue(harness.keys, harness.subjects(n))
              Row(
                event,
                AnalyticsDigest.sha256Hex(event.getBytes("UTF-8")),
                "APPLICATION_CREATED",
                harness.topic,
                n % 3,
                (n / 3).toLong,
                Timestamp.from(observed.minusSeconds(86400L)),
                "Application",
                s"application-$n",
                s"application-$n",
                "job-1",
                "Created",
                Vector("Scala"),
                token,
                Vector(token),
                "CLOSED_DAY",
                Timestamp.from(observed),
                Timestamp.from(expiry)
              )
            }
            harness.spark.createDataFrame(rows.asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts))
          }
          .flatMap(frame =>
            harness.writer.merge(
              frame,
              harness.paths.lateFacts,
              "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
            )
          )
      }
      .as((request, observed, expiry))
  }
}
