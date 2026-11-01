package com.example.hiring.analytics.service.streaming

import cats.effect.Sync
import com.example.hiring.analytics.domain.StreamingBatchId
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.FiniteDuration

/** Sanitized per-ingest evidence of facts that fall outside the allowed lateness bound.
  *
  * `closedDay` counts facts whose UTC day is already closed by the durable watermark; they are stored only in the
  * late-fact dataset and reach a report solely through explicit operator replay. `future` counts facts too far ahead of
  * the observation time (or without an event time); they are stored only in quarantine and block that batch's
  * publication. `generation` and `revision` come from the batch's publication reservation, which is allocated before
  * ingestion whether or not the batch ends up publishing (a quality-blocked or erasure-pending batch still logs its
  * reservation). The counted closed-day facts are therefore missing from the report published under that generation and
  * revision; an explicit replay later publishes a higher revision. Counts are per ingest attempt; a recovered batch
  * logs again with the same `batch`, which lets a consumer de-duplicate. The line holds no payload, identifier of a
  * subject, or event ID.
  */
final case class StreamingEventTimeAdmissionLog(
    closedDay: Long,
    future: Long,
    allowedLateness: FiniteDuration,
    batchId: StreamingBatchId,
    generation: Long,
    revision: Long
) {
  def line: String =
    s"STREAMING_EVENT_TIME_ADMISSION closedDay=$closedDay future=$future " +
      s"allowedLatenessMillis=${allowedLateness.toMillis} batch=${batchId.value} generation=$generation revision=$revision"
}

object StreamingEventTimeAdmissionLog {
  val LoggerName = "com.example.hiring.analytics.streaming.admission"

  def emit[F[_]: Sync](entry: StreamingEventTimeAdmissionLog): F[Unit] =
    Slf4jLogger.getLoggerFromName[F](LoggerName).info(entry.line)
}
