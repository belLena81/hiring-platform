package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.{RangeFingerprint, RunId}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation

import cats.syntax.all.*
import java.time.Instant

/** Validates typed persisted report records before they enter publication logic. */
private[analytics] object MongoAnalyticsReportRecords {
  final case class Run(reservation: AnalyticsReportReservation, state: String)
  final case class Control(
      generation: Long,
      nextRevision: Option[Long],
      lastPublishedRevision: Long,
      lastRunId: Option[String],
      state: String
  )
  final case class Snapshot(generation: Long, revision: Long, runId: RunId, expiresAt: Option[Instant]) {
    def matches(reservation: AnalyticsReportReservation): Boolean =
      generation == reservation.generation && revision == reservation.revision && runId == reservation.runId
  }

  private val malformed = AnalyticsError.InvalidConfiguration("analytics report record is malformed")

  def decodeRun(record: AnalyticsMongoRecords.ReportRun): Either[AnalyticsError, Run] =
    for {
      runId <- RunId.from(record._id).leftMap(_ => malformed)
      rangeFingerprint <- RangeFingerprint.from(record.rangeFingerprint).leftMap(_ => malformed)
      state <- Either.cond(Set("Reserved", "Published").contains(record.state), record.state, malformed)
    } yield Run(
      AnalyticsReportReservation(runId, rangeFingerprint, record.generation, record.revision),
      state
    )

  def decodeControl(record: AnalyticsMongoRecords.ReportControl): Either[AnalyticsError, Control] =
    Either.cond(
      record._id == "analytics-report",
      Control(record.generation, record.nextRevision, record.lastPublishedRevision, record.lastRunId, record.state),
      malformed
    )

  def decodeSnapshot(record: AnalyticsMongoRecords.ReportSnapshotMetadata): Either[AnalyticsError, Snapshot] =
    RunId.from(record.runId).leftMap(_ => malformed).map { runId =>
      Snapshot(record.generation, record.revision, runId, record.expiresAt)
    }
}
