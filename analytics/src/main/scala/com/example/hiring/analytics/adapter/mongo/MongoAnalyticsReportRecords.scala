package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.{RangeFingerprint, RunId}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation

import cats.syntax.all.*
import java.time.Instant
import scala.jdk.CollectionConverters.*

/** Validates typed persisted report records before they enter publication logic. */
private[analytics] object MongoAnalyticsReportRecords {

  /** Stored `state` strings of report runs, control and snapshot records. */
  enum ReportState(val persisted: String) {
    case Reserved extends ReportState("Reserved")
    case Published extends ReportState("Published")
    case Unpublished extends ReportState("Unpublished")
    case Hidden extends ReportState("Hidden")
  }
  object ReportState {
    val ControlId = "analytics-report"
    val CurrentSnapshotId = "current"

    /** States from which the control record may move to Published without erasure. */
    val PublishableFromControl: Set[ReportState] = Set(Published, Unpublished)
    val ErasurePublishableFromControl: Set[ReportState] = Set(Hidden, Published)
    def persisted(states: Set[ReportState]): java.util.List[String] =
      states.toVector.map(_.persisted).asJava
  }

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

  private val malformed = AnalyticsError.DataIntegrityViolation("analytics report record is malformed")

  def decodeRun(record: AnalyticsMongoRecords.ReportRun): Either[AnalyticsError, Run] =
    for {
      runId <- RunId.from(record._id).leftMap(_ => malformed)
      rangeFingerprint <- RangeFingerprint.from(record.rangeFingerprint).leftMap(_ => malformed)
      state <- Either.cond(
        Set(ReportState.Reserved, ReportState.Published).exists(_.persisted == record.state),
        record.state,
        malformed
      )
    } yield Run(
      AnalyticsReportReservation(runId, rangeFingerprint, record.generation, record.revision),
      state
    )

  def decodeControl(record: AnalyticsMongoRecords.ReportControl): Either[AnalyticsError, Control] =
    Either.cond(
      record._id == ReportState.ControlId,
      Control(record.generation, record.nextRevision, record.lastPublishedRevision, record.lastRunId, record.state),
      malformed
    )

  def decodeSnapshot(record: AnalyticsMongoRecords.ReportSnapshotMetadata): Either[AnalyticsError, Snapshot] =
    RunId.from(record.runId).leftMap(_ => malformed).map { runId =>
      Snapshot(record.generation, record.revision, runId, record.expiresAt)
    }
}
