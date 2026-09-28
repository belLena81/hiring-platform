package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.{RangeFingerprint, RunId}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation
import cats.syntax.all.*
import org.bson.{BsonValue, Document}

import java.util.Date

/** Validates driver-decoded report records before they enter report publication logic. */
private[analytics] object MongoAnalyticsReportRecords {
  type Record = MongoPojoCodecs.ReportRecord

  final case class Run(reservation: AnalyticsReportReservation, state: String)
  final case class Control(
      generation: Long,
      nextRevision: Option[Long],
      lastPublishedRevision: Long,
      lastRunId: Option[String],
      state: String,
      extraFields: Document
  )
  final case class Snapshot(
      generation: Long,
      revision: Long,
      runId: RunId,
      expiresAt: Option[Date],
      extraFields: Document
  ) {
    def matches(reservation: AnalyticsReportReservation): Boolean =
      generation == reservation.generation && revision == reservation.revision && runId == reservation.runId
  }

  private val malformed = AnalyticsError.InvalidConfiguration("analytics report record is malformed")

  private def required[A](value: BsonValue)(read: BsonValue => Option[A]): Either[AnalyticsError, A] =
    Option(value).filterNot(_.isNull).flatMap(read).toRight(malformed)

  private def optional[A](value: BsonValue)(read: BsonValue => Option[A]): Either[AnalyticsError, Option[A]] =
    Option(value).filterNot(_.isNull) match {
      case None        => Right(None)
      case Some(value) => read(value).map(Some(_)).toRight(malformed)
    }

  private def string(value: BsonValue): Option[String] =
    Option(value).filter(_.isString).map(_.asString().getValue)
  private def int64(value: BsonValue): Option[Long] =
    Option(value).filter(_.isInt64).map(_.asInt64().getValue)
  private def date(value: BsonValue): Option[Date] =
    Option(value).filter(_.isDateTime).map(v => new Date(v.asDateTime().getValue))

  def decodeRun(record: Record): Either[AnalyticsError, Run] =
    for {
      id <- required(record.getId)(string)
      fingerprint <- required(record.getRangeFingerprint)(string)
      runId <- RunId.from(id).toEither.leftMap(_ => malformed)
      rangeFingerprint <- RangeFingerprint.from(fingerprint).leftMap(_ => malformed)
      generation <- required(record.getGeneration)(int64)
      revision <- required(record.getRevision)(int64)
      state <- required(record.getState)(string).flatMap { value =>
        Either.cond(Set("Reserved", "Published").contains(value), value, malformed)
      }
    } yield Run(AnalyticsReportReservation(runId, rangeFingerprint, generation, revision), state)

  def decodeControl(record: Record): Either[AnalyticsError, Control] =
    for {
      generation <- required(record.getGeneration)(int64)
      nextRevision <- optional(record.getNextRevision)(int64)
      lastRevision <- required(record.getLastPublishedRevision)(int64)
      lastRunId <- optional(record.getLastRunId)(string)
      state <- required(record.getState)(string)
    } yield Control(
      generation,
      nextRevision,
      lastRevision,
      lastRunId,
      state,
      Option(record.getExtraFields).getOrElse(new Document())
    )

  def decodeSnapshot(record: Record): Either[AnalyticsError, Snapshot] =
    for {
      generation <- required(record.getGeneration)(int64)
      revision <- required(record.getRevision)(int64)
      rawRunId <- required(record.getRunId)(string)
      runId <- RunId.from(rawRunId).toEither.leftMap(_ => malformed)
      expiresAt <- optional(record.getExpiresAt)(date)
    } yield Snapshot(
      generation,
      revision,
      runId,
      expiresAt,
      Option(record.getExtraFields).getOrElse(new Document())
    )
}
