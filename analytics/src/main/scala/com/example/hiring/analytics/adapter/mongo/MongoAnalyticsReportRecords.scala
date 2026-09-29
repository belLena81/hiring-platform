package com.example.hiring.analytics.adapter.mongo

import com.example.hiring.analytics.domain.{RangeFingerprint, RunId}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation

import cats.syntax.all.*
import com.mongodb.MongoClientSettings
import org.bson.{BsonDocument, BsonValue, Document}

import java.util.Date
import scala.jdk.CollectionConverters.*

/** Validates raw driver documents before they enter report publication logic. */
private[analytics] object MongoAnalyticsReportRecords {
  final case class Run(reservation: AnalyticsReportReservation, state: String)
  final case class Control(
      generation: Long,
      nextRevision: Option[Long],
      lastPublishedRevision: Long,
      lastRunId: Option[String],
      state: String,
      extraFields: Map[String, BsonValue]
  )
  final case class Snapshot(
      generation: Long,
      revision: Long,
      runId: RunId,
      expiresAt: Option[Date],
      extraFields: Map[String, BsonValue]
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

  private def bsonDocument(record: Document): BsonDocument =
    record.toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry)

  private def extraFields(record: BsonDocument, known: Set[String]): Map[String, BsonValue] =
    record.entrySet().asScala.iterator.map(entry => entry.getKey -> entry.getValue).toMap.filterNot { case (name, _) =>
      known(name)
    }

  def decodeRun(rawRecord: Document): Either[AnalyticsError, Run] = {
    val record = bsonDocument(rawRecord)
    for {
      id <- required(record.get(AnalyticsCollections.Fields.Id))(string)
      fingerprint <- required(record.get(AnalyticsCollections.Fields.RangeFingerprint))(string)
      runId <- RunId.from(id).toEither.leftMap(_ => malformed)
      rangeFingerprint <- RangeFingerprint.from(fingerprint).leftMap(_ => malformed)
      generation <- required(record.get(AnalyticsCollections.Fields.Generation))(int64)
      revision <- required(record.get(AnalyticsCollections.Fields.Revision))(int64)
      state <- required(record.get(AnalyticsCollections.Fields.State))(string).flatMap { value =>
        Either.cond(Set("Reserved", "Published").contains(value), value, malformed)
      }
    } yield Run(AnalyticsReportReservation(runId, rangeFingerprint, generation, revision), state)
  }

  def decodeControl(rawRecord: Document): Either[AnalyticsError, Control] = {
    val record = bsonDocument(rawRecord)
    for {
      generation <- required(record.get(AnalyticsCollections.Fields.Generation))(int64)
      nextRevision <- optional(record.get(AnalyticsCollections.Fields.NextRevision))(int64)
      lastRevision <- required(record.get(AnalyticsCollections.Fields.LastPublishedRevision))(int64)
      lastRunId <- optional(record.get(AnalyticsCollections.Fields.LastRunId))(string)
      state <- required(record.get(AnalyticsCollections.Fields.State))(string)
    } yield Control(
      generation,
      nextRevision,
      lastRevision,
      lastRunId,
      state,
      extraFields(
        record,
        Set(
          AnalyticsCollections.Fields.Id,
          AnalyticsCollections.Fields.Generation,
          AnalyticsCollections.Fields.NextRevision,
          AnalyticsCollections.Fields.LastPublishedRevision,
          AnalyticsCollections.Fields.LastRunId,
          AnalyticsCollections.Fields.State
        )
      )
    )
  }

  def decodeSnapshot(rawRecord: Document): Either[AnalyticsError, Snapshot] = {
    val record = bsonDocument(rawRecord)
    for {
      generation <- required(record.get(AnalyticsCollections.Fields.Generation))(int64)
      revision <- required(record.get(AnalyticsCollections.Fields.Revision))(int64)
      rawRunId <- required(record.get(AnalyticsCollections.Fields.RunId))(string)
      runId <- RunId.from(rawRunId).toEither.leftMap(_ => malformed)
      expiresAt <- optional(record.get(AnalyticsCollections.Fields.ExpiresAt))(date)
    } yield Snapshot(
      generation,
      revision,
      runId,
      expiresAt,
      extraFields(
        record,
        Set(
          AnalyticsCollections.Fields.Id,
          AnalyticsCollections.Fields.State,
          AnalyticsCollections.Fields.Generation,
          AnalyticsCollections.Fields.Revision,
          AnalyticsCollections.Fields.RunId,
          AnalyticsCollections.Fields.ExpiresAt,
          AnalyticsCollections.Fields.AsOf,
          "funnel",
          "skillPostingActivity",
          "timeToHire"
        )
      )
    )
  }
}
