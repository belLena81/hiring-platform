package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.{ClaimedSearchSessionWork, PendingSearchSessionWork, SearchSessionWorkState}
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import org.bson.Document

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters.*

/** BSON representation for durable, query-free search-session materialization work. */
private[mongo] object MongoSearchSessionWorkCodecs {
  import MongoHiringCodecs.StoredDocumentError

  def work(value: PendingSearchSessionWork, now: Instant): Document = {
    val session = MongoHiringCodecs.searchSession(value.session.copy(query = None))
    new Document(Map[String, AnyRef](
      "_id" -> value.session.id.toString,
      "actorId" -> value.session.actorId.value.toString,
      "session" -> session,
      "event" -> MongoHiringCodecs.operationalEvent(sanitize(value.event)),
      "state" -> SearchSessionWorkState.Ready.toString,
      "attempts" -> java.lang.Integer.valueOf(0),
      "availableAt" -> Date.from(now),
      "createdAt" -> Date.from(now),
      "updatedAt" -> Date.from(now)
    ).asJava)
  }

  def readWork(document: Document): Either[StoredDocumentError, PendingSearchSessionWork] =
    for {
      sessionDocument <- requiredDocument(document, "session")
      session <- MongoHiringCodecs.readSearchSession(sessionDocument).toEither.leftMap(_.head)
      eventDocument <- requiredDocument(document, "event")
      event <- MongoHiringCodecs.readOperationalEvent(eventDocument).toEither.leftMap(_.head)
      _ <- Either.cond(session.query.isEmpty && event.aggregateId == session.id.toString, (), StoredDocumentError.InconsistentDocument)
    } yield PendingSearchSessionWork(session, event)

  def readClaim(document: Document): Either[StoredDocumentError, ClaimedSearchSessionWork] =
    for {
      work <- readWork(document)
      attempts <- requiredInt(document, "attempts")
      leaseToken <- requiredString(document, "leaseToken")
    } yield ClaimedSearchSessionWork(work, attempts, leaseToken)

  def readState(document: Document): Either[StoredDocumentError, SearchSessionWorkState] =
    requiredString(document, "state").flatMap(value =>
      SearchSessionWorkState.values.find(_.toString == value).toRight(StoredDocumentError.InvalidField("state"))
    )

  private def sanitize(event: OperationalEventEnvelope): OperationalEventEnvelope =
    event.copy(payload = event.payload.mapObject(_.remove("query")))

  private def requiredDocument(document: Document, field: String): Either[StoredDocumentError, Document] =
    Option(document.get(field)) match {
      case Some(value: Document) => Right(value)
      case None => Left(StoredDocumentError.MissingField(field))
      case _ => Left(StoredDocumentError.InvalidField(field))
    }

  private def requiredString(document: Document, field: String): Either[StoredDocumentError, String] =
    Option(document.get(field)) match {
      case Some(value: String) => Right(value)
      case None => Left(StoredDocumentError.MissingField(field))
      case _ => Left(StoredDocumentError.InvalidField(field))
    }

  private def requiredInt(document: Document, field: String): Either[StoredDocumentError, Int] =
    Option(document.get(field)) match {
      case Some(value: Number) => Right(value.intValue)
      case None => Left(StoredDocumentError.MissingField(field))
      case _ => Left(StoredDocumentError.InvalidField(field))
    }
}
