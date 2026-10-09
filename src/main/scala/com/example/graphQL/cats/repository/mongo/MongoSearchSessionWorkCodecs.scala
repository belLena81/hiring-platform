package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{
  ClaimedSearchSessionWork,
  PendingSearchSessionWork,
  SearchSessionWorkState
}
import org.bson.Document

import java.time.Instant
import scala.jdk.CollectionConverters.*

/** BSON representation for durable, query-free search-session materialization work. */
private[mongo] object MongoSearchSessionWorkCodecs {
  import MongoHiringCodecs.StoredDocumentError

  def work(value: PendingSearchSessionWork, now: Instant): Document = {
    val session = MongoHiringCodecs.searchSession(value.session.copy(query = None))
    new Document(
      Map[String, AnyRef](
        MongoFields.Id -> value.session.id.toString,
        MongoFields.ActorId -> value.session.actorId.value.toString,
        MongoFields.Session -> session,
        MongoFields.Event -> MongoHiringCodecs.operationalEvent(value.event),
        MongoFields.State -> SearchSessionWorkState.Ready.toString,
        MongoFields.Attempts -> java.lang.Integer.valueOf(0),
        MongoFields.AvailableAt -> now.toDate,
        MongoFields.CreatedAt -> now.toDate,
        MongoFields.UpdatedAt -> now.toDate
      ).asJava
    )
  }

  def readWork(document: Document): Either[StoredDocumentError, PendingSearchSessionWork] =
    for {
      sessionDocument <- MongoDocumentFields.requiredDocument(document, MongoFields.Session)
      session <- MongoHiringCodecs.readSearchSession(sessionDocument).toEither.leftMap(_.head)
      eventDocument <- MongoDocumentFields.requiredDocument(document, MongoFields.Event)
      event <- MongoHiringCodecs.readOperationalEvent(eventDocument).toEither.leftMap(_.head)
      _ <- Either.cond(
        session.query.isEmpty && PendingSearchSessionWork(session, event).addressesSession,
        (),
        StoredDocumentError.InconsistentDocument
      )
    } yield PendingSearchSessionWork(session, event)

  def readClaim(document: Document): Either[StoredDocumentError, ClaimedSearchSessionWork] =
    for {
      work <- readWork(document)
      attempts <- MongoDocumentFields.requiredInt32(document, MongoFields.Attempts)
      leaseToken <- MongoDocumentFields.requiredString(document, MongoFields.LeaseToken)
    } yield ClaimedSearchSessionWork(work, attempts, leaseToken)

  def readState(document: Document): Either[StoredDocumentError, SearchSessionWorkState] =
    MongoDocumentFields.requiredEnum(document, MongoFields.State)(
      MongoDocumentFields.byName(SearchSessionWorkState.values)
    )
}
