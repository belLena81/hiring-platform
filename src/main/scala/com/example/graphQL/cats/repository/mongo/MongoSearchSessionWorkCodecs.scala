package com.example.graphQL.cats.repository.mongo

import cats.data.{Validated, ValidatedNel}
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

  /** Every problem of the stored session and of the stored event is reported, not just the first. */
  def readWork(document: Document): ValidatedNel[StoredDocumentError, PendingSearchSessionWork] =
    (
      MongoDocumentFields
        .requiredDocument(document, MongoFields.Session)
        .toValidatedNel
        .andThen(MongoHiringCodecs.readSearchSession),
      MongoDocumentFields
        .requiredDocument(document, MongoFields.Event)
        .toValidatedNel
        .andThen(MongoHiringCodecs.readOperationalEvent)
    ).tupled.andThen { case (session, event) =>
      Validated.condNel(
        session.query.isEmpty && PendingSearchSessionWork(session, event).addressesSession,
        PendingSearchSessionWork(session, event),
        StoredDocumentError.InconsistentDocument
      )
    }

  def readClaim(document: Document): ValidatedNel[StoredDocumentError, ClaimedSearchSessionWork] =
    (
      readWork(document),
      MongoDocumentFields.requiredInt32(document, MongoFields.Attempts).toValidatedNel,
      MongoDocumentFields.requiredString(document, MongoFields.LeaseToken).toValidatedNel
    ).mapN(ClaimedSearchSessionWork.apply)

  def readState(document: Document): Either[StoredDocumentError, SearchSessionWorkState] =
    MongoDocumentFields.requiredEnum(document, MongoFields.State)(
      MongoDocumentFields.byName(SearchSessionWorkState.values)
    )
}
