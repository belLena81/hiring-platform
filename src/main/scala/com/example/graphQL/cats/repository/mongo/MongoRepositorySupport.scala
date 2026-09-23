package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationEvent, Job, User}
import com.example.graphQL.cats.repository.protocol.{RepositoryError, Versioned}
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.pagination.PageSize
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Sorts}
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant

private[mongo] trait MongoConflictWriteMapping {
  protected final def mapWrite[A](error: Throwable): Either[RepositoryError, A] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
      case _                                                             => Left(RepositoryError.Unavailable)
    }
}

private[mongo] trait MongoApplicationEventInsertion {
  protected final def insertApplicationEvent(
      events: MongoCollection[Document],
      session: Option[ClientSession],
      event: ApplicationEvent
  ): IO[Option[InsertOneResult]] =
    session.fold(
      PublisherBridge.first(events.insertOne(MongoHiringCodecs.event(event)))
    ) { active =>
      PublisherBridge.first(events.insertOne(active, MongoHiringCodecs.event(event)))
    }
}

private[mongo] trait MongoOperationalEventInsertion {
  protected final def insertOperationalEvents(
      outbox: MongoCollection[Document],
      session: Option[ClientSession],
      events: List[OperationalEventEnvelope],
      now: Instant
  ): IO[Either[RepositoryError, Unit]] =
    events
      .traverse_ { event =>
        val document = MongoHiringCodecs.outboxRecord(event, now)
        session.fold(PublisherBridge.first(outbox.insertOne(document)))(active =>
          PublisherBridge.first(outbox.insertOne(active, document))
        )
      }
      .as(Right(()))
      .handleError {
        case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
        case _                                                             => Left(RepositoryError.Unavailable)
      }
}

private[mongo] object MongoObservedStateFilters {
  def jobReplacement(job: Versioned[Job]): Bson =
    Filters.and(Filters.eq("_id", job.value.id.value.toString), Filters.eq("version", job.version))

  def jobEmbedding(job: Versioned[Job]): Bson =
    Filters.and(
      Filters.eq("_id", job.value.id.value.toString),
      Filters.eq("version", job.version),
      Filters.lt("version", Long.MaxValue)
    )

  def candidateEmbedding(user: Versioned[User]): Bson =
    Filters.and(
      Filters.eq("_id", user.value.id.value.toString),
      Filters.eq("version", user.version),
      Filters.lt("version", Long.MaxValue)
    )
}

private[mongo] object MongoStoredDocumentDecoding {
  def repository[A](decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]): Either[RepositoryError, A] =
    decoded.toEither.leftMap(_ => RepositoryError.Unavailable)

  def optional[A](
      decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, Option[A]]
  ): Either[RepositoryError, Option[A]] =
    repository(decoded)

  def values[A](
      decoded: List[ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]]
  ): Either[RepositoryError, List[A]] =
    repository(decoded.sequence)
}

private[mongo] object MongoKeysetPaging {
  def byId[A](collection: MongoCollection[Document], ids: List[String])(
      read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]
  ): IO[Either[RepositoryError, List[A]]] =
    if (ids.isEmpty) IO.pure(Right(Nil))
    else
      PublisherBridge
        .collectWithin(collection.find(Filters.in("_id", ids.distinct*)), ids.distinct.size)
        .map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
        .handleError(_ => Left(RepositoryError.Unavailable))

  def page[A](collection: MongoCollection[Document], filter: Bson, timestampField: String, pageSize: PageSize)(
      read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]
  ): IO[Either[RepositoryError, List[A]]] =
    PublisherBridge
      .collectWithin(
        collection
          .find(filter)
          .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending("_id")))
          .limit(pageSize.value),
        pageSize.value
      )
      .map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      .handleError(_ => Left(RepositoryError.Unavailable))

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

  def beforeCursor(timestampField: String, occurredAt: Instant, id: String): Bson =
    Filters.or(
      Filters.lt(timestampField, java.util.Date.from(occurredAt)),
      Filters.and(Filters.eq(timestampField, java.util.Date.from(occurredAt)), Filters.lt("_id", id))
    )
}
