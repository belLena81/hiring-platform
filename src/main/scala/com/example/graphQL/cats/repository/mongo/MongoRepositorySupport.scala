package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationEvent, Job, User}
import com.example.graphQL.cats.repository.protocol.{RepositoryError, RepositoryIO, Versioned}
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.pagination.PageSize
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Sorts}
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.{ClientSession, MongoCollection}
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant

private[mongo] object MongoRepositorySupport {
  def writeResult[A](result: Option[A]): Either[RepositoryError, A] =
    result.toRight(RepositoryError.MissingWriteResult)

  def reportFailure(diagnostics: Diagnostics, operation: String, error: Throwable): IO[Unit] =
    diagnostics.emit(
      LogEvent.MongoRepositoryFailed,
      fields = LogFields.failure(error) + (LogField.SpanName -> operation)
    )

  def guard[A](diagnostics: Diagnostics, operation: String)(result: IO[A])(map: Throwable => A): IO[A] =
    result.handleErrorWith { error =>
      reportFailure(diagnostics, operation, error).as(map(error))
    }

  def repositoryGuard[A](
      diagnostics: Diagnostics,
      operation: String
  )(result: IO[Either[RepositoryError, A]])(map: Throwable => Either[RepositoryError, A]): RepositoryIO[A] =
    RepositoryIO.fromIOEither(guard(diagnostics, operation)(result)(map))
}

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
      now: Instant,
      diagnostics: Diagnostics = Diagnostics.noop
  ): IO[Either[RepositoryError, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.outbox.insert")(
        events.foldLeft(IO.pure[Either[RepositoryError, Unit]](Right(()))) { (previous, event) =>
          previous.flatMap {
            case Left(error) => IO.pure(Left(error))
            case Right(_)    =>
              MongoHiringCodecs.outboxRecord(event, now) match {
                case Left(_)         => IO.pure(Left(RepositoryError.InvalidStoredData))
                case Right(document) =>
                  session
                    .fold(PublisherBridge.first(outbox.insertOne(document)))(active =>
                      PublisherBridge.first(outbox.insertOne(active, document))
                    )
                    .map(MongoRepositorySupport.writeResult(_).void)
              }
          }
        }
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
        case _                                                             => Left(RepositoryError.Unavailable)
      }
      .value
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
    decoded.toEither.leftMap(_ => RepositoryError.InvalidStoredData)

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
  )(diagnostics: Diagnostics = Diagnostics.noop): IO[Either[RepositoryError, List[A]]] =
    if (ids.isEmpty) IO.pure(Right(Nil))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "repository.findMany")(
          PublisherBridge
            .collectWithin(collection.find(Filters.in("_id", ids.distinct*)), ids.distinct.size)
            .map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
        )(_ => Left(RepositoryError.Unavailable))
        .value

  def page[A](collection: MongoCollection[Document], filter: Bson, timestampField: String, pageSize: PageSize)(
      read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]
  )(diagnostics: Diagnostics = Diagnostics.noop): IO[Either[RepositoryError, List[A]]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.page")(
        PublisherBridge
          .collectWithin(
            collection
              .find(filter)
              .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending("_id")))
              .limit(pageSize.value),
            pageSize.value
          )
          .map(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      )(_ => Left(RepositoryError.Unavailable))
      .value

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

  def beforeCursor(timestampField: String, occurredAt: Instant, id: String): Bson =
    Filters.or(
      Filters.lt(timestampField, java.util.Date.from(occurredAt)),
      Filters.and(Filters.eq(timestampField, java.util.Date.from(occurredAt)), Filters.lt("_id", id))
    )
}
