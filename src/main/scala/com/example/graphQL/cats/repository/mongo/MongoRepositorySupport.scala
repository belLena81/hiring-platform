package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.operations.{Filter, Update}
import mongo4cats.client.ClientSession
import com.mongodb.client.model.{InsertOneOptions, ReplaceOptions, UpdateOptions, Updates}
import com.mongodb.client.result.{InsertOneResult, UpdateResult}
import com.example.graphQL.cats.domain.model.ApplicationEvent
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.mongodb.{MongoException, MongoWriteException}
import com.mongodb.client.model.{Filters, Sorts}
import scala.jdk.CollectionConverters.*
import java.time.Instant
import org.bson.conversions.Bson

/** Effectfully acquired document collections used by repository adapters. */
private[mongo] object Mongo4catsCollections {
  def documents(database: MongoDatabase[IO], name: String): IO[MongoCollection[IO, org.bson.Document]] =
    database.getCollection(name).map(_.as[org.bson.Document])
}

/** Paired predicates keep the raw-BSON non-session overload and typed session overload in lockstep. */
private[mongo] final case class MongoFilter(bson: Bson, sessionFilter: Filter)

private[mongo] object MongoFilter {
  def eq[A](field: String, value: A): MongoFilter = MongoFilter(Filters.eq(field, value), Filter.eq(field, value))
  def ne[A](field: String, value: A): MongoFilter = MongoFilter(Filters.ne(field, value), Filter.ne(field, value))
  def lt[A](field: String, value: A): MongoFilter = MongoFilter(Filters.lt(field, value), Filter.lt(field, value))
  def gt[A](field: String, value: A): MongoFilter = MongoFilter(Filters.gt(field, value), Filter.gt(field, value))
  def lte[A](field: String, value: A): MongoFilter = MongoFilter(Filters.lte(field, value), Filter.lte(field, value))
  def gte[A](field: String, value: A): MongoFilter = MongoFilter(Filters.gte(field, value), Filter.gte(field, value))
  def exists(field: String): MongoFilter = MongoFilter(Filters.exists(field), Filter.exists(field))
  def exists(field: String, value: Boolean): MongoFilter =
    MongoFilter(Filters.exists(field, value), if (value) Filter.exists(field) else Filter.exists(field).not)
  def in[A](field: String, values: Seq[A]): MongoFilter =
    MongoFilter(Filters.in(field, values.asJava), Filter.in(field, values))
  def all[A](field: String, values: Seq[A]): MongoFilter =
    MongoFilter(Filters.all(field, values.asJava), Filter.all(field, values))
  def beforeCursor(timestampField: String, occurredAt: Instant, id: String): MongoFilter =
    or(
      lt(timestampField, java.util.Date.from(occurredAt)),
      and(
        eq(timestampField, java.util.Date.from(occurredAt)),
        lt(MongoFields.Id, id)
      )
    )
  def and(filters: MongoFilter*): MongoFilter =
    if (filters.isEmpty) exists(MongoFields.Id)
    else MongoFilter(Filters.and(filters.map(_.bson).asJava), Filter.and(filters.map(_.sessionFilter)*))
  def or(filters: MongoFilter*): MongoFilter =
    MongoFilter(Filters.or(filters.map(_.bson).asJava), Filter.or(filters.map(_.sessionFilter)*))
}

/** Paired updates keep the raw-BSON non-session overload and typed session overload in lockstep. */
private[mongo] final case class MongoUpdate(bson: Bson, sessionUpdate: Update)

private[mongo] object MongoUpdate {
  def set[A](field: String, value: A): MongoUpdate = MongoUpdate(Updates.set(field, value), Update.set(field, value))
  def setOnInsert[A](field: String, value: A): MongoUpdate =
    MongoUpdate(Updates.setOnInsert(field, value), Update.setOnInsert(field, value))
  def inc(field: String, value: Number): MongoUpdate = MongoUpdate(Updates.inc(field, value), Update.inc(field, value))
  def unset(field: String): MongoUpdate = MongoUpdate(Updates.unset(field), Update.unset(field))
  def push[A](field: String, value: A): MongoUpdate = MongoUpdate(Updates.push(field, value), Update.push(field, value))
  def addToSet[A](field: String, value: A): MongoUpdate =
    MongoUpdate(Updates.addToSet(field, value), Update.addToSet(field, value))
  def combine(updates: MongoUpdate*): MongoUpdate =
    MongoUpdate(
      Updates.combine(updates.map(_.bson).asJava),
      updates.map(_.sessionUpdate).reduce(_.combinedWith(_))
    )
}

private[mongo] object MongoSessionOperations {
  type Documents = MongoCollection[IO, org.bson.Document]

  def findOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter
  ): IO[Option[org.bson.Document]] =
    collection.flatMap(c =>
      session.fold(c.find(filter.bson).first)(active => c.find(active, filter.sessionFilter).first)
    )

  def insertOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      document: org.bson.Document
  ): IO[Option[InsertOneResult]] =
    collection.flatMap(c =>
      session.fold(c.insertOne(document, new InsertOneOptions).map(Some(_)))(active =>
        c.insertOne(active, document, new InsertOneOptions).map(Some(_))
      )
    )

  def updateOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions
  ): IO[Option[UpdateResult]] =
    collection.flatMap(c =>
      session.fold(c.updateOne(filter.bson, update.bson, options).map(Some(_)))(active =>
        c.updateOne(active, filter.sessionFilter, update.sessionUpdate, options).map(Some(_))
      )
    )

  /** mongo4cats exposes session-aware pipeline updates through its bulk write command. */
  def updateOnePipeline(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      pipeline: Seq[Bson],
      options: UpdateOptions
  ): IO[Option[com.mongodb.bulk.BulkWriteResult]] = {
    val commands = List(
      mongo4cats.models.collection.WriteCommand.PipelinedUpdateOne(filter.sessionFilter, pipeline, options)
    )
    val bulkOptions = new com.mongodb.client.model.BulkWriteOptions()
    collection.flatMap(c =>
      session
        .fold(c.bulkWrite(commands, bulkOptions))(active => c.bulkWrite(active, commands, bulkOptions))
        .map(Some(_))
    )
  }

  def updateMany(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions
  ): IO[Option[UpdateResult]] =
    collection.flatMap(c =>
      session.fold(c.updateMany(filter.bson, update.bson, options).map(Some(_)))(active =>
        c.updateMany(active, filter.sessionFilter, update.sessionUpdate, options).map(Some(_))
      )
    )

  def replaceOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      document: org.bson.Document,
      options: ReplaceOptions = new ReplaceOptions
  ): IO[Option[UpdateResult]] =
    collection.flatMap(c =>
      session.fold(c.replaceOne(filter.bson, document, options).map(Some(_)))(active =>
        c.replaceOne(active, filter.sessionFilter, document, options).map(Some(_))
      )
    )
}

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

  /** Preserve retry labels within an active transaction; the owning runner sanitizes final failure. */
  def transactionGuard[A](
      diagnostics: Diagnostics,
      operation: String,
      session: Option[ClientSession[IO]]
  )(result: RepositoryIO[A])(map: Throwable => Either[RepositoryError, A]): RepositoryIO[A] =
    RepositoryIO.fromIOEither(result.value.handleErrorWith {
      case error: MongoException if session.nonEmpty && error.hasErrorLabel("TransientTransactionError") =>
        IO.raiseError(error)
      case error => reportFailure(diagnostics, operation, error).as(map(error))
    })

  def repositoryGuard[A](
      diagnostics: Diagnostics,
      operation: String
  )(result: RepositoryIO[A])(map: Throwable => Either[RepositoryError, A]): RepositoryIO[A] =
    // This boundary observes unexpected driver failures; typed failures pass through unchanged.
    RepositoryIO.fromIOEither(guard(diagnostics, operation)(result.value)(map))
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
      events: IO[MongoCollection[IO, org.bson.Document]],
      session: Option[ClientSession[IO]],
      event: ApplicationEvent
  ): IO[Option[InsertOneResult]] =
    MongoSessionOperations.insertOne(events, session, MongoHiringCodecs.event(event))

}

private[mongo] trait MongoOperationalEventInsertion {
  protected final def insertOperationalEvents(
      outbox: IO[MongoCollection[IO, org.bson.Document]],
      session: Option[ClientSession[IO]],
      events: List[OperationalEventEnvelope],
      now: Instant,
      diagnostics: Diagnostics
  ): RepositoryIO[Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.outbox.insert")(
        MongoOperationalEventInsertion.insertSequence(events, now)(document =>
          RepositoryIO
            .lift(MongoSessionOperations.insertOne(outbox, session, document))
            .subflatMap(MongoRepositorySupport.writeResult(_).void)
        )
      ) {
        case write: MongoWriteException if write.getError.getCode == 11000 => Left(RepositoryError.Conflict)
        case _                                                             => Left(RepositoryError.Unavailable)
      }

}

private[mongo] object MongoOperationalEventInsertion {
  def insertSequence(events: List[OperationalEventEnvelope], now: Instant)(
      write: org.bson.Document => RepositoryIO[Unit]
  ): RepositoryIO[Unit] =
    events.traverse_ { event =>
      for {
        document <- RepositoryIO.fromEither(
          MongoHiringCodecs.outboxRecord(event, now).leftMap(_ => RepositoryError.InvalidStoredData)
        )
        _ <- write(document)
      } yield ()
    }
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
  def byId[A](collection: IO[MongoCollection[IO, org.bson.Document]], ids: List[String])(
      read: org.bson.Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]
  )(diagnostics: Diagnostics): RepositoryIO[List[A]] =
    if (ids.isEmpty) RepositoryIO.fromEither(Right(Nil))
    else
      MongoRepositorySupport
        .repositoryGuard(diagnostics, "repository.findMany")(
          RepositoryIO
            .lift(
              collection
                .flatMap(
                  _.find(MongoFilter.in(MongoFields.Id, ids.distinct).bson)
                    .boundedStream(ids.distinct.size)
                    .compile
                    .toList
                )
            )
            .subflatMap(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
        )(_ => Left(RepositoryError.Unavailable))

  def page[A](
      collection: IO[MongoCollection[IO, org.bson.Document]],
      filter: MongoFilter,
      timestampField: String,
      pageSize: PageSize
  )(read: org.bson.Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A])(
      diagnostics: Diagnostics
  ): RepositoryIO[List[A]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.page")(
        RepositoryIO
          .lift(
            collection
              .flatMap(
                _.find(filter.bson)
                  .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending(MongoFields.Id)))
                  .limit(pageSize.value)
                  .boundedStream(pageSize.value)
                  .compile
                  .toList
              )
          )
          .subflatMap(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      )(_ => Left(RepositoryError.Unavailable))

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

}
