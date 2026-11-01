package com.example.graphQL.cats.repository.mongo

import cats.data.ValidatedNel
import cats.effect.IO
import cats.syntax.all.*
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.operations.{Filter, Update}
import mongo4cats.client.ClientSession
import com.mongodb.client.model.{
  DeleteOptions,
  FindOneAndUpdateOptions,
  InsertOneOptions,
  ReturnDocument,
  UpdateOptions,
  Updates
}
import com.mongodb.client.result.{DeleteResult, InsertOneResult, UpdateResult}
import com.example.graphQL.cats.domain.model.ApplicationEvent
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.mongodb.{
  MongoBulkWriteException,
  MongoCommandException,
  MongoConnectionPoolClearedException,
  MongoException,
  MongoNodeIsRecoveringException,
  MongoNotPrimaryException,
  MongoSocketException,
  MongoTimeoutException,
  MongoWriteException
}
import com.example.graphQL.cats.shared.CauseChain
import com.mongodb.client.model.{Filters, Sorts}
import org.bson.Document

import scala.jdk.CollectionConverters.*
import java.time.Instant
import org.bson.conversions.Bson
import org.bson.{BsonArray, BsonDateTime, BsonDocument, BsonInt64, BsonString, BsonValue}
import fs2.interop.reactivestreams.*
import scala.concurrent.duration.FiniteDuration

/** Effectfully acquired document collections used by repository adapters. */
private[mongo] object Mongo4catsCollections {
  def documents(database: MongoDatabase[IO], name: String): IO[MongoCollection[IO, Document]] =
    database.getCollection(name).map(_.as[Document])
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
      lt(timestampField, occurredAt.toDate),
      and(
        eq(timestampField, occurredAt.toDate),
        lt(MongoFields.Id, id)
      )
    )

  /** An aggregation expression evaluated per document, for example a comparison against `$$NOW`. */
  def expr(expression: BsonDocument): MongoFilter =
    MongoFilter(Filters.expr(expression), Filter.eq("$expr", expression))
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
  def addToSet[A](field: String, value: A): MongoUpdate =
    MongoUpdate(Updates.addToSet(field, value), Update.addToSet(field, value))
  def combine(updates: MongoUpdate*): MongoUpdate =
    MongoUpdate(
      Updates.combine(updates.map(_.bson).asJava),
      updates.map(_.sessionUpdate).reduce(_.combinedWith(_))
    )
}

/** A single-document update pipeline; `upsert` inserts the document from the filter's equality when none matches. */
private[mongo] final case class PipelineUpdate(filter: MongoFilter, pipeline: Seq[Bson], upsert: Boolean = false)

private[mongo] object MongoSessionOperations {
  type Documents = MongoCollection[IO, Document]

  def findOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      projection: Option[Bson] = None
  ): IO[Option[Document]] =
    collection.flatMap { c =>
      val query = session.fold(c.find(filter.bson))(active => c.find(active, filter.sessionFilter))
      projection.fold(query)(query.projection).first
    }

  def findById(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      id: String,
      projection: Option[Bson] = None
  ): IO[Option[Document]] = {
    val filter = MongoFilter.eq(MongoFields.Id, id)
    collection.flatMap { c =>
      val query = session.fold(c.find(filter.bson))(active => c.find(active, filter.sessionFilter))
      projection.fold(query)(query.projection).first
    }
  }

  /** Reads at most `limit` documents in `_id` order; the stream is bounded to the same limit. */
  def findManyById(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      limit: Int,
      projection: Option[Bson] = None
  ): IO[List[Document]] =
    collection.flatMap { c =>
      val ordered = Sorts.ascending(MongoFields.Id)
      val query = session.fold(c.find(filter.bson))(active => c.find(active, filter.sessionFilter))
      projection.fold(query)(query.projection).sort(ordered).limit(limit).boundedStream(limit).compile.toList
    }

  /** One ordered bulk write of upserting single-document updates: one command for any number of entries. The first
    * failing entry (for example a duplicate key) fails the whole write.
    */
  def upsertEach(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      entries: List[(MongoFilter, MongoUpdate)]
  ): IO[Unit] =
    if (entries.isEmpty) IO.unit
    else {
      val upsert = new UpdateOptions().upsert(true)
      val commands = entries.map { case (filter, update) =>
        mongo4cats.models.collection.WriteCommand.UpdateOne(filter.sessionFilter, update.sessionUpdate, upsert)
      }
      val options = new com.mongodb.client.model.BulkWriteOptions()
      collection
        .flatMap(c => session.fold(c.bulkWrite(commands, options))(active => c.bulkWrite(active, commands, options)))
        .void
    }

  /** One ordered bulk write of single-document pipeline updates (so `$$NOW` is usable): one command for any number of
    * entries; the first failing entry (for example a duplicate key) fails the whole write. An upserting entry cannot
    * carry an `$expr` predicate (the server refuses it), so a lease acquisition pairs a non-upserting entry that holds
    * the expiry predicate with an upserting entry that only matches the document it just leased.
    */
  def pipelineEach(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      entries: List[PipelineUpdate]
  ): IO[Unit] =
    if (entries.isEmpty) IO.unit
    else {
      val commands = entries.map { entry =>
        mongo4cats.models.collection.WriteCommand
          .PipelinedUpdateOne(entry.filter.sessionFilter, entry.pipeline, new UpdateOptions().upsert(entry.upsert))
      }
      val options = new com.mongodb.client.model.BulkWriteOptions()
      collection
        .flatMap(c => session.fold(c.bulkWrite(commands, options))(active => c.bulkWrite(active, commands, options)))
        .void
    }

  def insertOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      document: Document
  ): IO[InsertOneResult] =
    collection.flatMap(c =>
      session.fold(c.insertOne(document, new InsertOneOptions))(active =>
        c.insertOne(active, document, new InsertOneOptions)
      )
    )

  def updateOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions
  ): IO[UpdateResult] =
    collection.flatMap(c =>
      session.fold(c.updateOne(filter.bson, update.bson, options))(active =>
        c.updateOne(active, filter.sessionFilter, update.sessionUpdate, options)
      )
    )

  /** mongo4cats exposes session-aware pipeline updates through its bulk write command. */
  def updateOnePipeline(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      pipeline: Seq[Bson],
      options: UpdateOptions
  ): IO[com.mongodb.bulk.BulkWriteResult] = {
    val commands = List(
      mongo4cats.models.collection.WriteCommand.PipelinedUpdateOne(filter.sessionFilter, pipeline, options)
    )
    val bulkOptions = new com.mongodb.client.model.BulkWriteOptions()
    collection.flatMap(c =>
      session.fold(c.bulkWrite(commands, bulkOptions))(active => c.bulkWrite(active, commands, bulkOptions))
    )
  }

  def updateMany(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: UpdateOptions = new UpdateOptions
  ): IO[UpdateResult] =
    collection.flatMap(c =>
      session.fold(c.updateMany(filter.bson, update.bson, options))(active =>
        c.updateMany(active, filter.sessionFilter, update.sessionUpdate, options)
      )
    )

  def findOneAndUpdate(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      update: MongoUpdate,
      options: FindOneAndUpdateOptions = new FindOneAndUpdateOptions
  ): IO[Option[Document]] =
    collection.flatMap(c =>
      session.fold(c.findOneAndUpdate(filter.bson, update.bson, options))(active =>
        c.findOneAndUpdate(active, filter.sessionFilter, update.sessionUpdate, options)
      )
    )

  /** `findOneAndUpdate` with an update pipeline (so `$$NOW` is usable); the collection keeps its write concern. */
  def findOneAndUpdatePipeline(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      pipeline: Seq[Bson],
      options: FindOneAndUpdateOptions
  ): IO[Option[Document]] =
    collection.flatMap { c =>
      val publisher = session.fold(c.underlying.findOneAndUpdate(filter.bson, pipeline.asJava, options))(active =>
        c.underlying.findOneAndUpdate(active.underlying, filter.bson, pipeline.asJava, options)
      )
      publisher.toStreamBuffered[IO](bufferSize = 1).compile.last
    }

  /** `updateMany` with an update pipeline (so `$$NOW` is usable); the collection keeps its write concern. */
  def updateManyPipeline(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      pipeline: Seq[Bson],
      options: UpdateOptions = new UpdateOptions
  ): IO[UpdateResult] =
    collection.flatMap { c =>
      val publisher = session.fold(c.underlying.updateMany(filter.bson, pipeline.asJava, options))(active =>
        c.underlying.updateMany(active.underlying, filter.bson, pipeline.asJava, options)
      )
      publisher.toStreamBuffered[IO](bufferSize = 1).compile.lastOrError
    }

  def deleteOne(
      collection: IO[Documents],
      session: Option[ClientSession[IO]],
      filter: MongoFilter,
      options: DeleteOptions = new DeleteOptions
  ): IO[DeleteResult] =
    collection.flatMap(c =>
      session.fold(c.deleteOne(filter.bson, options))(active => c.deleteOne(active, filter.sessionFilter, options))
    )
}

/** Shared lease semantics for durable work queues (embedding work, search-session work, the event outbox, interview
  * commands).
  *
  * The database clock decides every lease: a lease is held while `leaseUntil > $$NOW` and reclaimable once
  * `leaseUntil <= $$NOW`, and a claim or renewal stamps `leaseUntil = $$NOW + duration` in an update pipeline. No
  * caller instant takes part in an ownership decision, so skewed application clocks cannot reclaim a live lease or
  * displace its holder. `availableAt` and retry backoff stay caller-supplied scheduling values. State names and sort
  * orders stay repository-owned because stored values differ per queue.
  */
private[mongo] object MongoLeaseQueue {
  private val DatabaseNow = new BsonString("$$NOW")

  private def literal(value: String): BsonDocument = new BsonDocument("$literal", new BsonString(value))

  /** `$$NOW + duration` as a BSON date, evaluated by the server. */
  private def databaseNowPlus(duration: FiniteDuration): BsonDocument =
    new BsonDocument("$add", new BsonArray(List[BsonValue](DatabaseNow, new BsonInt64(duration.toMillis)).asJava))

  /** A pipeline of one `$set` stage. */
  def setStage(fields: (String, BsonValue)*): Seq[Bson] =
    List(new BsonDocument("$set", fields.foldLeft(new BsonDocument())((doc, field) => doc.append(field._1, field._2))))

  /** Stored names of a queue's lease: most queues use `leaseOwner`/`leaseToken`/`leaseUntil`. */
  final case class LeaseFields(owner: String, token: String, until: String) {

    private def compareToNow(operator: String): MongoFilter =
      MongoFilter.expr(
        new BsonDocument(
          operator,
          new BsonArray(List[BsonValue](new BsonString("$" + until), DatabaseNow).asJava)
        )
      )

    /** The lease is held while its expiry is after the database clock. */
    val held: MongoFilter = compareToNow("$gt")

    /** The lease is reclaimable once its expiry is at or before the database clock. */
    val expired: MongoFilter = compareToNow("$lte")

    /** Clears the lease so the work is claimable again; combined with the queue's own state change. */
    val release: MongoUpdate =
      MongoUpdate.combine(MongoUpdate.unset(owner), MongoUpdate.unset(token), MongoUpdate.unset(until))

    /** The `$set` fields of a claim: holder, fencing token and `$$NOW + duration` as expiry. */
    def stamp(workerId: String, leaseToken: String, duration: FiniteDuration): List[(String, BsonValue)] =
      List(
        owner -> literal(workerId),
        token -> literal(leaseToken),
        until -> databaseNowPlus(duration)
      )

    /** The `$set` field of a renewal. */
    def extension(duration: FiniteDuration): (String, BsonValue) = until -> databaseNowPlus(duration)
  }

  val Standard: LeaseFields = LeaseFields(MongoFields.LeaseOwner, MongoFields.LeaseToken, MongoFields.LeaseUntil)

  val leaseHeld: MongoFilter = Standard.held

  /** Ready work that is due at the caller's `now` (`availableAt` is a caller scheduling value). */
  def readyDue(readyStates: List[String], now: Instant): MongoFilter =
    MongoFilter.and(
      MongoFilter.in(MongoFields.State, readyStates),
      MongoFilter.lte(MongoFields.AvailableAt, now.toDate)
    )

  /** In-progress work whose lease has expired on the database clock. */
  def leaseExpired(inProgressState: String): MongoFilter =
    MongoFilter.and(MongoFilter.eq(MongoFields.State, inProgressState), Standard.expired)

  /** Work is claimable when it is available or when its in-progress lease has expired on the database clock. */
  def claimable(available: MongoFilter, inProgress: MongoFilter): MongoFilter =
    MongoFilter.or(available, MongoFilter.and(inProgress, Standard.expired))

  val releaseLease: MongoUpdate = Standard.release

  /** The claim update: state, holder, token, `leaseUntil = $$NOW + duration` and the caller's `updatedAt`, plus the
    * queue's own `extra` fields, as one pipeline stage.
    */
  def leaseStamp(
      inProgressState: String,
      workerId: String,
      leaseToken: String,
      duration: FiniteDuration,
      now: Instant,
      extra: (String, BsonValue)*
  ): Seq[Bson] =
    setStage(
      (List[(String, BsonValue)](MongoFields.State -> literal(inProgressState)) ++
        Standard.stamp(workerId, leaseToken, duration) ++
        List[(String, BsonValue)](MongoFields.UpdatedAt -> new BsonDateTime(now.toEpochMilli)) ++ extra)*
    )

  /** Atomically claims the first claimable document in `sort` order and decodes the post-claim document. */
  def claimNext[A](
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      claimable: MongoFilter,
      claim: Seq[Bson],
      sort: Bson
  )(decode: Document => RepositoryF[IO, A]): RepositoryF[IO, Option[A]] =
    RepositoryF
      .lift(
        MongoSessionOperations.findOneAndUpdatePipeline(
          collection,
          session,
          claimable,
          claim,
          new FindOneAndUpdateOptions().sort(sort).returnDocument(ReturnDocument.AFTER)
        )
      )
      .flatMap(_.traverse(decode))

  /** The shared claim of a state-machine queue (`Ready`/`Retry` work that is due, or `Processing` work whose lease
    * expired): validates the request, stamps the lease with `token` and decodes the claimed document. Queues differ
    * only in their states, sort order and decoder.
    */
  def claimReady[A](
      collection: IO[MongoSessionOperations.Documents],
      readyStates: List[String],
      inProgressState: String,
      workerId: String,
      token: String,
      now: Instant,
      duration: FiniteDuration,
      sort: Bson
  )(decode: Document => RepositoryF[IO, A]): RepositoryF[IO, Option[A]] =
    LeaseRequest.whenValid(workerId, duration) {
      claimNext(
        collection,
        None,
        claimable(readyDue(readyStates, now), MongoFilter.eq(MongoFields.State, inProgressState)),
        leaseStamp(inProgressState, workerId, token, duration, now),
        sort
      )(decode)
    }

  /** Extends a lease to `$$NOW + duration` only while `held` still matches; `false` reports a lost lease without
    * failing.
    */
  def renewHeld(
      collection: IO[MongoSessionOperations.Documents],
      session: Option[ClientSession[IO]],
      held: MongoFilter,
      duration: FiniteDuration,
      leaseFields: LeaseFields = Standard
  ): RepositoryF[IO, Boolean] =
    RepositoryF
      .lift(
        MongoSessionOperations
          .updateOnePipeline(collection, session, held, setStage(leaseFields.extension(duration)), new UpdateOptions)
      )
      .map(_.getMatchedCount == 1)
}

private[mongo] object MongoRepositorySupport {

  /** A guarded single-document transition: exactly one match succeeds, a lost guard is `mismatch`. */
  def matchedOne(
      result: UpdateResult,
      mismatch: RepositoryError = RepositoryError.Conflict
  ): Either[RepositoryError, Unit] =
    Either.cond(result.getMatchedCount == 1L, (), mismatch)

  /** A guarded single-document delete: exactly one deletion succeeds, a lost guard is `mismatch`. */
  def deletedOne(
      result: DeleteResult,
      mismatch: RepositoryError = RepositoryError.Conflict
  ): Either[RepositoryError, Unit] =
    Either.cond(result.getDeletedCount == 1L, (), mismatch)

  def reportFailure(diagnostics: Diagnostics[IO], operation: String, error: Throwable): IO[Unit] =
    diagnostics.emit(
      LogEvent.MongoRepositoryFailed,
      fields = LogFields.failure(error) + (LogField.SpanName -> operation)
    )

  /** Reports an unexpected failure and maps it. A failure carrying the transient-transaction label (write conflict or
    * lost transaction state) is re-raised unreported: it can only occur inside a transaction, and only the owning
    * `MongoTransactionRunner` may retry the whole transaction and finally classify it. Mapping it here would turn a
    * retriable conflict into `Unavailable` and count it against the dependency breaker.
    */
  def guard[A](diagnostics: Diagnostics[IO], operation: String)(result: IO[A])(map: Throwable => A): IO[A] =
    result.handleErrorWith { error =>
      // A retriable conflict is routine under contention: only the owning runner logs it, once, when retries run out.
      if (MongoErrors.isTransient(error)) IO.raiseError[A](error)
      else reportFailure(diagnostics, operation, error).as(map(error))
    }

  /** Cleanup variant that never re-raises; used where a failure must not replace the primary outcome. */
  def guardCleanup(diagnostics: Diagnostics[IO], operation: String)(result: IO[Unit]): IO[Unit] =
    result.handleErrorWith(reportFailure(diagnostics, operation, _))

  /** Preserve retry labels within an active transaction; the owning runner sanitizes final failure. */
  def transactionGuard[A](
      diagnostics: Diagnostics[IO],
      operation: String,
      session: Option[ClientSession[IO]]
  )(result: RepositoryF[IO, A])(map: Throwable => Either[RepositoryError, A]): RepositoryF[IO, A] =
    RepositoryF.fromFEither(result.value.handleErrorWith {
      case error: MongoException if session.nonEmpty && MongoErrors.isTransient(error) =>
        IO.raiseError(error)
      case error => reportFailure(diagnostics, operation, error).as(map(error))
    })

  def transactionGuard[A](diagnostics: Diagnostics[IO], operation: String, session: Option[ClientSession[IO]])(
      result: RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    transactionGuard(diagnostics, operation, session)(result)(unavailable)

  def repositoryGuard[A](
      diagnostics: Diagnostics[IO],
      operation: String
  )(result: RepositoryF[IO, A])(map: Throwable => Either[RepositoryError, A]): RepositoryF[IO, A] =
    // This boundary observes unexpected driver failures; typed failures pass through unchanged.
    RepositoryF.fromFEither(guard(diagnostics, operation)(result.value)(map))

  /** Default boundary: an unexpected driver failure is reported and surfaces as `Unavailable`. */
  def repositoryGuard[A](diagnostics: Diagnostics[IO], operation: String)(
      result: RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    repositoryGuard(diagnostics, operation)(result)(unavailable)

  /** Like `repositoryGuard`, but failures that count against the discovery breaker propagate to the policy boundary,
    * which classifies and maps them once.
    */
  def discoveryGuard[A](diagnostics: Diagnostics[IO], operation: String)(
      result: RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    RepositoryF.fromFEither(result.value.handleErrorWith { error =>
      reportFailure(diagnostics, operation, error) *>
        (if (DiscoveryQueryPolicy.countsAgainstBreaker(error)) IO.raiseError(error)
         else IO.pure(unavailable[A](error)))
    })

  def unavailable[A](@annotation.unused error: Throwable): Either[RepositoryError, A] = Left(
    RepositoryError.Unavailable
  )
}

/** Matches the MongoDB duplicate-key write failure (server error code 11000), single or inside a bulk write. */
private[mongo] object MongoDuplicateKey {
  val Code: Int = 11000

  def unapply(error: Throwable): Option[MongoException] = error match {
    case write: MongoWriteException if write.getError.getCode == Code                           => Some(write)
    case bulk: MongoBulkWriteException if bulk.getWriteErrors.asScala.exists(_.getCode == Code) => Some(bulk)
    case _                                                                                      => None
  }
}

/** The single classification of MongoDB write failures into repository outcomes. */
private[mongo] object MongoErrors {
  val TransientTransactionErrorLabel = "TransientTransactionError"
  val UnknownTransactionCommitResultLabel = "UnknownTransactionCommitResult"

  def isTransient(error: Throwable): Boolean = error match {
    case mongo: MongoException => mongo.hasErrorLabel(TransientTransactionErrorLabel)
    case _                     => false
  }

  def isUnknownCommitResult(error: Throwable): Boolean = error match {
    case mongo: MongoException => mongo.hasErrorLabel(UnknownTransactionCommitResultLabel)
    case _                     => false
  }

  /** Only an unknown commit result that is not also transient is retried as a commit; both labels retry the
    * transaction.
    */
  def isCommitRetriable(error: Throwable): Boolean = isUnknownCommitResult(error) && !isTransient(error)

  /** Connection, socket (including read timeout), server-selection, pool and no-primary failures: the deployment cannot
    * be reached. Classified by exception type, never by label: the driver also labels these as transient inside a
    * transaction. Command errors (write conflict, duplicate key, `maxTime` expiry) are never unavailability.
    */
  def isDependencyUnavailable(error: Throwable): Boolean =
    CauseChain(error).exists {
      case _: MongoTimeoutException | _: MongoSocketException | _: MongoNotPrimaryException |
          _: MongoNodeIsRecoveringException | _: MongoConnectionPoolClearedException =>
        true
      case _ => false
    }

  def isDuplicateKey(error: Throwable): Boolean = MongoDuplicateKey.unapply(error).nonEmpty

  def isWriteConflict(error: MongoCommandException): Boolean =
    error.getErrorCode == 112 || error.hasErrorLabel(TransientTransactionErrorLabel)

  /** Duplicate key is `duplicateKeyError`, any write conflict or transient label is `transientError`. */
  def toRepositoryError[A](
      error: Throwable,
      duplicateKeyError: RepositoryError,
      transientError: RepositoryError = RepositoryError.Conflict
  ): Either[RepositoryError, A] =
    error match {
      case MongoDuplicateKey(_)                                       => Left(duplicateKeyError)
      case unreachable if isDependencyUnavailable(unreachable)        => Left(RepositoryError.Unavailable)
      case command: MongoCommandException if isWriteConflict(command) => Left(transientError)
      case mongo: MongoException if isTransient(mongo)                => Left(transientError)
      case _                                                          => Left(RepositoryError.Unavailable)
    }

  /** Duplicate key is a `Conflict`; every other failure is `Unavailable`. */
  def duplicateAsConflict[A](error: Throwable): Either[RepositoryError, A] =
    if (isDuplicateKey(error)) Left(RepositoryError.Conflict) else Left(RepositoryError.Unavailable)

  /** Duplicate key is `duplicateKeyError`; only command write conflicts are `Conflict`. */
  def duplicateAs[A](duplicateKeyError: RepositoryError)(error: Throwable): Either[RepositoryError, A] =
    error match {
      case MongoDuplicateKey(_)                                       => Left(duplicateKeyError)
      case command: MongoCommandException if isWriteConflict(command) => Left(RepositoryError.Conflict)
      case _                                                          => Left(RepositoryError.Unavailable)
    }
}

private[mongo] object MongoApplicationEventInsertion {
  def insert(
      events: IO[MongoCollection[IO, Document]],
      session: Option[ClientSession[IO]],
      event: ApplicationEvent
  ): IO[InsertOneResult] =
    MongoSessionOperations.insertOne(events, session, MongoHiringCodecs.event(event))
}

private[mongo] object MongoOperationalEventInsertion {
  def insert(
      outbox: IO[MongoCollection[IO, Document]],
      session: Option[ClientSession[IO]],
      events: List[OperationalEventEnvelope],
      now: Instant,
      diagnostics: Diagnostics[IO]
  ): RepositoryF[IO, Unit] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.outbox.insert")(
        insertSequence(events, now)(document =>
          RepositoryF.lift(MongoSessionOperations.insertOne(outbox, session, document).void)
        )
      )(MongoErrors.duplicateAsConflict)

  def insertSequence(events: List[OperationalEventEnvelope], now: Instant)(
      write: Document => RepositoryF[IO, Unit]
  ): RepositoryF[IO, Unit] =
    MongoF
      .fromEither(
        events.traverse { event =>
          com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec
            .validate(event)
            .leftMap(_ => RepositoryError.InvalidEvent)
            .flatMap(valid => MongoHiringCodecs.outboxRecord(valid, now).leftMap(_ => RepositoryError.InvalidEvent))
        }
      )
      .flatMap(_.traverse_(write))
}

private[mongo] object MongoStoredDocumentDecoding {

  extension [A](decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]) {

    /** The one place where a decode failure becomes the repository's `InvalidStoredData`. Accumulation is internal to
      * the codec (`ValidatedNel` keeps every `StoredDocumentError` until here, so none is dropped by an earlier
      * `.head`); the detail is deliberately not surfaced past this point. The read fails closed, and neither the error,
      * the field names nor any stored text is logged or returned.
      */
    def toStored: Either[RepositoryError, A] = decoded.toEither.leftMap(_ => RepositoryError.InvalidStoredData)
  }

  extension [A](decoded: Either[MongoHiringCodecs.StoredDocumentError, A]) {

    /** The same collapse for a single field read. */
    def toStored: Either[RepositoryError, A] = decoded.leftMap(_ => RepositoryError.InvalidStoredData)
  }

  def repository[A](decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]): Either[RepositoryError, A] =
    decoded.toStored

  def optional[A](
      decoded: ValidatedNel[MongoHiringCodecs.StoredDocumentError, Option[A]]
  ): Either[RepositoryError, Option[A]] =
    repository(decoded)

  def values[A](
      decoded: List[ValidatedNel[MongoHiringCodecs.StoredDocumentError, A]]
  ): Either[RepositoryError, List[A]] =
    repository(decoded.sequence)
}

private[mongo] object MongoRevisionReads {

  /** The stored revision of one document, reading nothing else. */
  def revision(
      collection: IO[MongoCollection[IO, Document]],
      id: String
  ): RepositoryF[IO, Option[Long]] =
    RepositoryF
      .lift(MongoSessionOperations.findById(collection, None, id, Some(MongoReadProjections.revision)))
      .subflatMap(
        _.traverse(document =>
          MongoDocumentFields.toRepository(MongoDocumentFields.requiredInt64(document, MongoFields.Version, min = 0L))
        )
      )
}

private[mongo] object MongoKeysetPaging {
  def page[A](
      collection: IO[MongoCollection[IO, Document]],
      filter: MongoFilter,
      timestampField: String,
      pageSize: PageSize,
      projection: Option[Bson] = None
  )(read: Document => ValidatedNel[MongoHiringCodecs.StoredDocumentError, A])(
      diagnostics: Diagnostics[IO]
  ): RepositoryF[IO, List[A]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "repository.page")(
        RepositoryF
          .lift(
            collection
              .flatMap { c =>
                val query = c
                  .find(filter.bson)
                  .sort(Sorts.orderBy(Sorts.descending(timestampField), Sorts.descending(MongoFields.Id)))
                  .limit(pageSize.value)
                projection.fold(query)(query.projection).boundedStream(pageSize.value).compile.toList
              }
          )
          .subflatMap(documents => MongoStoredDocumentDecoding.values(documents.map(read)))
      )

  def filter(filters: List[Option[Bson]]): Bson =
    Filters.and(filters.flatten*)

}
