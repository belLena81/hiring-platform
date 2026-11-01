package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.Date

/** Lease expiry is decided by the database clock, so tests read that clock and move a stored lease, never a caller
  * instant. The test replica set runs on this host, so a stored expiry is compared with the database time around the
  * write rather than with the client clock.
  */
private[mongo] object LeaseTestSupport {

  /** The database server's current time (`hello.localTime`). */
  def databaseTime(database: MongoDatabase[IO]): IO[Instant] =
    MongoAccessEvaluationSupport
      .command(database, new Document("hello", 1))
      .map(_.getDate("localTime").toInstant)

  /** The stored expiry of one document. */
  def storedExpiry(
      database: MongoDatabase[IO],
      collection: String,
      id: String,
      field: String = MongoFields.LeaseUntil
  ): IO[Instant] =
    MongoRepositoryTestSupport
      .findOne(database, collection, Filters.eq(MongoFields.Id, id))
      .flatMap(IO.fromOption(_)(new AssertionError(s"Missing $collection/$id")))
      .map(_.getDate(field).toInstant)

  /** Runs `claim` and returns its result with the database time just before and just after it. */
  def withinDatabaseWindow[A](database: MongoDatabase[IO])(claim: IO[A]): IO[(A, Instant, Instant)] =
    for {
      before <- databaseTime(database)
      value <- claim
      after <- databaseTime(database)
    } yield (value, before, after)

  /** Moves a stored lease to the distant past (as if its holder stopped renewing): reclaimable, and a renewal is lost.
    */
  def expire(
      database: MongoDatabase[IO],
      collection: String,
      id: String,
      field: String = MongoFields.LeaseUntil
  ): IO[Unit] =
    set(database, collection, id, field, Date.from(Instant.parse("2000-01-01T00:00:00Z")))

  /** Expires the lease of every document matching `filter` (a holder that stopped renewing, found by a field). */
  def expireWhere(
      database: MongoDatabase[IO],
      collection: String,
      filter: org.bson.conversions.Bson,
      field: String
  ): IO[Unit] =
    MongoRepositoryTestSupport
      .collection(database, collection)
      .flatMap(_.updateMany(filter, Updates.set(field, Date.from(Instant.parse("2000-01-01T00:00:00Z")))))
      .void

  /** Moves a stored lease far into the future (as if its holder renewed it), whatever the caller clock says. */
  def extend(
      database: MongoDatabase[IO],
      collection: String,
      id: String,
      field: String = MongoFields.LeaseUntil
  ): IO[Unit] =
    set(database, collection, id, field, Date.from(Instant.parse("2100-01-01T00:00:00Z")))

  /** Pins a stored expiry to an instant, to shape a fixture the report-time classification reads. */
  def setExpiry(
      database: MongoDatabase[IO],
      collection: String,
      id: String,
      at: Instant,
      field: String = MongoFields.LeaseUntil
  ): IO[Unit] = set(database, collection, id, field, Date.from(at))

  private def set(database: MongoDatabase[IO], collection: String, id: String, field: String, to: Date): IO[Unit] =
    MongoRepositoryTestSupport
      .collection(database, collection)
      .flatMap(_.updateOne(Filters.eq(MongoFields.Id, id), Updates.set(field, to)))
      .flatMap(result =>
        IO.raiseWhen(result.getMatchedCount != 1L)(new AssertionError(s"Missing $collection/$id to move its lease"))
      )
}
