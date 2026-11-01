package com.example.graphQL.cats.repository.mongo

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.operations.Filter
import munit.FunSuite
import org.bson.conversions.Bson
import org.bson.{BsonDocument, BsonValue}

import java.time.Instant
import java.util.Date
import scala.concurrent.duration.*

/** DDR-01: lease expiry is decided by the database clock; no caller instant appears in a lease predicate or stamp. */
final class MongoLeaseQueueSpec extends FunSuite {
  private val now = Instant.parse("2026-10-08T12:00:00Z")
  private val available = MongoFilter.and(
    MongoFilter.in(MongoFields.State, List("Ready", "Retry")),
    MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
  )
  private val inProgress = MongoFilter.eq(MongoFields.State, "Processing")

  private def document(bson: Bson): BsonDocument =
    bson.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry)

  private def json(text: String): BsonDocument = BsonDocument.parse(text)

  private def databaseNowComparison(operator: String, field: String): BsonDocument =
    json(s"""{"$$expr": {"$operator": ["$$$field", "$$$$NOW"]}}""")

  test("DDR-01.1 a lease is held while its expiry is after the database clock and reclaimable at or before it") {
    assertEquals(document(MongoLeaseQueue.leaseHeld.bson), databaseNowComparison("$gt", "leaseUntil"))
    assertEquals(document(MongoLeaseQueue.Standard.expired.bson), databaseNowComparison("$lte", "leaseUntil"))
  }

  test("DDR-01.1 the database-clock predicate has the same session encoding as its raw encoding") {
    val expression = json("""{"$lte": ["$leaseUntil", "$$NOW"]}""")
    assertEquals(MongoLeaseQueue.Standard.expired.sessionFilter, Filter.eq("$expr", expression))
  }

  test("DDR-01.3 claimable work is available work or in-progress work whose lease expired on the database clock") {
    val claimable = MongoLeaseQueue.claimable(available, inProgress)
    val expected = Filters.or(
      available.bson,
      Filters.and(Filters.eq("state", "Processing"), Filters.expr(json("""{"$lte": ["$leaseUntil", "$$NOW"]}""")))
    )
    assertEquals(document(claimable.bson), document(expected))
  }

  test("DDR-01.1 a claim stamps state, owner, token, database-time expiry and updatedAt in one pipeline stage") {
    val stamp = MongoLeaseQueue.leaseStamp("Processing", "worker-1", "token-1", 30.seconds, now)
    val expected = json(
      s"""{"$$set": {
         |  "state": {"$$literal": "Processing"},
         |  "leaseOwner": {"$$literal": "worker-1"},
         |  "leaseToken": {"$$literal": "token-1"},
         |  "leaseUntil": {"$$add": ["$$$$NOW", {"$$numberLong": "30000"}]},
         |  "updatedAt": {"$$date": {"$$numberLong": "${now.toEpochMilli}"}}
         |}}""".stripMargin
    )
    assertEquals(stamp.map(document), List(expected))
  }

  test("DDR-01.1 a worker id that looks like a field path is stored as a literal, not evaluated") {
    val stamp = MongoLeaseQueue.leaseStamp("Processing", "$state", "$$NOW", 1.second, now)
    val set = document(stamp.head).getDocument("$set")
    assertEquals(set.getDocument("leaseOwner"), json("""{"$literal": "$state"}"""))
    assertEquals(set.getDocument("leaseToken"), json("""{"$literal": "$$NOW"}"""))
  }

  test("DDR-01.1 a lease stored under other field names stamps, holds and releases exactly those names") {
    val fields = MongoLeaseQueue.LeaseFields("claimOwner", "claimToken", "claimUntil")
    val stamped: Map[String, BsonValue] = fields.stamp("worker-1", "token-1", 30.seconds).toMap
    assertEquals(stamped.keySet, Set("claimOwner", "claimToken", "claimUntil"))
    assertEquals(stamped("claimUntil"), json("""{"$add": ["$$NOW", {"$numberLong": "30000"}]}"""))
    assertEquals(fields.extension(5.seconds)._1, "claimUntil")
    assertEquals(
      document(fields.release.bson),
      document(Updates.combine(Updates.unset("claimOwner"), Updates.unset("claimToken"), Updates.unset("claimUntil")))
    )
    assertEquals(document(fields.held.bson), databaseNowComparison("$gt", "claimUntil"))
    assertEquals(document(fields.expired.bson), databaseNowComparison("$lte", "claimUntil"))
  }
}
