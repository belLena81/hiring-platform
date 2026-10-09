package com.example.graphQL.cats.repository.mongo

import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.operations.{Filter, Update}
import munit.FunSuite
import org.bson.BsonDocument
import org.bson.conversions.Bson

import java.time.Instant
import java.util.Date

final class MongoLeaseQueueSpec extends FunSuite {
  private val now = Instant.parse("2026-10-08T12:00:00Z")
  private val leaseUntil = now.plusSeconds(30)
  private val available = MongoFilter.and(
    MongoFilter.in(MongoFields.State, List("Ready", "Retry")),
    MongoFilter.lte(MongoFields.AvailableAt, Date.from(now))
  )
  private val inProgress = MongoFilter.eq(MongoFields.State, "Processing")

  private def document(bson: Bson): BsonDocument =
    bson.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry)

  test("a lease is held strictly after its expiry instant and reclaimable at or before it") {
    assertEquals(document(MongoLeaseQueue.leaseHeld(now).bson), document(Filters.gt("leaseUntil", Date.from(now))))
    assertEquals(
      document(MongoLeaseQueue.leaseExpired(now).bson),
      document(Filters.lte("leaseUntil", Date.from(now)))
    )
  }

  test("claimable work is available work or in-progress work whose lease expired at the claim instant") {
    val expected = Filters.or(
      available.bson,
      Filters.and(Filters.eq("state", "Processing"), Filters.lte("leaseUntil", Date.from(now)))
    )
    val claimable = MongoLeaseQueue.claimable(available, inProgress, now)
    assertEquals(document(claimable.bson), document(expected))
    assertEquals(
      claimable.sessionFilter,
      Filter.or(available.sessionFilter, Filter.and(inProgress.sessionFilter, Filter.lte("leaseUntil", Date.from(now))))
    )
  }

  test("a claim stamps state, owner, token, lease expiry and updatedAt in both update encodings") {
    val expected = Updates.combine(
      Updates.set("state", "Processing"),
      Updates.set("leaseOwner", "worker-1"),
      Updates.set("leaseToken", "token-1"),
      Updates.set("leaseUntil", Date.from(leaseUntil)),
      Updates.set("updatedAt", Date.from(now))
    )
    val stamp = MongoLeaseQueue.leaseStamp("Processing", "worker-1", "token-1", leaseUntil, now)
    assertEquals(document(stamp.bson), document(expected))
    assertEquals(
      stamp.sessionUpdate,
      Update
        .set("state", "Processing")
        .set("leaseOwner", "worker-1")
        .set("leaseToken", "token-1")
        .set("leaseUntil", Date.from(leaseUntil))
        .set("updatedAt", Date.from(now))
    )
  }
}
