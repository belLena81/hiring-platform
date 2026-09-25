package com.example.hiring.analytics

import munit.CatsEffectSuite
import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.MongoClientSettings

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoAnalyticsErasureWorkerStoreSpec extends CatsEffectSuite {
  test("erasure phases have a stable forward-only order") {
    val phases = ErasurePhase.values.toVector
    assertEquals(phases, Vector(
      ErasurePhase.Requested,
      ErasurePhase.PublisherDrained,
      ErasurePhase.OutboxPurged,
      ErasurePhase.DeltaPurged,
      ErasurePhase.GoldRebuilt,
      ErasurePhase.ReadyToPublish,
      ErasurePhase.ReportPublished
    ))
    assert(phases.sliding(2).forall {
      case Vector(previous, next) => previous.precedes(next)
      case _                      => true
    })
    assertEquals(ErasurePhase.fromString("DeltaPurged"), Some(ErasurePhase.DeltaPurged))
    assertEquals(ErasurePhase.fromString("Complete"), None)
  }

  test("a claimed request resumes its durable named phase and progress") {
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val expiry = Instant.parse("2026-09-23T12:00:00Z")
    val key = ErasurePhase.DeltaPurged.ordinal.toLong * MongoAnalyticsErasureWorkerStore.ProgressPerPhase + 17L
    val document = new Document("_id", id)
      .append("fencingVersion", 1)
      .append("leaseToken", token)
      .append("leaseUntil", java.util.Date.from(expiry))
      .append("phase", "DeltaPurged")
      .append("progress", 17)
      .append("progressKey", key)

    val claim = MongoAnalyticsErasureWorkerStore.decodeClaim(document)
    assertEquals(
      claim,
      Some(ErasureClaim(id, token, expiry, ErasurePhase.DeltaPurged, 17, key))
    )
    assertEquals(MongoAnalyticsErasureWorkerStore.decodeClaim(new Document(document).append("fencingVersion", 0)), None)
    assertEquals(MongoAnalyticsErasureWorkerStore.decodeClaim(document.append("phase", "unknown")), None)
  }

  test("lease compare-and-set filter binds request, processing state, current token, and unexpired lease") {
    val now = Instant.parse("2026-09-23T11:00:00Z")
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val claim = ErasureClaim(id, token, now.plusSeconds(60), ErasurePhase.Requested, 0, 0L)
    val rendered = render(MongoAnalyticsErasureWorkerStore.ownedClaimFilter(claim, now))

    val json = rendered.toJson
    assert(json.contains(id))
    assert(json.contains("Processing"))
    assert(json.contains(token))
    assert(json.contains("leaseUntil"))
    assert(json.contains("$gt"))
  }

  private def render(filter: Bson): org.bson.BsonDocument =
    filter.toBsonDocument(classOf[Document], MongoClientSettings.getDefaultCodecRegistry)
}
