package com.example.hiring.analytics
import com.example.hiring.analytics.cli.AnalyticsErasureRepairMain
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import munit.CatsEffectSuite
import io.circe.Json
import org.bson.Document
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.conversions.Bson
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import com.mongodb.MongoClientSettings

import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoAnalyticsErasureAdaptersSpec extends CatsEffectSuite {
  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
  private def asJson(document: Document) = MongoAnalyticsErasureStoreSupport.jsonFromBson(document)
  private val jsonCodec = MongoAnalyticsErasureStoreSupport.jsonRegistry.get(classOf[Json])

  test("Mongo JSON codec preserves application keys that resemble BSON metadata") {
    val value = Json.obj(
      "_id" -> Json.fromString("outbox-record"),
      "$__analyticsBsonInt32Fields" -> Json.arr(Json.fromString("applicationField")),
      "payload" -> Json.obj(
        "$__analyticsBsonInt64Fields" -> Json.obj("nested" -> Json.fromInt(7)),
        "applicationField" -> Json.fromString("kept")
      )
    )
    val bson = new BsonDocument()
    jsonCodec.encode(
      new BsonDocumentWriter(bson),
      value,
      EncoderContext.builder().isEncodingCollectibleDocument(false).build()
    )
    val decoded = jsonCodec.decode(
      new BsonDocumentReader(bson),
      DecoderContext.builder().build()
    )
    assertEquals(decoded, value)
  }

  test("Mongo JSON codec rejects BSON values that cannot be represented as JSON") {
    val bson = new BsonDocument()
    bson.put("unsupported", new org.bson.BsonJavaScript("return 1"))

    intercept[IllegalArgumentException] {
      jsonCodec.decode(
        new BsonDocumentReader(bson),
        DecoderContext.builder().build()
      )
    }
  }

  test("erasure phases have a stable forward-only order") {
    val phases = ErasurePhase.values.toVector
    assertEquals(
      phases,
      Vector(
        ErasurePhase.Requested,
        ErasurePhase.PublisherDrained,
        ErasurePhase.OutboxPurged,
        ErasurePhase.DeltaPurged,
        ErasurePhase.GoldRebuilt,
        ErasurePhase.ReadyToPublish,
        ErasurePhase.ReportPublished
      )
    )
    assert(phases.sliding(2).forall {
      case Vector(previous, next) => previous.next.contains(next)
      case _                      => true
    })
    assertEquals(ErasurePhase.fromString("DeltaPurged"), Some(ErasurePhase.DeltaPurged))
    assertEquals(ErasurePhase.fromString("Complete"), None)
  }

  test("erasure request states retain their stored names and reject unknown values") {
    assertEquals(
      ErasureRequestState.values.toVector.map(_.persistedName),
      Vector("Pending", "Processing", "Complete")
    )
    assertEquals(ErasureRequestState.fromString("Processing"), Some(ErasureRequestState.Processing))
    assertEquals(ErasureRequestState.fromString("unknown"), None)
  }

  test("a claimed request resumes its durable named phase and progress") {
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val expiry = Instant.parse("2026-09-23T12:00:00Z")
    val key = ErasurePhase.DeltaPurged.ordinal.toLong * ErasurePhase.ProgressPerPhase + 17L
    val document = new Document("_id", id)
      .append("fencingVersion", 1)
      .append("leaseToken", token)
      .append("leaseUntil", java.util.Date.from(expiry))
      .append("phase", "DeltaPurged")
      .append("progress", 17)
      .append("progressKey", key)

    val claim = MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(document))
    assertEquals(
      claim,
      Some(ErasureClaim(asAccountSubjectId(id), token, expiry, ErasurePhase.DeltaPurged, 17, key))
    )
    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(new Document(document).append("fencingVersion", 0))),
      None
    )
    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(new Document(document).append("fencingVersion", 1.5d))),
      None
    )
    assertEquals(MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(document.append("phase", "unknown"))), None)
  }

  test("missing optional claim fields keep defaults while malformed BSON fails closed") {
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val expiry = Instant.parse("2026-09-23T12:00:00Z")
    val minimal = new Document("_id", id)
      .append("fencingVersion", 1)
      .append("leaseToken", token)
      .append("leaseUntil", java.util.Date.from(expiry))

    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(minimal)),
      Some(ErasureClaim(asAccountSubjectId(id), token, expiry, ErasurePhase.Requested, 0, 0L, 0))
    )
    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(new Document(minimal).append("progress", "bad"))),
      None
    )
  }

  test("failure labels are fixed sanitized values and repaired claims preserve phase checkpoints") {
    assertEquals(
      ErasureFailureCategory.values.toVector.map(_.persistedName),
      Vector("TRANSIENT_STORAGE", "TRANSIENT_SOURCE", "INVALID_STATE", "UNKNOWN")
    )
    val request = new Document("_id", UUID.randomUUID().toString)
      .append("fencingVersion", 1)
      .append("leaseToken", UUID.randomUUID().toString)
      .append("leaseUntil", java.util.Date.from(Instant.parse("2026-09-23T12:00:00Z")))
      .append("phase", "DeltaPurged")
      .append("progress", 17)
      .append("progressKey", ErasurePhase.DeltaPurged.ordinal.toLong * ErasurePhase.ProgressPerPhase + 17L)
      .append("attemptCount", 7)
      .append("failureCategory", "TRANSIENT_STORAGE")
      .append("repairRequired", true)
    val claim = MongoAnalyticsErasureStoreSupport.decodeClaim(asJson(request))
    assertEquals(claim.map(_.phase), Some(ErasurePhase.DeltaPurged))
    assertEquals(claim.map(_.progress), Some(17))
    assertEquals(claim.map(_.attemptCount), Some(7))
  }

  test("a rejected repair compare-and-set is reported as an unsuccessful command") {
    assertEquals(
      AnalyticsErasureRepairMain.requeueExitCode(ErasureUpdate.Applied),
      cats.effect.ExitCode.Success
    )
    assertEquals(
      AnalyticsErasureRepairMain.requeueExitCode(ErasureUpdate.LeaseLost),
      cats.effect.ExitCode.Error
    )
  }

  test("lease compare-and-set filter binds request, processing state, current token, and unexpired lease") {
    val now = Instant.parse("2026-09-23T11:00:00Z")
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val claim = ErasureClaim(asAccountSubjectId(id), token, now.plusSeconds(60), ErasurePhase.Requested, 0, 0L)
    val rendered = render(MongoAnalyticsErasureStoreSupport.ownedClaimFilter(claim, now))

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
