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
import org.bson.Document
import org.bson.conversions.Bson
import com.mongodb.MongoClientSettings

import java.time.Instant
import java.util.UUID

final class MongoAnalyticsErasureAdaptersSpec extends CatsEffectSuite {
  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
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
    val document = AnalyticsMongoRecords.ErasureRequest(
      id,
      fencingVersion = Some(1),
      leaseToken = Some(token),
      leaseUntil = Some(expiry),
      phase = Some("DeltaPurged"),
      progress = Some(17),
      progressKey = Some(key)
    )

    val claim = MongoAnalyticsErasureStoreSupport.decodeClaim(document)
    assertEquals(
      claim,
      Some(ErasureClaim(asAccountSubjectId(id), token, expiry, ErasurePhase.DeltaPurged, 17, key))
    )
    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(document.copy(fencingVersion = Some(0))),
      None
    )
    assertEquals(MongoAnalyticsErasureStoreSupport.decodeClaim(document.copy(phase = Some("unknown"))), None)
  }

  test("missing optional claim fields keep defaults while malformed BSON fails closed") {
    val id = UUID.randomUUID().toString
    val token = UUID.randomUUID().toString
    val expiry = Instant.parse("2026-09-23T12:00:00Z")
    val minimal = AnalyticsMongoRecords.ErasureRequest(
      id,
      fencingVersion = Some(1),
      leaseToken = Some(token),
      leaseUntil = Some(expiry)
    )

    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(minimal),
      Some(ErasureClaim(asAccountSubjectId(id), token, expiry, ErasurePhase.Requested, 0, 0L, 0))
    )
    assertEquals(
      MongoAnalyticsErasureStoreSupport.decodeClaim(minimal.copy(progress = Some(-1))),
      None
    )
  }

  test("failure labels are fixed sanitized values and repaired claims preserve phase checkpoints") {
    assertEquals(
      ErasureFailureCategory.values.toVector.map(_.persistedName),
      Vector("TRANSIENT_STORAGE", "TRANSIENT_SOURCE", "INVALID_STATE", "UNKNOWN")
    )
    val request = AnalyticsMongoRecords.ErasureRequest(
      UUID.randomUUID().toString,
      fencingVersion = Some(1),
      leaseToken = Some(UUID.randomUUID().toString),
      leaseUntil = Some(Instant.parse("2026-09-23T12:00:00Z")),
      phase = Some("DeltaPurged"),
      progress = Some(17),
      progressKey = Some(ErasurePhase.DeltaPurged.ordinal.toLong * ErasurePhase.ProgressPerPhase + 17L),
      attemptCount = Some(7),
      failureCategory = Some("TRANSIENT_STORAGE"),
      repairRequired = Some(true)
    )
    val claim = MongoAnalyticsErasureStoreSupport.decodeClaim(request)
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
