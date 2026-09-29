package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.service.keyretirement.*

import munit.FunSuite
import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}

import java.time.Instant

final class AnalyticsMongoRecordsSpec extends FunSuite {
  private def roundTrip[A](codec: Codec[A], value: A): (A, BsonDocument) = {
    val bson = new BsonDocument()
    codec.encode(
      new BsonDocumentWriter(bson),
      value,
      EncoderContext.builder().isEncodingCollectibleDocument(true).build()
    )
    val reader = new BsonDocumentReader(bson)
    reader.readBsonType()
    val decoded = codec.decode(reader, DecoderContext.builder().build())
    decoded -> bson
  }

  test("erasure request derived codec preserves stored field names and BSON numeric widths") {
    val record = AnalyticsMongoRecords.ErasureRequest(
      _id = "request-1",
      fencingVersion = Some(1),
      state = Some("Processing"),
      progress = Some(12),
      progressKey = Some(1_000_000_012L),
      deltaGeneration = Some(7L),
      kafkaRetentionBarrier = Some(
        AnalyticsMongoRecords.RetentionBarrier(
          "hiring.events",
          Vector(AnalyticsMongoRecords.RetentionPartition(2, 40L))
        )
      )
    )
    val (decoded, bson) = roundTrip(
      AnalyticsMongoRecords.erasureRequestRegistry.get(classOf[AnalyticsMongoRecords.ErasureRequest]),
      record
    )

    assertEquals(decoded, record)
    assertEquals(bson.getString("_id").getValue, "request-1")
    assertEquals(bson.getInt32("fencingVersion").getValue, 1)
    assertEquals(bson.getInt32("progress").getValue, 12)
    assertEquals(bson.getInt64("progressKey").getValue, 1_000_000_012L)
    assertEquals(bson.getInt64("deltaGeneration").getValue, 7L)
    assertEquals(bson.getDocument("kafkaRetentionBarrier").getString("topic").getValue, "hiring.events")
    assertEquals(
      bson
        .getDocument("kafkaRetentionBarrier")
        .getArray("partitions")
        .get(0)
        .asDocument()
        .getInt64("endOffsetExclusive")
        .getValue,
      40L
    )
  }

  test("derived codecs accept missing and null optional fields and reject incompatible BSON types") {
    val codec = AnalyticsMongoRecords.erasureRequestRegistry.get(classOf[AnalyticsMongoRecords.ErasureRequest])
    val missing = new BsonDocument("_id", new org.bson.BsonString("request-2"))
    val decodedMissing = codec.decode(new BsonDocumentReader(missing), DecoderContext.builder().build())
    assertEquals(decodedMissing.progress, None)
    assertEquals(decodedMissing.leaseUntil, None)

    val explicitNull = new BsonDocument("_id", new org.bson.BsonString("request-2"))
      .append("progress", org.bson.BsonNull.VALUE)
    val decodedNull = codec.decode(new BsonDocumentReader(explicitNull), DecoderContext.builder().build())
    assertEquals(decodedNull.progress, None)

    val wrongWidth = new BsonDocument("_id", new org.bson.BsonString("request-2"))
      .append("progressKey", new org.bson.BsonInt32(3))
    intercept[mongo4cats.errors.MongoJsonParsingException] {
      codec.decode(new BsonDocumentReader(wrongWidth), DecoderContext.builder().build())
    }
    val wrongType = new BsonDocument("_id", new org.bson.BsonString("request-2"))
      .append("progress", new org.bson.BsonString("3"))
    intercept[mongo4cats.errors.MongoJsonParsingException] {
      codec.decode(new BsonDocumentReader(wrongType), DecoderContext.builder().build())
    }
    val wrongIntWidth = new BsonDocument("_id", new org.bson.BsonString("request-2"))
      .append("progress", new org.bson.BsonInt64(3L))
    intercept[mongo4cats.errors.MongoJsonParsingException] {
      codec.decode(new BsonDocumentReader(wrongIntWidth), DecoderContext.builder().build())
    }
  }

  test("projected initial erasure claim decodes with absent phase and progress") {
    val requestId = java.util.UUID.randomUUID().toString
    val leaseToken = java.util.UUID.randomUUID().toString
    val codec = AnalyticsMongoRecords.erasureRequestRegistry.get(classOf[AnalyticsMongoRecords.ErasureRequest])
    val record = AnalyticsMongoRecords.ErasureRequest(
      _id = requestId,
      fencingVersion = Some(1),
      leaseToken = Some(leaseToken),
      leaseUntil = Some(Instant.parse("2026-09-29T20:00:00Z"))
    )
    val (decoded, _) = roundTrip(codec, record)
    assert(MongoAnalyticsErasureStoreSupport.decodeClaim(decoded).nonEmpty)
  }

  test("report record validation rejects malformed persisted identities and states") {
    val valid = AnalyticsMongoRecords.ReportRun(
      "batch-1",
      "a" * 64,
      1L,
      2L,
      "Reserved"
    )
    assert(MongoAnalyticsReportRecords.decodeRun(valid).isRight)
    assert(MongoAnalyticsReportRecords.decodeRun(valid.copy(state = "Unexpected")).isLeft)
    assert(MongoAnalyticsReportRecords.decodeRun(valid.copy(_id = " ")).isLeft)
    assert(MongoAnalyticsReportRecords.decodeRun(valid.copy(rangeFingerprint = "bad")).isLeft)
    assert(
      MongoAnalyticsReportRecords
        .decodeControl(
          AnalyticsMongoRecords.ReportControl("unexpected-id", 1L, None, 0L, None, "Active")
        )
        .isLeft
    )
  }

  test("report reservation derived codec keeps small revision counters as BSON Int64") {
    val record = AnalyticsMongoRecords.ReportRun("batch-2", "b" * 64, 1L, 2L, "Reserved")
    val (decoded, bson) = roundTrip(
      AnalyticsMongoRecords.reportRunRegistry.get(classOf[AnalyticsMongoRecords.ReportRun]),
      record
    )
    assertEquals(decoded, record)
    assert(bson.get("generation").isInt64)
    assert(bson.get("revision").isInt64)
  }

  test("HMAC authorization records retain their persisted date and are validated before domain use") {
    val facts = "operator evidence"
    val authorization = HmacKeyRetirementAuthorization(
      "a" * 64,
      "key-1",
      "v" * 43,
      facts,
      HmacKeyRetirementAuthorization.digest(facts),
      Instant.parse("2026-09-27T12:00:00Z")
    )
    val record = AnalyticsMongoRecords.HmacAuthorization(
      s"${authorization.lakehouseId}:${authorization.keyId}",
      authorization.lakehouseId,
      authorization.keyId,
      authorization.originalVerifier,
      authorization.evidenceFacts,
      authorization.evidenceDigest,
      authorization.authorizedAt
    )
    val (decoded, bson) = roundTrip(
      AnalyticsMongoRecords.hmacAuthorizationRegistry.get(classOf[AnalyticsMongoRecords.HmacAuthorization]),
      record
    )
    assertEquals(decoded, record)
    assert(bson.get("authorizedAt").isDateTime)
    assertEquals(
      HmacKeyRetirementAuthorization.validate(
        HmacKeyRetirementAuthorization(
          decoded.lakehouseId,
          decoded.keyId,
          decoded.originalVerifier,
          decoded.evidenceFacts,
          decoded.evidenceDigest,
          decoded.authorizedAt
        )
      ),
      Right(authorization)
    )
  }

  test("HMAC preparation derived record validates nested retention barrier") {
    val record = HmacKeyRetirementPreparation.MongoRecord(
      "a" * 64 + ":key-1",
      "a" * 64,
      "key-1",
      "v" * 43,
      Instant.parse("2026-09-27T12:00:00Z"),
      "hiring.phase6.runtime",
      Vector(HmacKeyRetirementPreparation.PartitionRecord(0, 10L)),
      "cluster-id",
      "topic-id",
      "unit_hmac-rotation-kafka",
      "/var/lib/docker/volumes/unit_hmac-rotation-kafka/_data",
      "2026-09-27T12:00:00Z",
      "127.0.0.1:19093"
    )
    val codec = HmacKeyRetirementPreparation.mongoRecordRegistry.get(classOf[HmacKeyRetirementPreparation.MongoRecord])
    val (decoded, bson) = roundTrip(codec, record)
    assertEquals(
      HmacKeyRetirementPreparation.decode(decoded).map(_.barrier.partitions.head.endOffsetExclusive.asInstanceOf[Long]),
      Right(10L)
    )
    assert(bson.get("capturedAt").isDateTime)
    assert(bson.getArray("partitions").get(0).asDocument().get("endOffsetExclusive").isInt64)
    assert(HmacKeyRetirementPreparation.decode(decoded.copy(partitions = Vector.empty)).isLeft)
  }
}
