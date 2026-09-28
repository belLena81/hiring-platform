package com.example.hiring.analytics
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

import com.example.hiring.analytics.adapter.mongo.{BsonDecoder, BsonValueDecoder, MongoAnalyticsReportRecords}
import com.example.hiring.analytics.adapter.mongo.HmacKeyRetirementAuthorizationBson.given
import com.example.hiring.analytics.adapter.mongo.MongoPojoCodecs
import com.mongodb.MongoClientSettings

import munit.FunSuite
import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter, Document}
import org.bson.codecs.{DecoderContext, EncoderContext}

final class BsonDecoderSpec extends FunSuite {
  private val malformed = AnalyticsError.MalformedMarker

  private def reportRecord(document: Document): MongoAnalyticsReportRecords.Record = {
    val bsonDocument = new BsonDocument()
    val documentCodec = MongoClientSettings.getDefaultCodecRegistry.get(classOf[Document])
    documentCodec.encode(new BsonDocumentWriter(bsonDocument), document, EncoderContext.builder().build())
    val codec = MongoPojoCodecs.registry.get(classOf[MongoPojoCodecs.ReportRecord])
    codec.decode(
      new BsonDocumentReader(bsonDocument),
      DecoderContext.builder().build()
    )
  }

  test("required BSON fields decode only their declared BSON type") {
    import BsonValueDecoder.given

    assertEquals(BsonDecoder.required[String](new Document("name", "value"), "name", malformed), Right("value"))
    assertEquals(BsonDecoder.required[String](new Document(), "name", malformed), Left(malformed))
    assertEquals(BsonDecoder.required[String](new Document("name", 1), "name", malformed), Left(malformed))
  }

  test("optional BSON fields distinguish absent values from malformed present values") {
    import BsonValueDecoder.given

    assertEquals(BsonDecoder.optional[Long](new Document(), "count", malformed), Right(None))
    assertEquals(BsonDecoder.optional[Long](new Document("count", 12L), "count", malformed), Right(Some(12L)))
    assertEquals(BsonDecoder.optional[Long](new Document("count", 12), "count", malformed), Left(malformed))
  }

  test("report control decoding keeps optional semantics and retains unrecognized BSON fields") {
    val document = new Document("generation", 4L)
      .append("lastPublishedRevision", 8L)
      .append("state", "Hidden")
      .append("operatorExtension", "preserved")

    val decoded = MongoAnalyticsReportRecords.decodeControl(reportRecord(document))
    assertEquals(decoded.map(_.nextRevision), Right(None))
    assertEquals(decoded.map(_.extraFields.getString("operatorExtension")), Right("preserved"))
    assertEquals(
      MongoAnalyticsReportRecords
        .decodeControl(
          reportRecord(new Document(document).append("nextRevision", null))
        )
        .map(_.nextRevision),
      Right(None)
    )
    assert(
      MongoAnalyticsReportRecords
        .decodeControl(reportRecord(new Document(document).append("nextRevision", "bad")))
        .isLeft
    )
  }

  test("report run decoding rejects malformed typed reservation identities") {
    val valid = new Document("_id", "batch-1")
      .append("rangeFingerprint", "a" * 64)
      .append("generation", 1L)
      .append("revision", 2L)
      .append("state", "Reserved")

    assert(MongoAnalyticsReportRecords.decodeRun(reportRecord(valid)).isRight)
    assert(
      MongoAnalyticsReportRecords
        .decodeRun(reportRecord(new Document(valid).append("state", "Unexpected")))
        .isLeft
    )
    assert(MongoAnalyticsReportRecords.decodeRun(reportRecord(new Document(valid).append("_id", " "))).isLeft)
    assert(
      MongoAnalyticsReportRecords
        .decodeRun(reportRecord(new Document(valid).append("rangeFingerprint", "bad")))
        .isLeft
    )
  }

  test("HMAC retirement authorization decodes through its model instance and rejects wrong BSON types") {
    val facts = "operator evidence"
    val authorization = HmacKeyRetirementAuthorization(
      "a" * 64,
      "key-1",
      "v" * 43,
      facts,
      HmacKeyRetirementAuthorization.digest(facts),
      java.time.Instant.parse("2026-09-27T12:00:00Z")
    )
    val document = new Document("_id", s"${authorization.lakehouseId}:${authorization.keyId}")
      .append("lakehouseId", authorization.lakehouseId)
      .append("keyId", authorization.keyId)
      .append("originalVerifier", authorization.originalVerifier)
      .append("evidenceFacts", authorization.evidenceFacts)
      .append("evidenceDigest", authorization.evidenceDigest)
      .append("authorizedAt", java.util.Date.from(authorization.authorizedAt))

    assertEquals(BsonDecoder[HmacKeyRetirementAuthorization].decode(document), Right(authorization))
    assert(
      BsonDecoder[HmacKeyRetirementAuthorization].decode(new Document(document).append("authorizedAt", "bad")).isLeft
    )
  }

  test("HMAC retirement preparation model decoder validates nested barrier BSON") {
    val lakehouseId = "a" * 64
    val keyId = "key-1"
    val volumeName = "unit_hmac-rotation-kafka"
    val lineage = HmacKeyRetirementKafkaLineage(
      "cluster-id",
      "topic-id",
      volumeName,
      s"/var/lib/docker/volumes/$volumeName/_data",
      "2026-09-27T12:00:00Z",
      "127.0.0.1:19093"
    )
    val preparation = HmacKeyRetirementPreparation(
      lakehouseId,
      keyId,
      "v" * 43,
      java.time.Instant.parse("2026-09-27T12:00:00Z"),
      KafkaRetentionBarrier("hiring.phase6.runtime", Vector(KafkaRetentionBarrier.Partition(0, 10L))),
      lineage
    )
    val document = new Document("_id", s"$lakehouseId:$keyId")
      .append("lakehouseId", lakehouseId)
      .append("keyId", keyId)
      .append("originalVerifier", preparation.originalVerifier)
      .append("capturedAt", java.util.Date.from(preparation.capturedAt))
      .append("topic", preparation.barrier.topic)
      .append("partitions", java.util.List.of(new Document("number", 0).append("endOffsetExclusive", 10L)))
      .append("clusterId", lineage.clusterId)
      .append("topicId", lineage.topicId)
      .append("kafkaVolumeName", lineage.volumeName)
      .append("kafkaVolumeMountpoint", lineage.volumeMountpoint)
      .append("kafkaVolumeCreatedAt", lineage.volumeCreatedAt)
      .append("kafkaBootstrapEndpoint", lineage.bootstrapEndpoint)

    assertEquals(BsonDecoder[HmacKeyRetirementPreparation].decode(document), Right(preparation))
    assert(BsonDecoder[HmacKeyRetirementPreparation].decode(new Document(document).append("partitions", "bad")).isLeft)
  }
}
