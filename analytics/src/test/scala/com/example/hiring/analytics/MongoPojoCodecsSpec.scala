package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.{MongoAnalyticsReportRecords, MongoPojoCodecs}
import org.bson.{BsonDocument, BsonDocumentReader, BsonDocumentWriter}
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext

import munit.FunSuite

final class MongoPojoCodecsSpec extends FunSuite {
  test("report POJOs decode exact BSON types and retain unknown fields") {
    val source = BsonDocument.parse(
      """{"_id":"control","generation":{"$numberLong":"4"},"lastPublishedRevision":{"$numberLong":"8"},"state":"Hidden","operatorExtension":"kept"}"""
    )
    val codec = MongoPojoCodecs.registry.get(classOf[MongoPojoCodecs.ReportRecord])
    val decoded = codec.decode(new BsonDocumentReader(source), DecoderContext.builder().build())
    val written = new BsonDocument()
    codec.encode(new BsonDocumentWriter(written), decoded, EncoderContext.builder().build())

    assertEquals(decoded.getGeneration.asInt64().getValue, 4L)
    assert(Option(decoded.getNextRevision).isEmpty)
    assertEquals(decoded.getExtraFields.getString("operatorExtension"), "kept")
    assertEquals(written.getString("operatorExtension").getValue, "kept")
    assertEquals(written.getInt64("lastPublishedRevision").getValue, 8L)
    assert(!written.containsKey("nextRevision"))
  }

  test("report POJO codec rejects BSON integer-width mismatches") {
    val source = BsonDocument.parse(
      """{"_id":"control","generation":{"$numberInt":"4"},"lastPublishedRevision":{"$numberLong":"8"},"state":"Hidden"}"""
    )
    val codec = MongoPojoCodecs.registry.get(classOf[MongoPojoCodecs.ReportRecord])
    val decoded = codec.decode(new BsonDocumentReader(source), DecoderContext.builder().build())
    assert(decoded.getGeneration.isInt32)
    assert(MongoAnalyticsReportRecords.decodeControl(decoded).isLeft)
  }
}
