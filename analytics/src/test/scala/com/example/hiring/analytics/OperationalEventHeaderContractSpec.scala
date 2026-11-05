package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.OperationalEventTransforms
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, current_timestamp}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, LongType, StringType, StructField, StructType}

import scala.jdk.CollectionConverters.*

/** Kafka record headers are optional metadata: a present header must agree with the body, absent headers are fine. */
class OperationalEventHeaderContractSpec extends FunSuite {
  private lazy val spark = AnalyticsTestSpark
    .builder("OperationalEventHeaderContractSpec")
    .getOrCreate()
  // The first test pays for cold Spark session start-up.
  override val munitTimeout: scala.concurrent.duration.Duration = scala.concurrent.duration.Duration(3, "min")
  override def afterAll(): Unit = if (!spark.sparkContext.isStopped) spark.stop()

  private val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))
  private val body = AnalyticsOperationalEventFixtures.complete(
    """{"eventId":"hdr-1","eventType":"JOB_CREATED","occurredAt":"2026-10-07T10:00:00Z","aggregateType":"Job","aggregateId":"hdr-job","actorId":"actor","payload":{}}"""
  )
  private val bodyEventId = AnalyticsOperationalEventFixtures.id("hdr-1")

  private type Headers = Seq[(String, String)]

  private def withHeaders(records: Seq[(String, Option[Headers])]): DataFrame = {
    val headerType = ArrayType(StructType(Seq(StructField("key", StringType), StructField("value", BinaryType))))
    val schema = StructType(
      Seq(
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType),
        StructField("value", BinaryType),
        StructField("headers", headerType)
      )
    )
    val rows = records.zipWithIndex.map { case ((value, headers), index) =>
      Row(
        "hiring.operational-events",
        0,
        index.toLong,
        value.getBytes("UTF-8"),
        headers.map(_.map { case (k, v) => Row(k, v.getBytes("UTF-8")) }).orNull
      )
    }
    OperationalEventTransforms.parseKafkaRecords(
      spark.createDataFrame(rows.asJava, schema).withColumn("timestamp", current_timestamp())
    )
  }

  private def rawHeaderValidity(headers: Seq[(String, Array[Byte])]): Boolean =
    rawHeaderFrame(headers).head().getAs[Boolean]("isValidEvent")

  private def rawHeaderReason(headers: Seq[(String, Array[Byte])]): String = {
    val analyzed = OperationalEventTransforms.analyzed(rawHeaderParsed(headers), keys)
    analyzed.select(OperationalEventTransforms.quarantineReason(analyzed)).head().getString(0)
  }

  private def rawHeaderFrame(headers: Seq[(String, Array[Byte])]): DataFrame =
    OperationalEventTransforms.withValidity(rawHeaderParsed(headers))

  private def rawHeaderParsed(headers: Seq[(String, Array[Byte])]): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType),
        StructField("value", BinaryType),
        StructField(
          "headers",
          ArrayType(StructType(Seq(StructField("key", StringType), StructField("value", BinaryType))))
        )
      )
    )
    val row = Row("hiring.operational-events", 0, 0L, body.getBytes("UTF-8"), headers.map((k, v) => Row(k, v)))
    OperationalEventTransforms.parseKafkaRecords(
      spark.createDataFrame(Seq(row).asJava, schema).withColumn("timestamp", current_timestamp())
    )
  }

  private def validity(headers: Option[Headers]): Boolean =
    OperationalEventTransforms
      .withValidity(withHeaders(Seq(body -> headers)))
      .head()
      .getAs[Boolean]("isValidEvent")

  test("headers absent keep the record valid") {
    assert(validity(None))
    assert(validity(Some(Seq.empty)))
    assert(validity(Some(Seq("traceparent" -> "00-abc-def-01"))))
    val withoutColumn = spark
      .createDataFrame(
        Seq(Row("hiring.operational-events", 0, 0L, body.getBytes("UTF-8"))).asJava,
        StructType(
          Seq(
            StructField("topic", StringType),
            StructField("partition", IntegerType),
            StructField("offset", LongType),
            StructField("value", BinaryType)
          )
        )
      )
      .withColumn("timestamp", current_timestamp())
    val parsed = OperationalEventTransforms.parseKafkaRecords(withoutColumn)
    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 1L)
  }

  test("agreeing headers keep the record valid") {
    assert(
      validity(
        Some(
          Seq(
            "content-type" -> "application/json",
            "hiring-schema-version" -> "1",
            "hiring-event-type" -> "JOB_CREATED",
            "hiring-event-id" -> bodyEventId.toUpperCase
          )
        )
      )
    )
  }

  test("KFH-03.3 header event type disagreeing with the body is invalid") {
    assert(!validity(Some(Seq("hiring-event-type" -> "APPLICATION_CREATED"))))
    assert(!validity(Some(Seq("hiring-event-id" -> AnalyticsOperationalEventFixtures.id("other")))))
    val analyzed = OperationalEventTransforms.analyzed(
      withHeaders(Seq(body -> Some(Seq("hiring-event-type" -> "APPLICATION_CREATED")))),
      keys
    )
    val reasons = OperationalEventTransforms
      .malformedEvents(analyzed)
      .select(OperationalEventTransforms.quarantineReason(analyzed).as("reason"))
      .collect()
      .map(_.getString(0))
    assertEquals(reasons.toSeq, Seq(OperationalEventTransforms.InvalidEnvelopeReason))
  }

  test("KFH-03.7 a duplicate header key is invalid") {
    assert(!validity(Some(Seq("traceparent" -> "a", "traceparent" -> "a"))))
    assert(!validity(Some(Seq("hiring-schema-version" -> "1", "hiring-schema-version" -> "1"))))
    val analyzed = OperationalEventTransforms.analyzed(
      withHeaders(Seq(body -> Some(Seq("hiring-schema-version" -> "2", "hiring-schema-version" -> "2")))),
      keys
    )
    val reason = analyzed.select(OperationalEventTransforms.quarantineReason(analyzed)).head().getString(0)
    assertEquals(reason, OperationalEventTransforms.InvalidEnvelopeReason)
  }

  test("KFH-03.7 a header value over 256 bytes is invalid") {
    assert(validity(Some(Seq("traceparent" -> "a" * 256))))
    assert(!validity(Some(Seq("traceparent" -> "a" * 257))))
  }

  test("KFH-03.7 a non-UTF-8 contract header value is invalid") {
    val bad = Array(0xc3.toByte, 0x28.toByte)
    assert(!rawHeaderValidity(Seq("hiring-event-type" -> bad)))
    assert(!rawHeaderValidity(Seq("content-type" -> bad)))
    assert(!rawHeaderValidity(Seq("hiring-schema-version" -> bad)))
    assert(!rawHeaderValidity(Seq("hiring-event-id" -> bad)))
  }

  test("KFH-03.7 a null-valued contract header is invalid as malformed") {
    Seq("content-type", "hiring-schema-version", "hiring-event-type", "hiring-event-id").foreach { name =>
      assert(!rawHeaderValidity(Seq(name -> null)), name)
      assert(rawHeaderReason(Seq(name -> null)) == OperationalEventTransforms.InvalidEnvelopeReason, name)
    }
  }

  test("KFH-03.7 a null-valued unknown header is ignored") {
    assert(rawHeaderValidity(Seq("traceparent" -> null)))
  }

  test("KFH-03.3 a content type other than JSON is invalid") {
    assert(!validity(Some(Seq("content-type" -> "text/plain"))))
    assert(validity(Some(Seq("content-type" -> "application/json"))))
  }

  test("KFH-03.3 unknown headers are ignored even if not UTF-8") {
    assert(rawHeaderValidity(Seq("x-binary" -> Array(0xc3.toByte, 0x28.toByte))))
  }

  test("KFH-03.4 schema version 2 is quarantined as UnsupportedVersion") {
    val analyzed = OperationalEventTransforms.analyzed(
      withHeaders(
        Seq(
          body -> Some(Seq("hiring-schema-version" -> "2")),
          body -> Some(Seq("hiring-schema-version" -> "1")),
          body -> None
        )
      ),
      keys
    )
    val rows = analyzed
      .select(col("offset"), col("isValidEvent"), OperationalEventTransforms.quarantineReason(analyzed).as("reason"))
      .collect()
      .sortBy(_.getLong(0))
    assertEquals(rows.map(_.getBoolean(1)).toSeq, Seq(false, true, true))
    assertEquals(rows.head.getString(2), OperationalEventTransforms.UnsupportedVersionReason)
    assertEquals(OperationalEventTransforms.UnsupportedVersionReason, "UNSUPPORTED_SCHEMA_VERSION")
  }

  test("the event fingerprint is the SHA-256 of the value only and headers do not widen Bronze") {
    val analyzed = OperationalEventTransforms.analyzed(
      withHeaders(
        Seq(
          body -> None,
          body -> Some(Seq("hiring-schema-version" -> "1", "traceparent" -> "x")),
          body -> Some(Seq("hiring-schema-version" -> "2"))
        )
      ),
      keys
    )
    val distinct = analyzed.select("eventFingerprint").distinct().collect().map(_.getString(0))
    assertEquals(distinct.length, 1)
    assertEquals(
      distinct.head,
      java.security.MessageDigest.getInstance("SHA-256").digest(body.getBytes("UTF-8")).map("%02x".format(_)).mkString
    )
    val bronze = OperationalEventTransforms.bronze(OperationalEventTransforms.validEvents(analyzed))
    assert(!bronze.columns.contains("headers"))
  }

  // The application build owns these fixtures; the analytics build reads them in place from the repository root
  // (no copy) so both readers are held to the same accepted/rejected examples.
  private val toleranceDirectory: java.nio.file.Path =
    Seq("../src/test/resources/operational-event-tolerance", "src/test/resources/operational-event-tolerance")
      .map(java.nio.file.Paths.get(_))
      .find(java.nio.file.Files.isDirectory(_))
      .getOrElse(fail("operational-event-tolerance fixtures not found"))

  test("KFH-03.5 Spark validity agrees with the shared Circe tolerance fixtures") {
    val files =
      java.nio.file.Files.list(toleranceDirectory).iterator().asScala.filter(_.toString.endsWith(".json")).toList
    assert(files.nonEmpty)
    files.foreach { file =>
      val json = io.circe.parser.parse(java.nio.file.Files.readString(file)).toOption.get
      val expected = json.hcursor.get[String]("expected").toOption.get
      // envelopeRaw is fed verbatim (a parsed object cannot hold duplicate keys); otherwise the envelope is serialised.
      val envelope = json.hcursor
        .get[String]("envelopeRaw")
        .toOption
        .getOrElse(json.hcursor.downField("envelope").focus.get.noSpaces)
      val valid = OperationalEventTransforms
        .withValidity(withHeaders(Seq(envelope -> None)))
        .head()
        .getAs[Boolean]("isValidEvent")
      assertEquals(valid, expected == "accepted", file.getFileName.toString)
    }
  }
}
