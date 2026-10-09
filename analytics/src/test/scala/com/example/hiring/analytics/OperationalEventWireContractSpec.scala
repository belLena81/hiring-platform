package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.OperationalEventTransforms
import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{current_timestamp, encode, col}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import scala.jdk.CollectionConverters.*

class OperationalEventWireContractSpec extends FunSuite {
  private lazy val spark = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("OperationalEventWireContractSpec")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  override def afterAll(): Unit = if (!spark.sparkContext.isStopped) spark.stop()

  private def event(kind: String, aggregate: String): String = AnalyticsOperationalEventFixtures.complete(
    s"""{"eventId":"wire-$kind","eventType":"$kind","occurredAt":"2026-10-07T10:00:00Z","aggregateType":"$aggregate","aggregateId":"aggregate-$kind","actorId":"actor","payload":{}}"""
  )
  private def frame(values: List[String]) = {
    val schema = StructType(
      Seq(
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType),
        StructField("value", StringType)
      )
    )
    val raw = spark
      .createDataFrame(
        values.zipWithIndex.map { case (value, index) =>
          Row("hiring.operational-events", 0, index.toLong, value)
        }.asJava,
        schema
      )
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", current_timestamp())
    OperationalEventTransforms.parseKafkaRecords(raw)
  }

  test("all active operational fact shapes pass native Spark validation") {
    val kinds = List(
      "JOB_CREATED" -> "Job",
      "JOB_UPDATED" -> "Job",
      "JOB_CLOSED" -> "Job",
      "APPLICATION_CREATED" -> "Application",
      "APPLICATION_STATUS_CHANGED" -> "Application",
      "CANDIDATE_HIRED" -> "Application",
      "SEARCH_PERFORMED" -> "Search",
      "JOB_VIEWED" -> "Search",
      "SEARCH_RESULT_CLICKED" -> "Search"
    )
    assertEquals(OperationalEventTransforms.validEvents(frame(kinds.map(event.tupled))).count(), 9L)
  }

  test("unknown fields, missing per-event fields, invalid IDs and aggregate mismatches are malformed") {
    val valid = parse(event("APPLICATION_STATUS_CHANGED", "Application")).toOption.get
    val payload = valid.hcursor.downField("payload").focus.get
    val invalid = List(
      valid.mapObject(_.add("schemaVersion", Json.fromInt(1))),
      valid.mapObject(_.add("actorId", Json.fromString("actor"))),
      valid.mapObject(_.add("aggregateId", Json.fromString(AnalyticsOperationalEventFixtures.id("other")))),
      valid.mapObject(_.add("payload", payload.mapObject(_.remove("candidateId")))),
      valid.mapObject(_.add("payload", payload.mapObject(_.add("feedback", Json.fromString("secret"))))),
      valid.mapObject(_.add("payload", payload.mapObject(_.add("newStatus", Json.fromString("Unknown")))))
    ).map(_.noSpaces)
    val parsed = frame(invalid)
    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(parsed).count(), invalid.size.toLong)
  }

  test("only timezone-bearing ISO instant timestamps are admitted and malformed dates quarantine under ANSI") {
    val base = parse(event("APPLICATION_CREATED", "Application")).toOption.get
    def at(value: String) = base.mapObject(_.add("occurredAt", Json.fromString(value))).noSpaces
    val bad = List("2026-10-07", "2026-10-07T10:00:00", "2026-02-30T10:00:00Z", "broken")
    assertEquals(OperationalEventTransforms.validEvents(frame(bad.map(at))).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(frame(bad.map(at))).count(), 4L)
    assertEquals(
      OperationalEventTransforms
        .validEvents(frame(List(at("2026-10-07T12:00:00+02:00"), at("2026-10-07T10:00:00.123456789Z"))))
        .count(),
      2L
    )
  }

  test("uppercase participant UUIDs are canonical before privacy marker matching") {
    val base = parse(event("APPLICATION_STATUS_CHANGED", "Application")).toOption.get
    val candidate = base.hcursor.downField("payload").get[String]("candidateId").toOption.get
    val actor = base.hcursor.get[String]("actorId").toOption.get
    val upper = base
      .mapObject(fields =>
        fields
          .add("actorId", Json.fromString(actor.toUpperCase))
          .add("payload", fields("payload").get.mapObject(_.add("candidateId", Json.fromString(candidate.toUpperCase))))
      )
      .noSpaces
    val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(9))
    val tokenized = com.example.hiring.analytics.adapter.spark.AnalyticsSubjectPrivacy
      .withSubjectToken(OperationalEventTransforms.validEvents(frame(List(upper))), keys)
    val marker = AnalyticsTestSubjectPseudonymizer.tokenValue(keys, candidate)
    val markers =
      spark.createDataFrame(List(Row(marker)).asJava, StructType(Seq(StructField("subjectToken", StringType))))
    assertEquals(
      com.example.hiring.analytics.adapter.spark.AnalyticsSubjectPrivacy
        .excludeActiveDeletionMarkers(tokenized, markers)
        .toOption
        .get
        .count(),
      0L
    )
  }

  test("optional fields reject coercion to null and skill bounds use Scala UTF-16 semantics") {
    val viewed = parse(event("JOB_VIEWED", "Search")).toOption.get
    val changed = parse(event("APPLICATION_STATUS_CHANGED", "Application")).toOption.get
    val job = parse(event("JOB_CREATED", "Job")).toOption.get
    def changePayload(base: Json, update: Json => Json): String =
      base.mapObject(fields => fields.add("payload", update(fields("payload").get))).noSpaces
    val bad = List(
      changePayload(viewed, _.mapObject(_.add("rank", Json.fromString("invalid")))),
      changePayload(viewed, _.mapObject(_.add("searchKind", Json.obj()))),
      changePayload(changed, _.mapObject(_.add("previousStatus", Json.obj()))),
      changePayload(
        job,
        _.mapObject(fields =>
          fields.add("job", fields("job").get.mapObject(_.add("skills", Json.arr(Json.fromString("\t\u0001")))))
        )
      ),
      changePayload(
        job,
        _.mapObject(fields =>
          fields.add("job", fields("job").get.mapObject(_.add("skills", Json.arr(Json.fromString("😀" * 129)))))
        )
      ),
      changePayload(
        job,
        _.mapObject(fields =>
          fields.add("job", fields("job").get.mapObject(_.add("skills", Json.arr(Json.fromInt(1)))))
        )
      ),
      changed.mapObject(_.add("eventId", Json.fromString("1-1-1-1-1"))).noSpaces
    )
    assertEquals(OperationalEventTransforms.validEvents(frame(bad)).count(), 0L)
    val emoji = changePayload(
      job,
      _.mapObject(fields =>
        fields.add("job", fields("job").get.mapObject(_.add("skills", Json.arr(Json.fromString("😀" * 128)))))
      )
    )
    assertEquals(OperationalEventTransforms.validEvents(frame(List(emoji))).count(), 1L)
    val mixedCase = changed
      .mapObject(fields =>
        fields.add(
          "payload",
          fields("payload").get
            .mapObject(payload => payload.add("applicationId", payload("applicationId").get.mapString(_.toUpperCase)))
        )
      )
      .noSpaces
    assertEquals(OperationalEventTransforms.validEvents(frame(List(mixedCase))).count(), 1L)
  }

  test("result arrays enforce IDs, ranks, finite scores and exact nested fields") {
    val valid = parse(event("SEARCH_PERFORMED", "Search")).toOption.get
    val base = Json.obj(
      "resultId" -> Json.fromString(AnalyticsOperationalEventFixtures.id("job")),
      "rank" -> Json.fromInt(1),
      "score" -> Json.fromDoubleOrNull(0.5)
    )
    def withResults(results: List[Json]): String = valid
      .mapObject(fields =>
        fields.add("payload", fields("payload").get.mapObject(_.add("results", Json.fromValues(results))))
      )
      .noSpaces
    assertEquals(OperationalEventTransforms.validEvents(frame(List(withResults(List(base))))).count(), 1L)
    val bad = List(
      withResults(List(base.mapObject(_.add("rank", Json.fromInt(0))))),
      withResults(List(base.mapObject(_.add("score", Json.Null)))),
      withResults(List(base.mapObject(_.add("score", Json.fromString("0.5"))))),
      withResults(List(base.mapObject(_.add("rank", Json.fromString("1"))))),
      withResults(List(base.mapObject(_.add("rank", Json.fromDoubleOrNull(1.0))))),
      withResults(List(base.mapObject(_.add("private", Json.fromString("secret"))))),
      withResults(List(base, base.mapObject(_.add("rank", Json.fromInt(2)))))
    )
    val numericId = parse(event("APPLICATION_CREATED", "Application")).toOption.get
      .mapObject(fields =>
        fields.add("payload", fields("payload").get.mapObject(_.add("candidateId", Json.fromInt(1))))
      )
      .noSpaces
    val wrongSearch = valid
      .mapObject(fields =>
        fields.add(
          "payload",
          fields("payload").get
            .mapObject(_.add("searchId", Json.fromString(AnalyticsOperationalEventFixtures.id("wrong"))))
        )
      )
      .noSpaces
    assertEquals(OperationalEventTransforms.validEvents(frame(bad ++ List(numericId, wrongSearch))).count(), 0L)
  }
}
