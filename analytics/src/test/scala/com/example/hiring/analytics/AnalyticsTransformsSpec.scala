package com.example.hiring.analytics

import munit.FunSuite
import org.apache.spark.sql.SparkSession

class AnalyticsTransformsSpec extends FunSuite {
  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("AnalyticsTransformsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  private def records(values: Seq[(String, Int, Long, String)]) = {
    import spark.implicits._
    values.toDF("topic", "partition", "offset", "value")
      .withColumn("value", org.apache.spark.sql.functions.encode(org.apache.spark.sql.functions.col("value"), "UTF-8"))
      .withColumn("timestamp", org.apache.spark.sql.functions.current_timestamp())
  }

  private def event(
      id: String,
      eventType: String,
      aggregateType: String = "Application",
      aggregateId: String = "application-1",
      payload: Option[String] = None
  ): String = {
    val defaultPayload = s"""{"applicationId":"$aggregateId","candidateId":"candidate-1","jobId":"job-1","newStatus":"Hired"}"""
    s"""{"eventId":"$id","eventType":"$eventType","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"$aggregateType","aggregateId":"$aggregateId","actorId":"actor-1","payload":${payload.getOrElse(defaultPayload)}}"""
  }

  test("bronze deduplicates Kafka delivery by topic partition and offset") {
    val source = records(Seq(
      ("hiring.operational-events", 0, 1L, event("event-1", "APPLICATION_CREATED")),
      ("hiring.operational-events", 0, 1L, event("event-2", "APPLICATION_CREATED"))
    ))
    assertEquals(OperationalEventTransforms.bronze(source).count(), 1L)
  }

  test("silver keeps identical event retries and quarantines conflicting event ids") {
    val same = event("event-1", "APPLICATION_CREATED")
    val conflict = event("event-1", "APPLICATION_STATUS_CHANGED")
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(Seq(
      ("hiring.operational-events", 0, 1L, same),
      ("hiring.operational-events", 0, 2L, same),
      ("hiring.operational-events", 0, 3L, conflict)
    )))
    val valid = OperationalEventTransforms.validEvents(parsed)
    assertEquals(OperationalEventTransforms.conflictingEventIds(valid).count(), 1L)
    assertEquals(OperationalEventTransforms.silver(valid).count(), 0L)
  }

  test("malformed envelopes are excluded from valid records") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(Seq(
      ("hiring.operational-events", 0, 1L, """{"eventId":"event-1"}""")
    )))
    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(parsed).count(), 1L)
  }

  test("funnel excludes the duplicate candidate hired event and suppresses groups below ten") {
    val events = (1 to 10).map(index =>
      ("hiring.operational-events", 0, index.toLong, event(s"created-$index", "APPLICATION_CREATED", aggregateId = s"application-$index"))
    ) ++ Seq(("hiring.operational-events", 0, 11L, event("hired-1", "CANDIDATE_HIRED")))
    val silver = OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(events))))
    val result = HiringGoldTransforms.funnelActivity(silver).select("eventType", "contributingApplications").collect().toSeq
    assertEquals(result.map(_.getString(0)).toSet, Set("APPLICATION_CREATED"))
    assertEquals(result.head.getLong(1), 10L)
  }

  test("skill posting activity normalizes only created job skills and applies k anonymity") {
    val payload = """{"job":{"skills":[" Scala ","scala","  "]}}"""
    val created = (1 to 10).map(index =>
      ("hiring.operational-events", 0, index.toLong, event(s"job-$index", "JOB_CREATED", "Job", s"job-$index", Some(payload)))
    )
    val update = ("hiring.operational-events", 0, 20L, event("update-1", "JOB_UPDATED", "Job", "job-update", Some(payload)))
    val silver = OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(created :+ update))))
    val result = HiringGoldTransforms.skillPostingActivity(silver).collect()
    assertEquals(result.length, 1)
    assertEquals(result.head.getString(result.head.fieldIndex("skill")), "scala")
    assertEquals(result.head.getLong(result.head.fieldIndex("contributingPostingEvents")), 10L)
  }

  test("offset manifests reject impossible or duplicated partition ranges") {
    assertEquals(AnalyticsRetention.BronzeDays, 7)
    assertEquals(AnalyticsRetention.SilverDays, 30)
    intercept[IllegalArgumentException](PartitionOffsetRange("topic", 0, 5L, 4L))
    intercept[IllegalArgumentException](AnalyticsRunManifest("run-1", Vector(
      PartitionOffsetRange("topic", 0, 0L, 1L),
      PartitionOffsetRange("topic", 0, 1L, 2L)
    )))
  }
}
