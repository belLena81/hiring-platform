package com.example.hiring.analytics

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.nio.file.Files
import java.time.Instant

class AnalyticsTransformsSpec extends FunSuite {
  private val pseudonymizer = SubjectPseudonymizer.fromSecret("analytics-test-secret".getBytes("UTF-8"))
  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("AnalyticsTransformsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  private def records(values: Seq[(String, Int, Long, String)]) = {
    import spark.implicits._
    values
      .toDF("topic", "partition", "offset", "value")
      .withColumn("value", org.apache.spark.sql.functions.encode(org.apache.spark.sql.functions.col("value"), "UTF-8"))
      .withColumn("timestamp", org.apache.spark.sql.functions.current_timestamp())
  }

  private def event(
      id: String,
      eventType: String,
      aggregateType: String = "Application",
      aggregateId: String = "application-1",
      payload: Option[String] = None,
      actorId: String = "actor-1"
  ): String = {
    val defaultPayload =
      s"""{"applicationId":"$aggregateId","candidateId":"candidate-1","jobId":"job-1","newStatus":"Hired"}"""
    s"""{"eventId":"$id","eventType":"$eventType","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"$aggregateType","aggregateId":"$aggregateId","actorId":"$actorId","payload":${payload
        .getOrElse(defaultPayload)}}"""
  }

  private def emptyMarkers = AnalyticsSubjectPrivacy.emptyMarkers(records(Seq.empty))

  test("bronze deduplicates Kafka delivery by topic partition and offset") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("event-1", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 1L, event("event-2", "APPLICATION_CREATED"))
      )
    )
    assertEquals(OperationalEventTransforms.bronze(source).count(), 1L)
  }

  test("silver keeps identical event retries and quarantines conflicting event ids") {
    val same = event("event-1", "APPLICATION_CREATED")
    val conflict = event("event-1", "APPLICATION_STATUS_CHANGED")
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          ("hiring.operational-events", 0, 1L, same),
          ("hiring.operational-events", 0, 2L, same),
          ("hiring.operational-events", 0, 3L, conflict)
        )
      )
    )
    val valid = OperationalEventTransforms.validEvents(parsed)
    assertEquals(OperationalEventTransforms.conflictingEventIds(valid).count(), 1L)
    assertEquals(OperationalEventTransforms.silver(valid, pseudonymizer, emptyMarkers).count(), 0L)
  }

  test("malformed envelopes are excluded from valid records") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          ("hiring.operational-events", 0, 1L, """{"eventId":"event-1"}""")
        )
      )
    )
    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(parsed).count(), 1L)
  }

  test("funnel excludes the duplicate candidate hired event and suppresses groups below ten") {
    val events = (1 to 10).map(index =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(
          s"created-$index",
          "APPLICATION_CREATED",
          aggregateId = s"application-$index",
          payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
        )
      )
    ) ++ Seq(("hiring.operational-events", 0, 11L, event("hired-1", "CANDIDATE_HIRED")))
    val silver = OperationalEventTransforms.silver(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(events))),
      pseudonymizer,
      emptyMarkers
    )
    val result =
      HiringGoldTransforms.funnelActivity(silver).select("eventType", "contributingApplications").collect().toSeq
    assertEquals(result.map(_.getString(0)).toSet, Set("APPLICATION_CREATED"))
    assertEquals(result.head.getLong(1), 10L)
  }

  test("wide funnel suppresses a day when any nonzero status cell is below ten subjects") {
    val created = (1 to 10).map(index =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(
          s"created-$index",
          "APPLICATION_CREATED",
          aggregateId = s"application-$index",
          payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
        )
      )
    )
    val oneAccepted = (
      "hiring.operational-events",
      0,
      11L,
      event(
        "accepted-1",
        "APPLICATION_STATUS_CHANGED",
        aggregateId = "application-1",
        payload = Some("""{"applicationId":"application-1","candidateId":"candidate-1","newStatus":"Accepted"}""")
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(created :+ oneAccepted))
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.wideFunnelDay(silver).count(), 0L)
  }

  test("time-to-hire suppresses eligible and excluded counts when either is below ten") {
    val lifecycle = (1 to 10).flatMap { index =>
      val applicationId = s"application-$index"
      val candidateId = s"candidate-$index"
      Seq(
        (
          "hiring.operational-events",
          0,
          index.toLong * 2,
          event(
            s"created-$index",
            "APPLICATION_CREATED",
            aggregateId = applicationId,
            payload = Some(s"""{"applicationId":"$applicationId","candidateId":"$candidateId"}""")
          )
        ),
        (
          "hiring.operational-events",
          0,
          index.toLong * 2 + 1,
          event(
            s"hired-$index",
            "APPLICATION_STATUS_CHANGED",
            aggregateId = applicationId,
            payload = Some(s"""{"applicationId":"$applicationId","candidateId":"$candidateId","newStatus":"Hired"}""")
          )
        )
      )
    } :+ (
      "hiring.operational-events",
      0,
      21L,
      event(
        "excluded",
        "APPLICATION_CREATED",
        aggregateId = "application-excluded",
        payload = Some("""{"applicationId":"application-excluded","candidateId":"candidate-excluded"}""")
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(lifecycle))
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.timeToHire(silver).count(), 0L)
  }

  test("ten eligible applications from one subject do not satisfy time-to-hire suppression") {
    val lifecycle = (1 to 10).flatMap { index =>
      val applicationId = s"application-$index"
      Seq(
        (
          "hiring.operational-events",
          0,
          index.toLong * 2,
          event(
            s"created-$index",
            "APPLICATION_CREATED",
            aggregateId = applicationId,
            payload = Some(s"""{"applicationId":"$applicationId","candidateId":"same-candidate"}""")
          )
        ),
        (
          "hiring.operational-events",
          0,
          index.toLong * 2 + 1,
          event(
            s"hired-$index",
            "APPLICATION_STATUS_CHANGED",
            aggregateId = applicationId,
            payload = Some(s"""{"applicationId":"$applicationId","candidateId":"same-candidate","newStatus":"Hired"}""")
          )
        )
      )
    }
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(lifecycle))
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.timeToHire(silver).count(), 0L)
  }

  test("skill posting activity normalizes only created job skills and applies k anonymity") {
    val payload = """{"job":{"skills":[" Scala ","scala","  "]}}"""
    val created = (1 to 10).map(index =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(s"job-$index", "JOB_CREATED", "Job", s"job-$index", Some(payload), actorId = s"recruiter-$index")
      )
    )
    val update =
      ("hiring.operational-events", 0, 20L, event("update-1", "JOB_UPDATED", "Job", "job-update", Some(payload)))
    val silver = OperationalEventTransforms.silver(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(created :+ update))),
      pseudonymizer,
      emptyMarkers
    )
    val result = HiringGoldTransforms.skillPostingActivity(silver).collect()
    assertEquals(result.length, 1)
    assertEquals(result.head.getString(result.head.fieldIndex("skill")), "scala")
    assertEquals(result.head.getLong(result.head.fieldIndex("postings")), 10L)
  }

  test("ten job postings by one recruiter do not satisfy skill activity suppression") {
    val payload = """{"job":{"skills":["scala"]}}"""
    val created = (1 to 10).map(index =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(s"job-$index", "JOB_CREATED", "Job", s"job-$index", Some(payload))
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(created))
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.skillPostingActivity(silver).count(), 0L)
  }

  test("offset manifests reject impossible or duplicated partition ranges") {
    assertEquals(AnalyticsRetention.BronzeDays, 7)
    assertEquals(AnalyticsRetention.SilverDays, 30)
    intercept[IllegalArgumentException](PartitionOffsetRange("topic", 0, 5L, 4L))
    intercept[IllegalArgumentException](
      AnalyticsRunManifest(
        "run-1",
        Vector(
          PartitionOffsetRange("topic", 0, 0L, 1L),
          PartitionOffsetRange("topic", 0, 1L, 2L)
        )
      )
    )
  }

  test("Kafka offset bounds are valid JSON without escaped structural quotes") {
    val ranges = Vector(PartitionOffsetRange("hiring.operational-events", 0, 3L, 8L))
    assertEquals(KafkaOffsetRangeSource.offsetJson(ranges, _.startOffset), "{\"hiring.operational-events\":{\"0\":3}}")
    assertEquals(
      KafkaOffsetRangeSource.offsetJson(ranges, _.endOffsetExclusive),
      "{\"hiring.operational-events\":{\"0\":8}}"
    )
  }

  test("bounded batch persists replayable layers and quality-blocks Gold on malformed or conflicting records") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-batch").toUri.toString.stripSuffix("/")
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"created-$index",
            "APPLICATION_CREATED",
            aggregateId = s"application-$index",
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      ) ++ Seq(
        ("hiring.operational-events", 0, 11L, event("bad", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 12L, event("bad", "APPLICATION_STATUS_CHANGED")),
        ("hiring.operational-events", 0, 13L, event("outside", "APPLICATION_CREATED"))
      )
    )
    val manifest = AnalyticsRunManifest("run-1", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 13L)))
    val publication = new HiringAnalyticsBatch(
      AnalyticsLakehousePaths(lakehouse),
      pseudonymizer,
      DataFrameDeletionMarkerSource(emptyMarkers),
      () => Instant.parse("2026-09-22T12:00:00Z")
    )
      .run(spark, DataFrameBatchSource(input), manifest)

    assertEquals(publication.bronzeRecords, 12L)
    assertEquals(publication.conflictingEventIds, 1L)
    assertEquals(publication.quarantinedRecords, 2L)
    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, publication.funnelGoldPath))
    assertEquals(spark.read.format("delta").load(AnalyticsLakehousePaths(lakehouse).silver).count(), 10L)
    assertEquals(
      spark.read
        .format("delta")
        .load(AnalyticsLakehousePaths(lakehouse).manifests)
        .filter(col("status") === "QUALITY_BLOCKED")
        .count(),
      1L
    )
  }

  test("unavailable deletion markers fail before creating a manifest or Bronze data") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-marker-failure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouse)
    val source = records(Seq(("hiring.operational-events", 0, 1L, event("created", "APPLICATION_CREATED"))))
    val markers = new ActiveDeletionMarkerSource {
      override def activeSubjectTokens(spark: SparkSession) =
        throw new IllegalStateException("marker source unavailable")
    }
    val batch = new HiringAnalyticsBatch(paths, pseudonymizer, markers)

    intercept[IllegalStateException] {
      batch.run(
        spark,
        DataFrameBatchSource(source),
        AnalyticsRunManifest("run-fail", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 2L)))
      )
    }
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze))
  }

  test("clean bounded batch returns Published and records PUBLISHED in the manifest") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-published").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"created-$index",
            "APPLICATION_CREATED",
            aggregateId = s"application-$index",
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val manifest =
      AnalyticsRunManifest("run-published", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 11L)))
    val publication = new HiringAnalyticsBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource(emptyMarkers),
      () => Instant.parse("2026-09-22T12:00:00Z")
    ).run(spark, DataFrameBatchSource(input), manifest)

    assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 1L)
  }

  test("data frame source honors the manifest offset boundary") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("in", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 2L, event("out", "APPLICATION_CREATED"))
      )
    )
    val manifest = AnalyticsRunManifest("run-1", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 2L)))
    assertEquals(DataFrameBatchSource(source).read(spark, manifest).count(), 1L)
  }

  test("subject tokens are deterministic opaque HMAC values") {
    val token = pseudonymizer.token("candidate-1")
    assertEquals(token, pseudonymizer.token("candidate-1"))
    assertNotEquals(token, pseudonymizer.token("candidate-2"))
    assert(!token.contains("candidate-1"))
    assert(token.startsWith("hmac-v1_"))
  }

  test("active deletion tokens are excluded before Silver persistence without retaining raw identities") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          (
            "hiring.operational-events",
            0,
            1L,
            event("deleted", "APPLICATION_CREATED", aggregateId = "application-deleted")
          ),
          (
            "hiring.operational-events",
            0,
            2L,
            event(
              "kept",
              "APPLICATION_CREATED",
              aggregateId = "application-kept",
              payload =
                Some("{\"applicationId\":\"application-kept\",\"candidateId\":\"candidate-2\",\"jobId\":\"job-1\"}")
            )
          )
        )
      )
    )
    import spark.implicits._
    val markers = Seq(pseudonymizer.token("candidate-1")).toDF("subjectToken")
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, markers)

    assertEquals(silver.select("eventId").as[String].collect().toSet, Set("kept"))
    assertEquals(silver.select("subjectToken").as[String].collect().toSet, Set(pseudonymizer.token("candidate-2")))
    assert(!silver.columns.contains("actorId"))
    assert(!silver.columns.contains("candidateId"))
  }
}
