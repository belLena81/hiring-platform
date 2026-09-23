package com.example.hiring.analytics

import cats.effect.{Clock, IO}
import cats.effect.unsafe.implicits.global
import munit.FunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class AnalyticsTransformsSpec extends FunSuite {
  private val pseudonymizer = SubjectPseudonymizer.fromSecret("analytics-test-secret".getBytes("UTF-8"))
  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
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
    val schema = StructType(
      Seq(
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("offset", LongType, nullable = false),
        StructField("value", StringType, nullable = true)
      )
    )
    spark
      .createDataFrame(
        values.map { case (topic, partition, offset, value) =>
          Row(topic, partition, offset, value)
        }.asJava,
        schema
      )
      .withColumn("value", org.apache.spark.sql.functions.encode(org.apache.spark.sql.functions.col("value"), "UTF-8"))
      .withColumn("timestamp", org.apache.spark.sql.functions.current_timestamp())
  }

  private def validatedManifest(runId: String, ranges: Vector[PartitionOffsetRange]): AnalyticsRunManifest =
    AnalyticsRunManifest.validated(runId, ranges).toEither.fold(errors => fail(errors.toString), identity)

  private def fixedClock(instant: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
    override def realTime: IO[FiniteDuration] = IO.pure(instant.toEpochMilli.millis)
    override def monotonic: IO[FiniteDuration] = IO.pure(0.nanos)
  }

  private def markerFrame(tokens: Seq[String]) =
    spark.createDataFrame(
      tokens.map(Row(_)).asJava,
      StructType(
        Seq(
          StructField("subjectToken", StringType, nullable = false)
        )
      )
    )

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

  test("blank event and aggregate identifiers are malformed") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          ("hiring.operational-events", 0, 1L, event("", "APPLICATION_CREATED")),
          ("hiring.operational-events", 0, 2L, event("   ", "APPLICATION_CREATED")),
          ("hiring.operational-events", 0, 3L, event("event-3", "APPLICATION_CREATED", aggregateId = "")),
          ("hiring.operational-events", 0, 4L, event("event-4", "APPLICATION_CREATED", aggregateId = "   ")),
          (
            "hiring.operational-events",
            0,
            5L,
            """{"eventId":"\t","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"Application","aggregateId":"application-5","actorId":"actor-1","payload":{}}"""
          ),
          (
            "hiring.operational-events",
            0,
            6L,
            """{"eventId":"event-6","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"Application","aggregateId":"\n","actorId":"actor-1","payload":{}}"""
          )
        )
      )
    )

    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(parsed).count(), 6L)
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

    assertEquals(HiringGoldTransforms.timeToHire(silver).unsafeRunSync().count(), 0L)
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

    assertEquals(HiringGoldTransforms.timeToHire(silver).unsafeRunSync().count(), 0L)
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
    assert(PartitionOffsetRange.validate(PartitionOffsetRange("topic", 0, 5L, 4L)).isInvalid)
    assert(
      AnalyticsRunManifest
        .validated(
          "run-1",
          Vector(PartitionOffsetRange("topic", 0, 0L, 1L), PartitionOffsetRange("topic", 0, 1L, 2L))
        )
        .isInvalid
    )
    val errors = AnalyticsRunManifest
      .validated(
        "",
        Vector(PartitionOffsetRange("", -1, -1L, -2L), PartitionOffsetRange("", -1, 0L, 1L))
      )
      .toEither
      .swap
      .toOption
      .get
      .toNonEmptyList
      .toList
    assert(errors.contains("run id must be non-empty"))
    assert(errors.contains("topic must be non-empty"))
    assert(errors.contains("each topic partition may occur only once"))
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
    val manifest = validatedManifest("run-1", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 13L)))
    val publication = new HiringAnalyticsBatch(
      AnalyticsLakehousePaths(lakehouse),
      pseudonymizer,
      DataFrameDeletionMarkerSource(emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
      .run(spark, DataFrameBatchSource(input), manifest)
      .unsafeRunSync()

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

  test("tombstone quarantine rows deduplicate when a Kafka range is replayed") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-tombstone-replay").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouse)
    val input = records(Seq(("hiring.operational-events", 3, 17L, null.asInstanceOf[String])))
    val batch = new HiringAnalyticsBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource(emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
    val firstManifest = validatedManifest(
      "tombstone-first-run",
      Vector(PartitionOffsetRange("hiring.operational-events", 3, 17L, 18L))
    )
    val replayManifest = validatedManifest(
      "tombstone-replay-run",
      Vector(PartitionOffsetRange("hiring.operational-events", 3, 17L, 18L))
    )

    val first = batch.run(spark, DataFrameBatchSource(input), firstManifest).unsafeRunSync()
    val replay = batch.run(spark, DataFrameBatchSource(input), replayManifest).unsafeRunSync()
    val quarantine = spark.read.format("delta").load(paths.quarantine)

    assertEquals(first.quarantinedRecords, 1L)
    assertEquals(replay.quarantinedRecords, 1L)
    assertEquals(quarantine.count(), 1L)
    assertEquals(quarantine.filter(col("quarantineId").isNull).count(), 0L)
    assertEquals(quarantine.filter(col("quarantineId").like("tombstone:%")).count(), 1L)
  }

  test("unavailable deletion markers fail before creating a manifest or Bronze data") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-marker-failure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouse)
    val source = records(Seq(("hiring.operational-events", 0, 1L, event("created", "APPLICATION_CREATED"))))
    val markers = new ActiveDeletionMarkerSource {
      override def activeSubjectTokens(spark: SparkSession): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.MissingMarkerCollection)
    }
    val batch = new HiringAnalyticsBatch(paths, pseudonymizer, markers)

    val failure = intercept[AnalyticsError.MissingMarkerCollection.type] {
      batch
        .run(
          spark,
          DataFrameBatchSource(source),
          validatedManifest("run-fail", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 2L)))
        )
        .unsafeRunSync()
    }
    assertEquals(failure, AnalyticsError.MissingMarkerCollection)
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
      validatedManifest("run-published", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 11L)))
    val publication = new HiringAnalyticsBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource(emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    ).run(spark, DataFrameBatchSource(input), manifest).unsafeRunSync()

    assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
    assertEquals(publication.completedAt, Instant.parse("2026-09-22T12:00:00Z"))
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 1L)
    assertEquals(
      spark.read.format("delta").load(paths.manifests).select("updatedAt").collect().map(_.getString(0)).toSet,
      Set("2026-09-22T12:00:00Z")
    )
  }

  test("active deletion markers purge stored Silver and rebuild Gold before a quality-blocked range") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-silver-erasure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouse)
    val initialEvents = (1 to 10).map(index =>
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
    val seedParsed = OperationalEventTransforms.parseKafkaRecords(records(initialEvents))
    val seedAt = Instant.parse("2026-09-22T12:00:00Z")
    val seedSilver = OperationalEventTransforms
      .silver(OperationalEventTransforms.validEvents(seedParsed), pseudonymizer, emptyMarkers)
      .withColumn("ingestedAt", lit(Timestamp.from(seedAt)))
      .withColumn("expiresAt", lit(Timestamp.from(seedAt.plusSeconds(30L * 24L * 60L * 60L))))
    seedSilver.write.format("delta").save(paths.silver)
    HiringGoldTransforms.wideFunnelDay(seedSilver).write.format("delta").save(paths.funnelGold)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 10L)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).count(), 1L)

    val replayAndMalformed = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          11L,
          event(
            "created-1",
            "APPLICATION_CREATED",
            aggregateId = "application-1",
            payload = Some("""{"applicationId":"application-1","candidateId":"candidate-1"}""")
          )
        ),
        ("hiring.operational-events", 0, 12L, "not-json")
      )
    )
    val markers = markerFrame(Seq(pseudonymizer.token("candidate-1")))
    val deletionBatch = new HiringAnalyticsBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource(markers),
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    )
    val deletionRun = validatedManifest(
      "erasure-with-bad-range",
      Vector(PartitionOffsetRange("hiring.operational-events", 0, 11L, 13L))
    )

    assertEquals(
      deletionBatch.run(spark, DataFrameBatchSource(replayAndMalformed), deletionRun).unsafeRunSync().outcome,
      AnalyticsRunOutcome.QualityBlocked
    )
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 9L)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).count(), 0L)
  }

  test("data frame source honors the manifest offset boundary") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("in", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 2L, event("out", "APPLICATION_CREATED"))
      )
    )
    val manifest = validatedManifest("run-1", Vector(PartitionOffsetRange("hiring.operational-events", 0, 1L, 2L)))
    assertEquals(DataFrameBatchSource(source).read(spark, manifest).unsafeRunSync().count(), 1L)
  }

  test("data frame source reports missing Kafka columns as a typed schema error") {
    val source = records(Seq.empty).drop("partition", "offset")
    val manifest =
      validatedManifest("run-missing-columns", Vector(PartitionOffsetRange("hiring.operational-events", 0, 0L, 1L)))
    val failure = intercept[AnalyticsError.InvalidSourceSchema] {
      DataFrameBatchSource(source).read(spark, manifest).unsafeRunSync()
    }
    assertEquals(failure.missing, Vector("offset", "partition"))
  }

  test("subject tokens are deterministic opaque HMAC values") {
    val token = pseudonymizer.token("candidate-1")
    assertEquals(token, pseudonymizer.token("candidate-1"))
    assertNotEquals(token, pseudonymizer.token("candidate-2"))
    assert(!token.contains("candidate-1"))
    assert(token.startsWith("hmac-v1_"))
  }

  test("Mongo erasure request subject IDs map to HMAC tokens and malformed IDs fail closed") {
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e")
    assertEquals(
      MongoActiveDeletionMarkerSource.tokenFor(new org.bson.Document("_id", subjectId.toString), pseudonymizer),
      Right(SubjectToken.fromHmac(pseudonymizer.token(subjectId.toString)))
    )
    assertEquals(
      MongoActiveDeletionMarkerSource.tokenFor(new org.bson.Document("_id", "not-a-uuid"), pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(
      MongoActiveDeletionMarkerSource.tokenFor(new org.bson.Document("_id", 17), pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
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
    val markers = markerFrame(Seq(pseudonymizer.token("candidate-1")))
    val silver =
      OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(parsed), pseudonymizer, markers)

    assertEquals(silver.select("eventId").collect().map(_.getString(0)).toSet, Set("kept"))
    assertEquals(
      silver.select("subjectToken").collect().map(_.getString(0)).toSet,
      Set(pseudonymizer.token("candidate-2"))
    )
    assert(!silver.columns.contains("actorId"))
    assert(!silver.columns.contains("candidateId"))
  }

  test("cached deletion markers are released when the source fails") {
    val markers = markerFrame(Seq.empty)
    val paths = AnalyticsLakehousePaths(Files.createTempDirectory("analytics-marker-cleanup").toUri.toString)
    val batch = new HiringAnalyticsBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource(markers))
    val source = new BoundedOperationalEventSource {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.SourceReadFailure(new IllegalStateException("injected read failure")))
    }
    val manifest = validatedManifest("cleanup", Vector(PartitionOffsetRange("topic", 0, 0L, 1L)))

    intercept[AnalyticsError.SourceReadFailure](batch.run(spark, source, manifest).unsafeRunSync())
    assertEquals(markers.storageLevel, StorageLevel.NONE)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }
}
