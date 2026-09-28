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
import AnalyticsBatchTestSupport.{newBatch, newKeyContinuityStage}

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.{Clock, Deferred, IO}
import cats.effect.unsafe.implicits.global
import munit.FunSuite
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class AnalyticsTransformsSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes

  private def hmacKey(seed: String): Array[Byte] = seed.padTo(32, 'x').getBytes("UTF-8")
  private val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(hmacKey("analytics-test-secret"))
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

  private def silverFrame(
      valid: DataFrame,
      pseudonymizer: SubjectPseudonymizer,
      markers: DataFrame
  ): DataFrame =
    OperationalEventTransforms.silver(valid, pseudonymizer, markers).fold(error => fail(error.toString), identity)

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
    assertEquals(silverFrame(valid, pseudonymizer, emptyMarkers).count(), 0L)
  }

  test("active marker exclusion reports each missing token column as a typed error") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(Seq(("hiring.operational-events", 0, 1L, event("event-1", "APPLICATION_CREATED"))))
    )
    val events = AnalyticsSubjectPrivacy.withSubjectToken(
      OperationalEventTransforms.validEvents(parsed),
      pseudonymizer
    )
    assertEquals(
      AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(events.drop("subjectToken"), emptyMarkers).left.toOption,
      Some(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
    )
    assertEquals(
      AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(events.drop("subjectTokens"), emptyMarkers).left.toOption,
      Some(AnalyticsError.InvalidSourceSchema(Vector("subjectTokens")))
    )
    assertEquals(
      AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(events, emptyMarkers.drop("subjectToken")).left.toOption,
      Some(AnalyticsError.InvalidSourceSchema(Vector("subjectToken")))
    )
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
    val silver = silverFrame(
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
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

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
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.timeToHireAction[IO](silver).unsafeRunSync().count(), 0L)
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
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.timeToHireAction[IO](silver).unsafeRunSync().count(), 0L)
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
    val silver = silverFrame(
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
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.skillPostingActivity(silver).count(), 0L)
  }

  test("offset manifests reject impossible or duplicated partition ranges") {
    assertEquals(AnalyticsRetention.BronzeDays, 7)
    assertEquals(AnalyticsRetention.SilverDays, 30)
    assert(PartitionOffsetRange.from("topic", 0, 5L, 4L).isInvalid)
    assert(
      AnalyticsRunManifest
        .validated(
          "run-1",
          Vector(PartitionOffsetRange.unsafe("topic", 0, 0L, 1L), PartitionOffsetRange.unsafe("topic", 0, 1L, 2L))
        )
        .isInvalid
    )
    val errors = AnalyticsRunManifest
      .validated(
        "",
        Vector(PartitionOffsetRange.unsafe("topic", 0, 0L, 1L), PartitionOffsetRange.unsafe("topic", 0, 1L, 2L))
      )
      .toEither
      .swap
      .toOption
      .get
      .toNonEmptyList
      .toList
    assert(errors.contains("run id must be non-empty"))
    val topicErrors = PartitionOffsetRange.from("", 0, 0L, 1L).toEither.swap.toOption.get
    assert(topicErrors.toNonEmptyList.toList.contains("topic must be non-empty"))
    val numericErrors = PartitionOffsetRange.from("topic", -1, -1L, -2L).toEither.swap.toOption.get
    assert(numericErrors.toNonEmptyList.toList.contains("partition must be non-negative"))
    assert(numericErrors.toNonEmptyList.toList.contains("start offset must be non-negative"))
    assert(numericErrors.toNonEmptyList.toList.contains("end offset must be non-negative"))
    assert(errors.contains("each topic partition may occur only once"))
  }

  test("Kafka offset JSON escapes topic strings and preserves the configured shape") {
    val ranges = Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 3L, 8L))
    assertEquals(KafkaOffsetRangeSource.assignJson(ranges), "{\"hiring.operational-events\":[0]}")
    assertEquals(KafkaOffsetRangeSource.offsetJson(ranges, _.startOffset), "{\"hiring.operational-events\":{\"0\":3}}")
    assertEquals(
      KafkaOffsetRangeSource.offsetJson(ranges, _.endOffsetExclusive),
      "{\"hiring.operational-events\":{\"0\":8}}"
    )
    val escaped = Vector(PartitionOffsetRange.unsafe("topic\"\\\n\u0001", 2, 3L, 8L))
    assertEquals(KafkaOffsetRangeSource.assignJson(escaped), "{\"topic\\\"\\\\\\n\\u0001\":[2]}")
    assertEquals(
      KafkaOffsetRangeSource.offsetJson(escaped, _.startOffset),
      "{\"topic\\\"\\\\\\n\\u0001\":{\"2\":3}}"
    )
  }

  test("shared analytics SHA-256 encoding matches the lowercase UTF-8 vector") {
    assertEquals(
      AnalyticsDigest.sha256Hex("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )
  }

  test("Kafka range reader defaults authenticated metadata and Spark settings to SASL_SSL") {
    val authenticated = KafkaConnection("kafka:9092", Some("analytics_reader"), Some("local-secret"))
    val client = KafkaClientProperties.clientProperties(authenticated)
    val spark = KafkaClientProperties.sparkOptions(authenticated)
    assertEquals(client.get("security.protocol"), Some("SASL_SSL"))
    assertEquals(client.get("sasl.mechanism"), Some("PLAIN"))
    assert(client.getOrElse("sasl.jaas.config", "").contains("username=\"analytics_reader\""))
    assertEquals(spark.get("kafka.security.protocol"), Some("SASL_SSL"))
    assertEquals(spark.get("kafka.group.id"), Some("hiring-analytics-batch"))
    assertEquals(spark.get("kafka.isolation.level"), Some("read_committed"))
    assertEquals(KafkaConnection.validate(KafkaConnection("kafka:9092", Some("reader"), None)).isInvalid, true)
  }

  test("Kafka SASL_PLAINTEXT requires explicit opt-in and reaches both client property sets") {
    val unapproved = KafkaConnection("kafka:9092", Some("reader"), Some("secret"), "SASL_PLAINTEXT")
    assert(KafkaConnection.validate(unapproved).isInvalid)
    intercept[IllegalArgumentException](KafkaClientProperties.clientProperties(unapproved))
    intercept[IllegalArgumentException](KafkaClientProperties.sparkOptions(unapproved))

    val approved = unapproved.copy(allowPlaintext = true)
    assert(KafkaConnection.validate(approved).isValid)
    assertEquals(KafkaClientProperties.clientProperties(approved).get("security.protocol"), Some("SASL_PLAINTEXT"))
    assertEquals(KafkaClientProperties.sparkOptions(approved).get("kafka.security.protocol"), Some("SASL_PLAINTEXT"))
  }

  test("Kafka rejects an unsupported security protocol") {
    val invalid = KafkaConnection("kafka:9092", Some("reader"), Some("secret"), "PLAINTEXT", allowPlaintext = true)
    assert(KafkaConnection.validate(invalid).isInvalid)
    intercept[IllegalArgumentException](KafkaClientProperties.clientProperties(invalid))
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
    val manifest =
      validatedManifest("run-1", Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 13L)))
    val publication = newBatch(
      AnalyticsLakehousePaths.unsafe(lakehouse),
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
      .run(spark, DataFrameBatchSource[IO](input), manifest)
      .unsafeRunSync()

    assertEquals(publication.bronzeRecords, 12L)
    assertEquals(publication.conflictingEventIds, 1L)
    assertEquals(publication.quarantinedRecords, 2L)
    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, publication.funnelGoldPath))
    assertEquals(spark.read.format("delta").load(AnalyticsLakehousePaths.unsafe(lakehouse).silver).count(), 10L)
    assertEquals(
      spark.read
        .format("delta")
        .load(AnalyticsLakehousePaths.unsafe(lakehouse).manifests)
        .filter(col("status") === "QUALITY_BLOCKED")
        .count(),
      1L
    )
  }

  test("tombstone quarantine rows deduplicate when a Kafka range is replayed") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-tombstone-replay").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(Seq(("hiring.operational-events", 3, 17L, null.asInstanceOf[String])))
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
    val firstManifest = validatedManifest(
      "tombstone-first-run",
      Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 3, 17L, 18L))
    )
    val replayManifest = validatedManifest(
      "tombstone-replay-run",
      Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 3, 17L, 18L))
    )

    val first = batch.run(spark, DataFrameBatchSource[IO](input), firstManifest).unsafeRunSync()
    val replay = batch.run(spark, DataFrameBatchSource[IO](input), replayManifest).unsafeRunSync()
    val quarantine = spark.read.format("delta").load(paths.quarantine)

    assertEquals(first.quarantinedRecords, 1L)
    assertEquals(replay.quarantinedRecords, 1L)
    assertEquals(quarantine.count(), 1L)
    assertEquals(quarantine.filter(col("quarantineId").isNull).count(), 0L)
    assertEquals(quarantine.filter(col("quarantineId").like("tombstone:%")).count(), 1L)
  }

  test("unavailable deletion markers fail before creating a manifest or Bronze data") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-marker-failure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
    val source = records(Seq(("hiring.operational-events", 0, 1L, event("created", "APPLICATION_CREATED"))))
    val markers = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens(spark: SparkSession): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.MissingMarkerCollection)
    }
    val batch = newBatch(paths, pseudonymizer, markers)

    val failure = intercept[AnalyticsError.MissingMarkerCollection.type] {
      batch
        .run(
          spark,
          DataFrameBatchSource[IO](source),
          validatedManifest("run-fail", Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L)))
        )
        .unsafeRunSync()
    }
    assertEquals(failure, AnalyticsError.MissingMarkerCollection)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze))
  }

  test("clean bounded batch returns Published and records PUBLISHED in the manifest") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-published").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
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
      validatedManifest("run-published", Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L)))
    val publication = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    ).run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()

    assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
    assertEquals(publication.completedAt, Instant.parse("2026-09-22T12:00:00Z"))
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 1L)
    assertEquals(
      spark.read.format("delta").load(paths.manifests).select("updatedAt").collect().map(_.getString(0)).toSet,
      Set("2026-09-22T12:00:00Z")
    )
  }

  test("new raw Delta tables retain replay bytes without copying raw values into transaction log statistics") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-private-delta-stats").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
    val sensitiveMarker = "raw-log-personal-sentinel-8f17c2"
    val input = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          0L,
          event(
            "private-stats-event",
            "APPLICATION_CREATED",
            aggregateId = "private-stats-application",
            payload = Some(s"""{"applicationId":"private-stats-application","candidateId":"$sensitiveMarker"}""")
          )
        )
      )
    )
    val manifest =
      validatedManifest(
        "run-private-delta-stats",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
      )

    newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](emptyMarkers))
      .run(spark, DataFrameBatchSource[IO](input), manifest)
      .unsafeRunSync()

    val rawBronze = spark.read.format("delta").load(paths.bronze).select("rawValue").head().getString(0)
    assert(rawBronze.contains(sensitiveMarker), "Bronze must retain bounded replay bytes")
    val logDirectory = new org.apache.hadoop.fs.Path(s"${paths.bronze}/_delta_log")
    val fileSystem = logDirectory.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val jsonLogs =
      fileSystem.listStatus(logDirectory).filter(status => status.isFile && status.getPath.getName.endsWith(".json"))
    assert(jsonLogs.nonEmpty, "Delta should have transaction log commits")
    val logContents = jsonLogs.map { status =>
      val stream = fileSystem.open(status.getPath)
      try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
      finally stream.close()
    }
    val mapper = new ObjectMapper()
    def addFileActions(contents: Vector[String]): Vector[com.fasterxml.jackson.databind.JsonNode] =
      contents.flatMap { content =>
        content.linesIterator.flatMap(line => Option(mapper.readTree(line).get("add"))).toVector
      }
    def rawValueStats(
        actions: Vector[com.fasterxml.jackson.databind.JsonNode]
    ): Vector[com.fasterxml.jackson.databind.JsonNode] =
      actions.flatMap(action => Option(action.get("stats")).filterNot(_.isNull)).map { statsNode =>
        if (statsNode.isTextual) mapper.readTree(statsNode.asText()) else statsNode
      }
    val addActions = addFileActions(logContents.toVector)
    assert(addActions.nonEmpty, "the log fixture must contain AddFile actions")
    val statistics = rawValueStats(addActions)
    assert(
      statistics.forall(stats => !stats.path("minValues").has("rawValue") && !stats.path("maxValues").has("rawValue")),
      "rawValue statistics must be disabled"
    )
    val properties = io.delta.tables.DeltaTable
      .forPath(spark, paths.bronze)
      .detail()
      .select("properties")
      .head()
      .getAs[scala.collection.Map[String, String]]("properties")
    assertEquals(properties.get("delta.dataSkippingNumIndexedCols"), Some("0"))

    val controlPath = Files.createTempDirectory("hiring-analytics-stats-control").resolve("delta").toString
    spark.read
      .format("delta")
      .load(paths.bronze)
      .write
      .format("delta")
      .option("delta.dataSkippingNumIndexedCols", "32")
      .save(controlPath)
    val controlDirectory = new org.apache.hadoop.fs.Path(s"$controlPath/_delta_log")
    val controlFs = controlDirectory.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val controlLogs =
      controlFs.listStatus(controlDirectory).filter(status => status.isFile && status.getPath.getName.endsWith(".json"))
    val controlContents = controlLogs.map { status =>
      val stream = controlFs.open(status.getPath)
      try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
      finally stream.close()
    }.toVector
    assert(rawValueStats(addFileActions(controlContents)).exists(stats => stats.path("minValues").has("rawValue")))
  }

  test("Mongo publication failure never marks the Delta manifest PUBLISHED") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-publication-failure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"publication-failure-$index",
            "APPLICATION_CREATED",
            aggregateId = s"application-$index",
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val reportPublisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        IO.pure(AnalyticsReportReservation(runId, rangeFingerprint, 0L, 1L))

      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] = IO.raiseError(AnalyticsError.RunIdRangeConflict(reservation.runId.value))
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.unit
    }
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z")),
      reportPublisher = reportPublisher
    )
    val manifest =
      validatedManifest(
        "run-publication-fails",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L))
      )

    val error = intercept[AnalyticsError.RunIdRangeConflict] {
      batch.run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()
    }

    assertEquals(error.runId, "run-publication-fails")
    assertEquals(
      spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(),
      0L
    )
  }

  test("same-range retry completes the manifest after Mongo publication succeeds") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-manifest-retry").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"manifest-retry-$index",
            "APPLICATION_CREATED",
            aggregateId = s"application-$index",
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val manifest =
      validatedManifest(
        "run-manifest-retry",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L))
      )
    val published = new AtomicInteger(0)
    val failPublishedManifestOnce = new AtomicBoolean(true)
    val publisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        IO.pure(AnalyticsReportReservation(runId, rangeFingerprint, 0L, 1L))

      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] = IO.delay { published.incrementAndGet(); () }
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.unit
    }
    val realManifestStore = new DeltaManifestStore[IO](paths)
    val manifestStore = new ManifestStore[IO] {
      override def persist(session: SparkSession, run: AnalyticsRunManifest, status: String, at: String): IO[Unit] =
        if (status == "PUBLISHED" && failPublishedManifestOnce.compareAndSet(true, false))
          IO.raiseError(AnalyticsError.LakehouseFailure(new IllegalStateException("injected manifest failure")))
        else realManifestStore.persist(session, run, status, at)
    }
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z")),
      reportPublisher = publisher,
      manifests = Some(manifestStore)
    )

    intercept[AnalyticsError.LakehouseFailure] {
      batch.run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()
    }
    assertEquals(published.get(), 1)
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 0L)

    val retry = batch.run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()

    assertEquals(retry.outcome, AnalyticsRunOutcome.Published)
    assertEquals(published.get(), 2)
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 1L)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 10L)
  }

  test("batch reads deletion markers before reserving its publication generation") {
    val reserved = new AtomicBoolean(false)
    val markers = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens(session: SparkSession): IO[org.apache.spark.sql.DataFrame] = IO.delay {
        assert(!reserved.get(), "the marker snapshot must be read before publication is reserved")
        emptyMarkers
      }
    }
    val publisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        IO.delay {
          reserved.set(true)
          AnalyticsReportReservation(runId, rangeFingerprint, 0L, 1L)
        }

      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] = IO.raiseError(new AssertionError("the test source fails before publication"))
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.unit
    }
    val source = new BoundedOperationalEventSource[IO] {
      override def read(session: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.SourceReadFailure(new IllegalStateException("injected source failure")))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.unit
    }
    val batch = newBatch(
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-reserve-order").toUri.toString),
      pseudonymizer,
      markers,
      fixedClock(Instant.parse("2026-09-22T12:00:00Z")),
      reportPublisher = publisher
    )
    val manifest =
      validatedManifest(
        "reserve-before-markers",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L))
      )

    val error = intercept[AnalyticsError.SourceReadFailure](batch.run(spark, source, manifest).unsafeRunSync())

    assertEquals(error.getMessage, "analytics source read failed")
    assert(reserved.get())
  }

  test("active deletion markers purge stored Silver and rebuild Gold before a quality-blocked range") {
    val lakehouse = Files.createTempDirectory("hiring-analytics-silver-erasure").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths.unsafe(lakehouse)
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
    val seedSilver = silverFrame(OperationalEventTransforms.validEvents(seedParsed), pseudonymizer, emptyMarkers)
      .withColumn("ingestedAt", lit(Timestamp.from(seedAt)))
      .withColumn("expiresAt", lit(Timestamp.from(seedAt.plusSeconds(30L * 24L * 60L * 60L))))
    newKeyContinuityStage(paths, pseudonymizer)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
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
    val deletionBatch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](markers),
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    )
    intercept[AnalyticsError.LakehouseFailure] {
      deletionBatch.verifyMarkedSubjectsAbsent(spark, markers).unsafeRunSync()
    }
    val deletionRun = validatedManifest(
      "erasure-with-bad-range",
      Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 11L, 13L))
    )

    val publication =
      deletionBatch.run(spark, DataFrameBatchSource[IO](replayAndMalformed), deletionRun).unsafeRunSync()
    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assertEquals(publication.suppressedRecords, 1L)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 9L)
    assertEquals(spark.read.format("delta").load(paths.quarantine).count(), 0L)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).count(), 0L)
    deletionBatch.verifyMarkedSubjectsAbsent(spark, markers).unsafeRunSync()
  }

  test("data frame source honors the manifest offset boundary") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("in", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 2L, event("out", "APPLICATION_CREATED"))
      )
    )
    val manifest =
      validatedManifest("run-1", Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L)))
    assertEquals(DataFrameBatchSource[IO](source).read(spark, manifest).unsafeRunSync().count(), 1L)
  }

  test("data frame source reports missing Kafka columns as a typed schema error") {
    val source = records(Seq.empty).drop("partition", "offset")
    val manifest =
      validatedManifest(
        "run-missing-columns",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
      )
    val failure = intercept[AnalyticsError.InvalidSourceSchema] {
      DataFrameBatchSource[IO](source).read(spark, manifest).unsafeRunSync()
    }
    assertEquals(failure.missing, Vector("offset", "partition"))
  }

  test("empty requested ranges fail before reading or writing a manifest") {
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-empty-offset-range").toUri.toString)
    val source = DataFrameBatchSource[IO](records(Seq.empty))
    val manifest = validatedManifest("empty-offset-range", Vector(PartitionOffsetRange.unsafe("topic", 0, 4L, 4L)))
    val failure = intercept[AnalyticsError.EmptyRequestedRange] {
      newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](emptyMarkers))
        .run(spark, source, manifest)
        .unsafeRunSync()
    }
    assertEquals(failure, AnalyticsError.EmptyRequestedRange("topic", 0, 4L))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("missing partition and interior offset gaps fail before a manifest is written") {
    val input = records(
      Seq(
        ("topic", 0, 1L, event("first", "APPLICATION_CREATED")),
        ("topic", 0, 3L, event("third", "APPLICATION_CREATED"))
      )
    )
    val scenarios = Vector(
      ("missing-partition", PartitionOffsetRange.unsafe("topic", 1, 5L, 6L), 1L, 0L),
      ("missing-interior", PartitionOffsetRange.unsafe("topic", 0, 1L, 4L), 3L, 2L)
    )
    scenarios.foreach { case (runId, range, requested, observed) =>
      val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory(s"analytics-$runId").toUri.toString)
      val failure = intercept[AnalyticsError.MissingOffsetRange] {
        newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](emptyMarkers))
          .run(spark, DataFrameBatchSource[IO](input), validatedManifest(runId, Vector(range)))
          .unsafeRunSync()
      }
      assertEquals(failure, AnalyticsError.MissingOffsetRange(range.topic, range.partition, requested, observed))
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze))
    }
  }

  test("broker retention bounds distinguish expired from not yet available offsets") {
    val range = PartitionOffsetRange.unsafe("topic", 0, 3L, 8L)
    assertEquals(
      AnalyticsOffsetRanges.available(range, earliestAvailable = 4L, latestExclusive = 10L),
      Left(AnalyticsError.ExpiredOffsetRange("topic", 0, 3L, 4L))
    )
    assertEquals(
      AnalyticsOffsetRanges.available(range, earliestAvailable = 0L, latestExclusive = 7L),
      Left(AnalyticsError.MissingOffsetRange("topic", 0, 5L, 4L))
    )
    assertEquals(AnalyticsOffsetRanges.available(range, earliestAvailable = 0L, latestExclusive = 8L), Right(()))
  }

  test("subject tokens are deterministic opaque HMAC values") {
    val token = pseudonymizer.token("candidate-1")
    assertEquals(token, pseudonymizer.token("candidate-1"))
    assertNotEquals(token, pseudonymizer.token("candidate-2"))
    assert(!token.contains("candidate-1"))
    assert(token.startsWith("hmac-v1_"))
  }

  test("pseudonymizer Mac cache is per-thread and reinitialized after closure serialization") {
    val subjectIds = Vector.tabulate(128)(index => s"candidate-$index")
    val expected = subjectIds.map(pseudonymizer.token)
    val bytes = new java.io.ByteArrayOutputStream()
    val output = new java.io.ObjectOutputStream(bytes)
    output.writeObject(pseudonymizer)
    output.close()
    val input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val serializedCopy = input.readObject().asInstanceOf[SubjectPseudonymizer]
    input.close()
    assertEquals(subjectIds.map(serializedCopy.token), expected)

    val executor = java.util.concurrent.Executors.newFixedThreadPool(8)
    try {
      val actual = subjectIds.zip(expected).map { case (subjectId, token) =>
        executor.submit(new java.util.concurrent.Callable[String] {
          override def call(): String = pseudonymizer.token(subjectId)
        }) -> token
      }
      assertEquals(actual.map(_._1.get()), expected)
    } finally executor.shutdownNow()
  }

  test("temporary Delta rewrite path is removed after failure and cancellation") {
    val root = Files.createTempDirectory("delta-purge-resource")
    val failedPath = new org.apache.hadoop.fs.Path(root.resolve("failed").toUri)
    val failedFileSystem = failedPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val primaryFailure = new IllegalStateException("rewrite failed")
    val failure = DeltaPurgeRewrite
      .temporaryPath[IO](spark, failedPath.toString)
      .use(_ => IO.blocking(failedFileSystem.mkdirs(failedPath)) *> IO.raiseError[Unit](primaryFailure))
      .attempt
      .unsafeRunSync()
    assert(failure.swap.toOption.contains(primaryFailure))
    assert(!failedFileSystem.exists(failedPath))

    val canceledPath = new org.apache.hadoop.fs.Path(root.resolve("canceled").toUri)
    val canceledFileSystem = canceledPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val pathRemoved = (for {
      acquired <- Deferred[IO, Unit]
      fiber <- DeltaPurgeRewrite
        .temporaryPath[IO](spark, canceledPath.toString)
        .use(_ => IO.blocking(canceledFileSystem.mkdirs(canceledPath)) *> acquired.complete(()).void *> IO.never[Unit])
        .start
      _ <- acquired.get
      _ <- fiber.cancel
      remains <- IO.blocking(canceledFileSystem.exists(canceledPath))
    } yield remains).unsafeRunSync()
    assert(!pathRemoved)
  }

  test("HMAC secrets shorter than 256 bits are rejected") {
    val errors = SubjectPseudonymizer
      .validatedKeyRing("hmac-v1", "short-secret".getBytes("UTF-8"), Vector.empty)
      .toEither
      .swap
      .toOption
      .getOrElse(fail("expected short secret rejection"))
      .toChain
      .toList
      .mkString(" ")
    assert(errors.contains("32 bytes"))
    val encoded = java.util.Base64.getEncoder.encodeToString("short-secret".getBytes("UTF-8"))
    assert(SubjectPseudonymizer.validateFromBase64(Some(encoded), "hmac-v1", None, None).isInvalid)
  }

  test("lakehouse operation boundary preserves typed errors and adapts thrown failures") {
    class Boundary extends LakehouseOperation[IO] {
      override protected val async: cats.effect.Async[IO] = cats.effect.Async[IO]
      def run[A](work: => A): IO[A] = lakehouse(work)
      def runIO[A](work: IO[A]): IO[A] = lakehouseIO(work)
      def runEither[A](work: => Either[AnalyticsError, A]): IO[A] = lakehouseEither(work)
    }
    val boundary = new Boundary
    val expected = AnalyticsError.InvalidConfiguration("expected lakehouse validation failure")
    val cause = new IllegalStateException("spark failure")

    assertEquals(boundary.runEither[String](Left(expected)).attempt.unsafeRunSync(), Left(expected))
    val failure = boundary.run[Unit](throw cause).attempt.unsafeRunSync().swap.toOption
    val ioFailure = boundary.runIO(IO.raiseError[Unit](cause)).attempt.unsafeRunSync().swap.toOption
    val adapted = failure.exists {
      case AnalyticsError.LakehouseFailure(underlying) => underlying eq cause
      case _                                           => false
    }
    assert(adapted, "expected the Spark failure to become LakehouseFailure")
    assertEquals(ioFailure, failure)
  }

  test("versioned HMAC key rings preserve retiring tokens for deletion markers") {
    val oldSecret = hmacKey("old-analytics-key")
    val newSecret = hmacKey("new-analytics-key")
    val oldKey = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", oldSecret, Vector.empty)
    val rotating = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v2", newSecret, Vector("hmac-v1" -> oldSecret))
    assertNotEquals(rotating.token("candidate-1"), oldKey.token("candidate-1"))
    assertEquals(
      rotating.matchingTokens("candidate-1"),
      Right(Vector(rotating.token("candidate-1"), oldKey.token("candidate-1")))
    )
    assertEquals(rotating.matchingTokens(null), Left("subject id must be non-empty"))
    assertEquals(rotating.matchingTokens(""), Left("subject id must be non-empty"))
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e").toString
    assertEquals(
      MongoActiveDeletionMarkerSource
        .tokensFor(new org.bson.Document("_id", subjectId), rotating)
        .map(_.map(_.value).toSet),
      rotating
        .matchingTokens(subjectId)
        .left
        .map(AnalyticsError.InvalidConfiguration.apply)
        .map(_.toSet)
    )
  }

  test("Mongo erasure request subject IDs map to HMAC tokens and malformed IDs fail closed") {
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e")
    assertEquals(
      MongoActiveDeletionMarkerSource
        .tokenFor(new org.bson.Document("_id", subjectId.toString), pseudonymizer)
        .toOption,
      SubjectToken.fromHmac(pseudonymizer.token(subjectId.toString)).toOption
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

  test("completed deletion markers fail closed when their retention expiry is missing or malformed") {
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e").toString
    val now = Instant.parse("2026-09-25T12:00:00Z")
    val missingExpiry = new org.bson.Document("_id", subjectId).append("state", "Complete")
    val malformedExpiry = new org.bson.Document("_id", subjectId)
      .append("state", "Complete")
      .append("expiresAt", "not-a-date")
    val expired = new org.bson.Document("_id", subjectId)
      .append("state", "Complete")
      .append("expiresAt", java.util.Date.from(now.minusSeconds(1)))

    assertEquals(
      MongoActiveDeletionMarkerSource.activeTokens(missingExpiry, now, pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(
      MongoActiveDeletionMarkerSource.activeTokens(malformedExpiry, now, pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(MongoActiveDeletionMarkerSource.activeTokens(expired, now, pseudonymizer), Right(None))
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
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, markers)

    assertEquals(silver.select("eventId").collect().map(_.getString(0)).toSet, Set("kept"))
    assertEquals(
      silver.select("subjectToken").collect().map(_.getString(0)).toSet,
      Set(pseudonymizer.token("candidate-2"))
    )
    assert(!silver.columns.contains("actorId"))
    assert(!silver.columns.contains("candidateId"))
  }

  test("attributable actor and candidate search result identities are both marker-filtered") {
    val source = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          1L,
          event(
            "application-event",
            "APPLICATION_CREATED",
            payload = Some("""{"applicationId":"application-1","candidateId":"candidate-2"}""")
          )
        ),
        (
          "hiring.operational-events",
          0,
          2L,
          event(
            "candidate-search",
            "SEARCH_PERFORMED",
            aggregateType = "Search",
            aggregateId = "search-1",
            payload = Some("""{"searchKind":"candidateMatches","results":[{"resultId":"candidate-3"}]}""")
          )
        )
      )
    )
    val valid = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(source))
    val withTokens = AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer)
    val tokenRows = withTokens
      .select("eventId", "subjectTokens")
      .collect()
      .map { row =>
        row.getString(0) -> row.getSeq[String](1).toList
      }
      .toMap

    assertEquals(
      tokenRows("application-event").toSet,
      Set(pseudonymizer.token("actor-1"), pseudonymizer.token("candidate-2"))
    )
    assertEquals(
      tokenRows("candidate-search").toSet,
      Set(pseudonymizer.token("actor-1"), pseudonymizer.token("candidate-3"))
    )
    assertEquals(
      silverFrame(
        valid,
        pseudonymizer,
        markerFrame(Seq(pseudonymizer.token("actor-1")))
      ).count(),
      0L
    )
    assertEquals(
      silverFrame(
        valid,
        pseudonymizer,
        markerFrame(Seq(pseudonymizer.token("candidate-3")))
      ).select("eventId")
        .collect()
        .map(_.getString(0))
        .toSet,
      Set("application-event")
    )
  }

  test("subject-token UDFs represent missing actor and candidate identities as SQL nulls") {
    val source = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          3L,
          event("unattributed", "APPLICATION_CREATED", payload = Some("{\"candidateId\":\"\"}"), actorId = "")
        )
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(source)
    val attributed = AnalyticsSubjectPrivacy.withSubjectToken(parsed, pseudonymizer).head()

    assert(attributed.isNullAt(attributed.fieldIndex("subjectToken")))
    assertEquals(attributed.getSeq[String](attributed.fieldIndex("subjectTokens")), Seq.empty)
  }

  test("new event attribution emits only the primary token while deletion matching retains previous keys") {
    val oldSecret = hmacKey("old-search-key")
    val current = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
      "hmac-v2",
      hmacKey("new-search-key"),
      Vector("hmac-v1" -> oldSecret)
    )
    val source = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          1L,
          event(
            "rotation-application",
            "APPLICATION_CREATED",
            payload = Some("""{"applicationId":"rotation-app","candidateId":"rotation-candidate"}""")
          )
        )
      )
    )
    val valid = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(source))
    val attributed = AnalyticsSubjectPrivacy.withSubjectToken(valid, current).first()
    val tokens = attributed.getAs[Seq[String]]("subjectTokens").toSet
    assertEquals(attributed.getAs[String]("subjectToken"), current.token("rotation-candidate"))
    assertEquals(tokens, Set(current.token("actor-1"), current.token("rotation-candidate")))
    assertEquals(
      current.matchingTokens("rotation-candidate"),
      Right(
        Vector(
          current.token("rotation-candidate"),
          AnalyticsTestSubjectPseudonymizer.fromSecret(oldSecret).token("rotation-candidate")
        )
      )
    )
  }

  test("batch refuses an HMAC primary key cutover while unexpired Silver rows use the retiring key") {
    val oldSecret = hmacKey("old-cutover-key")
    val old = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", oldSecret, Vector.empty)
    val rotating = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
      "hmac-v2",
      hmacKey("new-cutover-key"),
      Vector("hmac-v1" -> oldSecret)
    )
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-cutover").toUri.toString)
    newKeyContinuityStage(paths, old)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    val schema = StructType(
      Seq(
        StructField("subjectToken", StringType, nullable = false),
        StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("expiresAt", TimestampType, nullable = false)
      )
    )
    spark
      .createDataFrame(
        Seq(
          Row(old.token("candidate-1"), Seq(old.token("candidate-1")), Timestamp.from(Instant.now().plusSeconds(3600L)))
        ).asJava,
        schema
      )
      .write
      .format("delta")
      .save(paths.silver)
    val markerSchema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    val markers = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], markerSchema)
    val batch = newBatch(paths, rotating, DataFrameDeletionMarkerSource[IO](markers))
    val source = new BoundedOperationalEventSource[IO] {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(new AssertionError("Kafka must not be read after an unsafe HMAC cutover"))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.unit
    }
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      batch
        .run(
          spark,
          source,
          validatedManifest(
            "key-cutover",
            Vector(
              PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L)
            )
          )
        )
        .unsafeRunSync()
    }
    assert(error.getMessage.contains("retain the old primary"))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("key removal stays blocked by the continuity registry after current rows disappear") {
    val oldSecret = hmacKey("old-stored-key")
    val old = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", oldSecret, Vector.empty)
    val rotating = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
      "hmac-v2",
      hmacKey("new-stored-key"),
      Vector("hmac-v1" -> oldSecret)
    )
    val removed = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v2", hmacKey("new-stored-key"), Vector.empty)
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-retirement").toUri.toString)
    newKeyContinuityStage(paths, old)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    val markers = DataFrameDeletionMarkerSource[IO](markerFrame(Seq.empty))
    val rotatingStage = newKeyContinuityStage(paths, rotating)
    rotatingStage.validateKeyMaterialContinuity(spark).unsafeRunSync()
    val stored = spark.createDataFrame(
      Seq(Row(old.token("candidate-1"), Seq(rotating.token("candidate-1")))).asJava,
      StructType(
        Seq(
          StructField("subjectToken", StringType, nullable = false),
          StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = false)
        )
      )
    )
    stored.write.format("delta").save(paths.silver)

    rotatingStage.validateStoredTokenKeys(spark).unsafeRunSync()
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, removed).validateStoredTokenKeys(spark).unsafeRunSync()
    }
    assert(error.getMessage.contains("not configured"))
    io.delta.tables.DeltaTable.forPath(spark, paths.silver).delete()
    val retirementBlocked = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, removed).validateKeyMaterialContinuity(spark).unsafeRunSync()
    }
    assert(retirementBlocked.getMessage.contains("durable cleanup and writer-exclusion authorization"))
  }

  test("HMAC key continuity rejects changed material under the same key ID") {
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-continuity").toUri.toString)
    val markers = DataFrameDeletionMarkerSource[IO](markerFrame(Seq.empty))
    val original =
      AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", hmacKey("original-continuity-key"), Vector.empty)
    val changed =
      AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", hmacKey("changed-continuity-key"), Vector.empty)
    newKeyContinuityStage(paths, original).validateKeyMaterialContinuity(spark).unsafeRunSync()
    newKeyContinuityStage(paths, original).validateKeyMaterialContinuity(spark).unsafeRunSync()
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, changed).validateKeyMaterialContinuity(spark).unsafeRunSync()
    }
    assert(error.getMessage.contains("changed without a new key ID"))
  }

  test("legacy lakehouse data without HMAC provenance is not silently anchored") {
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-unanchored-key").toUri.toString)
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(hmacKey("legacy-key-material"))
    spark
      .createDataFrame(
        Seq(Row(pseudonymizer.token("candidate-1"))).asJava,
        StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
      )
      .write
      .format("delta")
      .save(paths.silver)
    val markers = DataFrameDeletionMarkerSource[IO](markerFrame(Seq.empty))
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, pseudonymizer).validateKeyMaterialContinuity(spark).unsafeRunSync()
    }
    assert(error.getMessage.contains("no HMAC key continuity registry"))
    assert(error.getMessage.contains("reset or rebuild this local lakehouse explicitly"))
  }

  test("a configured key ID with stored rows but no registry anchor fails closed") {
    val paths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-partial-key-registry").toUri.toString)
    val current =
      AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v2", hmacKey("current-registry-key"), Vector.empty)
    newKeyContinuityStage(paths, current)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    val old = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", hmacKey("old-unanchored-key"), Vector.empty)
    spark
      .createDataFrame(
        Seq(Row(old.token("candidate-1"))).asJava,
        StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
      )
      .write
      .format("delta")
      .save(paths.silver)
    val rotating = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
      "hmac-v2",
      hmacKey("current-registry-key"),
      Vector("hmac-v1" -> hmacKey("old-unanchored-key"))
    )
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, rotating)
        .validateKeyMaterialContinuity(spark)
        .unsafeRunSync()
    }
    assert(error.getMessage.contains("without a continuity anchor"))
  }

  test("unavailable deletion markers do not create key registry or manifests") {
    val paths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-marker-before-key-registry").toUri.toString)
    val unavailableMarkers = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens(spark: SparkSession): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.MissingMarkerCollection)
    }
    val batch = newBatch(paths, pseudonymizer, unavailableMarkers)
    val source =
      DataFrameBatchSource[IO](
        records(Seq(("hiring.operational-events", 0, 0L, event("event", "APPLICATION_CREATED"))))
      )
    intercept[AnalyticsError.MissingMarkerCollection.type] {
      batch
        .run(
          spark,
          source,
          validatedManifest(
            "marker-before-key-registry",
            Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
          )
        )
        .unsafeRunSync()
    }
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("lazy marker evaluation failure does not create key registry or reserve publication") {
    val paths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-marker-action-before-write").toUri.toString)
    val reserved = new AtomicBoolean(false)
    val failingMarkers = spark
      .range(1L)
      .select(org.apache.spark.sql.functions.expr("raise_error('marker evaluation failed')").as("subjectToken"))
    val publisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        IO.delay {
          reserved.set(true)
          AnalyticsReportReservation(runId, rangeFingerprint, 0L, 1L)
        }

      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] = IO.unit
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.unit
    }
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](failingMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z")),
      reportPublisher = publisher
    )
    val source =
      DataFrameBatchSource[IO](
        records(Seq(("hiring.operational-events", 0, 0L, event("event", "APPLICATION_CREATED"))))
      )
    intercept[AnalyticsError.LakehouseFailure] {
      batch
        .run(
          spark,
          source,
          validatedManifest(
            "marker-action-before-key-registry",
            Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
          )
        )
        .unsafeRunSync()
    }
    assert(!reserved.get())
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("batch filters marked subjects before Bronze and does not persist unattributable malformed quarantine") {
    val paths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-erasure-before-bronze").toUri.toString)
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](markerFrame(Seq(pseudonymizer.token("candidate-1"))))
    )
    val source = DataFrameBatchSource[IO](
      records(
        Seq(
          ("hiring.operational-events", 0, 0L, event("deleted", "APPLICATION_CREATED")),
          (
            "hiring.operational-events",
            0,
            1L,
            event(
              "retained",
              "APPLICATION_CREATED",
              aggregateId = "application-retained",
              payload = Some("""{"applicationId":"application-retained","candidateId":"candidate-2"}""")
            )
          ),
          ("hiring.operational-events", 0, 2L, "{\"eventId\":\"malformed\"}")
        )
      )
    )
    val manifest =
      validatedManifest(
        "erasure-before-bronze",
        Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 3L))
      )

    val publication = batch.run(spark, source, manifest).unsafeRunSync()
    val bronze = spark.read.format("delta").load(paths.bronze)
    val quarantine = spark.read.format("delta").load(paths.quarantine)

    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assertEquals(bronze.count(), 1L)
    assertEquals(bronze.select("eventId").head().getString(0), "retained")
    assert(!quarantine.columns.contains("rawValue"))
    assert(!quarantine.columns.contains("actorId"))
    assert(
      quarantine.columns.toSet.subsetOf(
        Set(
          "topic",
          "partition",
          "offset",
          "payloadHash",
          "subjectTokens",
          "quarantineId",
          "quarantineReason",
          "expiresAt"
        )
      )
    )
    // With an active deletion marker, an unattributable malformed row cannot safely be retained even as metadata.
    // The run still counts it and remains quality-blocked, but no row is written to the quarantine table.
    assertEquals(publication.quarantinedRecords, 1L)
    assertEquals(quarantine.count(), 0L)
  }

  test("cached deletion markers are released when the source fails") {
    val markers = markerFrame(Seq.empty)
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-marker-cleanup").toUri.toString)
    val batch = newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markers))
    val source = new BoundedOperationalEventSource[IO] {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.SourceReadFailure(new IllegalStateException("injected read failure")))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.unit
    }
    val manifest = validatedManifest("cleanup", Vector(PartitionOffsetRange.unsafe("topic", 0, 0L, 1L)))

    intercept[AnalyticsError.SourceReadFailure](batch.run(spark, source, manifest).unsafeRunSync())
    assertEquals(markers.storageLevel, StorageLevel.NONE)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("physical erasure verification is tied to captured Delta file paths") {
    val root = Files.createTempDirectory("analytics-erasure-files")
    val paths = AnalyticsLakehousePaths.unsafe(root.toString)
    val schema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    spark
      .createDataFrame(Vector(Row("subject-deleted"), Row("subject-retained")).asJava, schema)
      .coalesce(1)
      .write
      .format("delta")
      .save(paths.silver)
    val batch =
      newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markerFrame(Seq("subject-deleted"))))

    val affectedFiles = batch.captureMarkedFiles(spark, markerFrame(Seq("subject-deleted"))).unsafeRunSync()
    assertEquals(affectedFiles.size, 1)
    val affectedPath = new org.apache.hadoop.fs.Path(affectedFiles.head)
    val fileSystem = affectedPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assert(fileSystem.exists(affectedPath))
    intercept[AnalyticsError.PhysicalReclamationUnverified.type](
      batch.verifyFilesAbsent(spark, affectedFiles).unsafeRunSync()
    )

    assert(fileSystem.delete(affectedPath, false))
    batch.verifyFilesAbsent(spark, affectedFiles).unsafeRunSync()
  }

  test("erasure marker matching shares array, scalar, and unattributed row semantics") {
    val marker = spark.createDataFrame(
      Vector(Row("deleted"), Row("deleted"), Row(null)).asJava,
      StructType(Seq(StructField("subjectToken", StringType, nullable = true)))
    )
    val arrayRows = spark.createDataFrame(
      Vector(
        Row("array-hit", Seq("deleted", "alias")),
        Row("array-miss", Seq("retained")),
        Row("array-empty", Seq.empty[String])
      ).asJava,
      StructType(
        Seq(
          StructField("rowId", StringType, nullable = false),
          StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = false)
        )
      )
    )
    val scalarRows = spark.createDataFrame(
      Vector(Row("scalar-hit", "deleted"), Row("scalar-miss", "retained")).asJava,
      StructType(
        Seq(
          StructField("rowId", StringType, nullable = false),
          StructField("subjectToken", StringType, nullable = false)
        )
      )
    )
    val unattributedRows = spark.createDataFrame(
      Vector(Row("raw-1"), Row("raw-2")).asJava,
      StructType(Seq(StructField("rowId", StringType, nullable = false)))
    )
    val markerRows = BatchSubjectMatching.markerRows(marker)

    assertEquals(BatchSubjectMatching.matchedBySubject(arrayRows, markerRows).count(), 1L)
    assertEquals(BatchSubjectMatching.matchedBySubject(scalarRows, markerRows).count(), 1L)
    assertEquals(BatchSubjectMatching.matchedBySubject(unattributedRows, markerRows).count(), 2L)
  }

  test("no-op erasure retries advance the raw-log checkpoint boundary") {
    val root = Files.createTempDirectory("analytics-no-op-log-checkpoint")
    val paths = AnalyticsLakehousePaths.unsafe(root.toString)
    val rawSchema = StructType(
      Seq(
        StructField("rawValue", StringType, nullable = true),
        StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = true),
        StructField("expiresAt", TimestampType, nullable = true)
      )
    )
    spark
      .createDataFrame(
        Vector(
          Row("retained-event", Seq("subject-retained"), Timestamp.from(Instant.parse("2026-09-24T12:00:00Z")))
        ).asJava,
        rawSchema
      )
      .write
      .format("delta")
      .save(paths.bronze)
    val markerRows = markerFrame(Seq("subject-not-present"))
    val initialBatch = newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markerRows))
    val firstVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assertEquals(initialBatch.countMarkedRows(spark, markerRows).unsafeRunSync(), 0L)

    val initialEvidence =
      initialBatch.captureMarkedFiles(spark, markerRows).unsafeRunSync().filter(_.contains("/_delta_log/"))
    assert(initialEvidence.nonEmpty)
    initialBatch.applyDeletionMarkers(spark, markerRows).unsafeRunSync()
    val firstRetired = initialBatch.checkpointPurgedRawLogs(spark).unsafeRunSync()
    val firstCheckpointVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assert(
      firstCheckpointVersion > firstVersion,
      "checkpoint boundary must advance even when the deletion matched no rows"
    )
    assert(
      initialEvidence.forall(firstRetired.contains),
      "all pre-purge raw log paths must be retired by a newer boundary"
    )

    // Model worker reconstruction and exact retry after a crash following checkpointing.
    val restartedBatch = newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markerRows))
    val retryEvidence =
      restartedBatch.captureMarkedFiles(spark, markerRows).unsafeRunSync().filter(_.contains("/_delta_log/"))
    val retryJson = retryEvidence.filter(_.endsWith(".json"))
    assert(retryJson.nonEmpty, "retry capture should include the current baseline commit")
    restartedBatch.applyDeletionMarkers(spark, markerRows).unsafeRunSync()
    val retryRetired = restartedBatch.checkpointPurgedRawLogs(spark).unsafeRunSync()
    val retryCheckpointVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assert(retryCheckpointVersion > firstCheckpointVersion, "a restarted retry must establish a later boundary")
    assert(retryJson.forall(retryRetired.contains), "retry baseline JSON must be older than the new clean checkpoint")
  }

  test("legacy raw log paths are checkpointed and removed once they age beyond log retention") {
    val root = Files.createTempDirectory("analytics-legacy-log-cleanup")
    val paths = AnalyticsLakehousePaths.unsafe(root.toString)
    val rawSchema = StructType(
      Seq(
        StructField("rawValue", StringType, nullable = true),
        StructField("expiresAt", TimestampType, nullable = true)
      )
    )
    val rawPath = paths.bronze
    val privateValue = "legacy-raw-stats-sentinel"
    spark
      .createDataFrame(
        Vector(Row(privateValue, Timestamp.from(Instant.parse("2026-09-24T12:00:00Z")))).asJava,
        rawSchema
      )
      .write
      .format("delta")
      .option("delta.dataSkippingNumIndexedCols", "32")
      .save(rawPath)
    val markerRows = markerFrame(Seq("subject-deleted"))
    val batch = newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markerRows))

    val beforePurge = batch.captureMarkedFiles(spark, markerRows).unsafeRunSync()
    val legacyLogs = beforePurge.filter(_.contains("/_delta_log/"))
    assert(legacyLogs.nonEmpty)
    val jsonLogs = legacyLogs.filter(_.endsWith(".json"))
    val fileSystem = new org.apache.hadoop.fs.Path(rawPath).getFileSystem(spark.sparkContext.hadoopConfiguration)
    def readLog(path: String): String = {
      val input = fileSystem.open(new org.apache.hadoop.fs.Path(path))
      try new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
      finally input.close()
    }
    val legacyMapper = new ObjectMapper()
    val legacyEntry = jsonLogs
      .map(path => path -> readLog(path))
      .flatMap { case (path, contents) =>
        contents.linesIterator.map(line => path -> legacyMapper.readTree(line)).find(_._2.has("add"))
      }
      .headOption
      .getOrElse(fail("legacy raw table should contain an AddFile action"))
    val legacyJson = legacyEntry._1
    val legacyAdd = legacyEntry._2.get("add")
    val legacyStats = legacyMapper.readTree(legacyAdd.get("stats").asText())
    assert(legacyStats.path("minValues").has("rawValue"), "legacy AddFile stats must include rawValue")

    batch.applyDeletionMarkers(spark, markerRows).unsafeRunSync()
    val retiredLogs = batch.checkpointPurgedRawLogs(spark).unsafeRunSync()
    assert(retiredLogs.contains(legacyJson))
    val baselineVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, rawPath).update().version
    val baselineFiles = fileSystem
      .listStatus(new org.apache.hadoop.fs.Path(s"$rawPath/_delta_log"))
      .filter(status => status.getPath.getName.startsWith(f"$baselineVersion%020d"))
    assert(
      baselineFiles.exists(_.getPath.getName.contains("checkpoint")),
      "purge must establish a clean checkpoint baseline"
    )
    val oldTimestamp = System.currentTimeMillis() - 3L * 24L * 60L * 60L * 1000L
    (legacyLogs ++ retiredLogs ++ baselineFiles.map(_.getPath.toString)).distinct.foreach { value =>
      val logPath = new org.apache.hadoop.fs.Path(value)
      fileSystem.setTimes(logPath, oldTimestamp, -1L)
    }
    spark.sql(s"ALTER TABLE delta.`$rawPath` SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 1 day')")
    spark
      .createDataFrame(
        Vector(Row("retained-after-erasure", Timestamp.from(Instant.parse("2026-09-24T12:00:00Z")))).asJava,
        rawSchema
      )
      .write
      .format("delta")
      .mode("append")
      .save(rawPath)
    batch.checkpointRawTableLogs(spark).unsafeRunSync()
    batch.verifyFilesAbsent(spark, retiredLogs).unsafeRunSync()
    assert(fileSystem.exists(new org.apache.hadoop.fs.Path(legacyJson)) == false)
  }
}
