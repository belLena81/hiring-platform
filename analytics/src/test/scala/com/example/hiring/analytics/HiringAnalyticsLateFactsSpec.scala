package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.{
  AnalyticsTestOperationalConfig,
  AnalyticsTestSubjectPseudonymizer,
  TestAnalyticsLakehousePaths
}
import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.delta.tables.DeltaTable
import munit.FunSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.{col, current_timestamp, encode, lit}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class HiringAnalyticsLateFactsSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes
  private val execution = SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)

  private lazy val spark: SparkSession = org.apache.spark.sql.classic.SparkSession
    .builder()
    .master("local[2]")
    .appName("HiringAnalyticsLateFactsSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  override def afterAll(): Unit =
    if (!spark.sparkContext.isStopped) spark.stop()

  test("closed-day facts are normalized, idempotent, deletion-aware, and isolated from reports") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-facts").toUri.toString)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    val stage = new AnalyticsLateFactStage[IO](paths, execution, writer)
    val observedAt = Instant.parse("2026-09-30T12:00:00Z")
    val initial = closedDayEvents()

    stage.persistClosedDayFacts(initial, markers(Vector.empty), observedAt).unsafeRunSync()
    stage.persistClosedDayFacts(initial, markers(Vector.empty), observedAt).unsafeRunSync()

    val stored = spark.read.format("delta").load(paths.lateFacts)
    assertEquals(stored.count(), 2L)
    assertEquals(stored.columns.toVector, AnalyticsTableSchemas.lateFacts.map(_._1))
    assert(!stored.columns.exists(Set("rawValue", "payload", "actorId", "aggregateId", "candidateId")))
    assertEquals(
      stored.select("expiresAt").distinct().head().getTimestamp(0).toInstant,
      observedAt.plus(30, java.time.temporal.ChronoUnit.DAYS)
    )
    assertEquals(
      DeltaTable
        .forPath(spark, paths.lateFacts)
        .detail()
        .select("properties")
        .head()
        .getAs[scala.collection.Map[String, String]]("properties")
        .get("delta.dataSkippingNumIndexedCols"),
      Some("0")
    )

    val marker = token("candidate-token-2")
    val erasure = new AnalyticsBatchErasureStage[IO](paths, execution, _ => IO.unit, 100)
    assertEquals(erasure.countMarkedRows(spark, markers(Vector(marker))).unsafeRunSync(), 1L)
    erasure.purgeMarkedSubjectRows(spark, paths.lateFacts, markers(Vector(marker))).unsafeRunSync()
    erasure.verifyMarkedSubjectsAbsent(spark, markers(Vector(marker))).unsafeRunSync()
    assertEquals(spark.read.format("delta").load(paths.lateFacts).count(), 1L)

    val report = AnalyticsGoldStage.extract[IO](spark, paths, observedAt, execution).unsafeRunSync()
    assertEquals(report.funnel, Vector.empty)
    assertEquals(report.skillPostingActivity, Vector.empty)
    assertEquals(report.timeToHire, None)
  }

  test("late-fact schema errors use the typed analytics error channel") {
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-facts-schema").toUri.toString)
    val stage = new AnalyticsLateFactStage[IO](paths, execution, new DeltaBatchWriter[IO](paths, execution))
    val invalid = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("event-id"))),
      StructType(Seq(StructField("eventId", IntegerType, nullable = true)))
    )
    val result = stage.persistClosedDayFacts(invalid, markers(Vector.empty), Instant.EPOCH).attempt.unsafeRunSync()
    assert(result.swap.toOption.exists(_.isInstanceOf[AnalyticsError.InvalidLateFactSchema]))
  }

  test("conflicting event IDs are detected against retained late facts") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("hiring-late-fact-conflict").toUri.toString)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(2))
    val lateFacts = new AnalyticsLateFactStage[IO](paths, execution, writer)
    val observedAt = Instant.parse("2026-09-30T12:00:00Z")
    val initial = closedDayEvents().filter(col("eventId") === "event-1")
    lateFacts.persistClosedDayFacts(initial, markers(Vector.empty), observedAt).unsafeRunSync()

    val raw = eventEnvelope("event-1", "APPLICATION_CREATED", "new event payload")
    val parsed = OperationalEventTransforms.parseKafkaRecords(kafkaRecords(Vector(raw)))
    val silver = new AnalyticsBatchSilverStage[IO](
      paths,
      pseudonymizer,
      execution,
      writer,
      new DeltaBatchReader[IO](execution),
      QuarantineIdentifier,
      AnalyticsTestOperationalConfig.operational.retention
    )
    val prepared = silver
      .separateQuarantine(spark, AnalyticsBronzeInput(parsed, observedAt, 1L, 1L, 0L), markers(Vector.empty), false)
      .unsafeRunSync()

    assertEquals(prepared.conflicts.count(), 1L)
    assertEquals(prepared.conflictingEventIds, 1L)
    assert(!DeltaTable.isDeltaTable(spark, paths.silver))
  }

  test("suppressed-only candidate events do not advance the watermark") {
    val eventTime = Timestamp.from(Instant.parse("2026-09-30T10:00:00Z"))
    val candidates = visibleFunnelFacts(1, eventTime, 100).withColumn("_effectiveTime", col("occurredAt"))
    val suppressedExisting = visibleFunnelFacts(8, eventTime)
      .withColumn("ingestedAt", lit(eventTime))
      .withColumn("expiresAt", lit(eventTime))
    val visibleExisting = visibleFunnelFacts(9, eventTime)
      .withColumn("ingestedAt", lit(eventTime))
      .withColumn("expiresAt", lit(eventTime))

    assertEquals(
      StreamingWatermarkAdmission
        .visibleCandidateEvents(suppressedExisting, candidates, "_effectiveTime")
        .count(),
      0L
    )
    assertEquals(
      StreamingWatermarkAdmission
        .visibleCandidateEvents(visibleExisting, candidates, "_effectiveTime")
        .count(),
      1L
    )
    val mixedSuppressedDay = visibleFunnelFacts(9, eventTime)
      .unionByName(funnelStatusFacts(eventTime))
    assertEquals(
      StreamingWatermarkAdmission
        .visibleCandidateEvents(mixedSuppressedDay, candidates, "_effectiveTime")
        .count(),
      0L
    )
  }

  private def visibleFunnelFacts(count: Int, eventTime: Timestamp, startIndex: Int = 0): DataFrame = {
    spark.createDataFrame(
      (startIndex until startIndex + count)
        .map(index => silverRow(index, "APPLICATION_CREATED", null, eventTime))
        .asJava,
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver)
    )
  }

  private def funnelStatusFacts(eventTime: Timestamp): DataFrame = {
    spark.createDataFrame(
      Seq(silverRow(1000, "APPLICATION_STATUS_CHANGED", "Accepted", eventTime)).asJava,
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver)
    )
  }

  private def silverRow(index: Int, eventType: String, status: String, eventTime: Timestamp): Row = {
    val subject = s"candidate-$index"
    Row(
      s"event-$index",
      eventType,
      eventTime,
      "Application",
      s"application-$index",
      s"application-$index",
      "job-id",
      status,
      Seq.empty[String],
      subject,
      Seq(subject),
      s"fingerprint-$index"
    )
  }

  private def closedDayEvents(): DataFrame = {
    val schema = StructType(
      Seq(
        StructField("eventId", StringType, nullable = false),
        StructField("rawValue", StringType, nullable = false),
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("offset", LongType, nullable = false),
        StructField("occurredAt", TimestampType, nullable = false),
        StructField("subjectToken", StringType, nullable = false),
        StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = false)
      )
    )
    val eventTime = Timestamp.from(Instant.parse("2026-09-20T10:00:00Z"))
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(
            "event-1",
            "raw envelope with candidate-1",
            "hiring.events",
            Int.box(0),
            Long.box(10L),
            eventTime,
            token("candidate-token-1").value,
            Seq(token("candidate-token-1").value)
          ),
          Row(
            "event-2",
            "raw envelope with candidate-2",
            "hiring.events",
            Int.box(1),
            Long.box(20L),
            eventTime,
            token("candidate-token-2").value,
            Seq(token("candidate-token-2").value)
          )
        )
      ),
      schema
    )
  }

  private def markers(tokens: Vector[SubjectToken]): DataFrame = {
    val schema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    spark.createDataFrame(spark.sparkContext.parallelize(tokens.map(token => Row(token.value))), schema)
  }

  private def token(subject: String): SubjectToken = {
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(1))
    AnalyticsTestSubjectPseudonymizer.token(pseudonymizer, subject)
  }

  private def kafkaRecords(values: Vector[String]): DataFrame = {
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
        spark.sparkContext.parallelize(values.zipWithIndex.map { case (value, index) =>
          Row("hiring.events", Int.box(0), Long.box(index.toLong), value)
        }),
        schema
      )
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", current_timestamp())
  }

  private def eventEnvelope(id: String, eventType: String, rawPayload: String): String =
    s"""{"eventId":"$id","eventType":"$eventType","occurredAt":"2026-09-20T10:00:00Z","aggregateType":"Application","aggregateId":"application-1","actorId":"actor-1","payload":{"applicationId":"application-1","candidateId":"candidate-1","jobId":"job-1","newStatus":"Hired","eventNote":"$rawPayload"}}"""
}
