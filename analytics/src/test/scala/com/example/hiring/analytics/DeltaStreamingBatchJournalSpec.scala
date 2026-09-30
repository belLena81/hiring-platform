package com.example.hiring.analytics

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.adapter.spark.{DeltaStreamingBatchJournal, SparkBlockingExecution}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.streaming.*
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.Files
import java.time.Instant
import java.util.Comparator
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class DeltaStreamingBatchJournalSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes

  private lazy val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("DeltaStreamingBatchJournalSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  private val execution = SparkBlockingExecution.forTests[IO](ExecutionContext.parasitic)
  private val rootPath = Files.createTempDirectory("analytics-streaming-journal-")
  private val root = rootPath.toUri.toString
  private val paths = right(AnalyticsLakehousePaths.from(root).toEither.left.map(_.toString))
  private val journal = new DeltaStreamingBatchJournal[IO](spark, paths, execution)

  private val lineage = right(StreamingLineage.from("hiring-stream"))
  private val identity = StreamingBatchIdentity(lineage, right(StreamingBatchId.from(1L)))
  private val fingerprint = right(RangeFingerprint.from("a" * 64))
  private val offsets = Vector(
    right(
      StreamingPartitionSummary
        .from("hiring.events", 0, 10L, 12L, 3L)
        .toEither
        .left
        .map(_.toString)
    )
  )
  private val prepared = StreamingInputPreparation(
    identity,
    Instant.parse("2026-09-30T12:00:00.123456789Z"),
    None,
    fingerprint,
    offsets
  )
  private val decision = StreamingDecisionRevision(
    identity,
    0L,
    "b" * 64,
    Some(Instant.parse("2026-09-29T12:00:00Z"))
  )
  private val publishedAt = Instant.parse("2026-09-30T12:01:00.987654321Z")

  test("immutable preparation, pre-ingestion decisions, and published watermark commit are idempotent") {
    for {
      _ <- journal.prepare(prepared)
      initial <- journal.load(identity)
      _ <- journal.appendDecision(decision)
      _ <- journal.appendDecision(decision)
      _ <- journal.markIngestionCommitted(identity)
      _ <- journal.markIngestionCommitted(identity)
      _ <- journal.commitPublished(decision, publishedAt)
      _ <- journal.commitPublished(decision, publishedAt.plusSeconds(10))
      loaded <- journal.load(identity)
      watermark <- journal.latestWatermark(lineage)
      hasState <- journal.hasLineageState(lineage)
    } yield {
      assertEquals(
        initial.map(_.preparation),
        Some(prepared.copy(observedAt = prepared.observedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS)))
      )
      assertEquals(initial.map(_.ingestionCommitted), Some(false))
      assertEquals(loaded.map(_.terminalOutcome), Some(Some(StreamingTerminalOutcome.Published)))
      assertEquals(loaded.flatMap(_.latestDecision), Some(decision))
      assertEquals(watermark, decision.candidateWatermark)
      assert(hasState)

      val row = spark.read.format("delta").load(paths.streamingProgress).head()
      assert(row.getAs[String]("outcome") == "Published")
      assertEquals(
        row.getAs[java.sql.Timestamp]("candidateWatermark").toInstant,
        expectSome(decision.candidateWatermark)
      )
      assertEquals(
        row.getAs[java.sql.Timestamp]("completedAt").toInstant,
        publishedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS)
      )
    }
  }

  test("conflicting preparation and decision retries fail closed") {
    val conflictIdentity = StreamingBatchIdentity(lineage, right(StreamingBatchId.from(2L)))
    val conflictPreparation = prepared.copy(identity = conflictIdentity)
    val conflictDecision = decision.copy(identity = conflictIdentity)
    val changedPreparation = conflictPreparation.copy(inputFingerprint = right(RangeFingerprint.from("c" * 64)))
    val changedDecision =
      conflictDecision.copy(candidateWatermark = conflictDecision.candidateWatermark.map(_.plusSeconds(1)))
    for {
      _ <- journal.prepare(conflictPreparation)
      _ <- journal.appendDecision(conflictDecision)
      preparationFailure <- journal.prepare(changedPreparation).attempt
      decisionFailure <- journal.appendDecision(changedDecision).attempt
    } yield {
      assert(preparationFailure.left.exists(_.isInstanceOf[AnalyticsError.InvalidInput]))
      assert(decisionFailure.left.exists(_.isInstanceOf[AnalyticsError.InvalidInput]))
    }
  }

  test("terminal non-published outcomes do not advance watermark; published null candidate preserves it") {
    val blockedIdentity = StreamingBatchIdentity(lineage, right(StreamingBatchId.from(3L)))
    val blockedPreparation = prepared.copy(identity = blockedIdentity)
    val emptyCandidateDecision = decision.copy(
      identity = blockedIdentity,
      deletionMarkerFingerprint = "d" * 64,
      candidateWatermark = None
    )
    val emptyPublishedIdentity = StreamingBatchIdentity(lineage, right(StreamingBatchId.from(4L)))
    val emptyPreparation = prepared.copy(identity = emptyPublishedIdentity)
    val emptyDecision = emptyCandidateDecision.copy(identity = emptyPublishedIdentity)
    for {
      _ <- journal.prepare(prepared)
      _ <- journal.appendDecision(decision)
      _ <- journal.markIngestionCommitted(identity)
      _ <- journal.commitPublished(decision, publishedAt)
      _ <- journal.prepare(blockedPreparation)
      _ <- journal.appendDecision(emptyCandidateDecision)
      _ <- journal.markIngestionCommitted(blockedIdentity)
      _ <- journal.complete(blockedIdentity, StreamingTerminalOutcome.QualityBlocked, publishedAt)
      _ <- journal.prepare(emptyPreparation)
      _ <- journal.appendDecision(emptyDecision)
      _ <- journal.markIngestionCommitted(emptyPublishedIdentity)
      _ <- journal.commitPublished(emptyDecision, publishedAt.plusSeconds(1))
      watermark <- journal.latestWatermark(lineage)
      blocked <- journal.load(blockedIdentity)
      emptyPublished <- journal.load(emptyPublishedIdentity)
    } yield {
      assertEquals(watermark, decision.candidateWatermark)
      assertEquals(blocked.flatMap(_.terminalOutcome), Some(StreamingTerminalOutcome.QualityBlocked))
      assertEquals(emptyPublished.flatMap(_.terminalOutcome), Some(StreamingTerminalOutcome.Published))
    }
  }

  test("existing incompatible Delta schema is rejected instead of adopted") {
    val incompatibleRoot = Files.createTempDirectory("analytics-streaming-journal-schema-")
    val incompatiblePaths = right(
      AnalyticsLakehousePaths.from(incompatibleRoot.toUri.toString).toEither.left.map(_.toString)
    )
    val badSchema = org.apache.spark.sql.types.StructType(
      Vector(
        org.apache.spark.sql.types.StructField("unexpected", org.apache.spark.sql.types.StringType, nullable = true)
      )
    )
    try {
      spark
        .createDataFrame(Vector(org.apache.spark.sql.Row("value")).asJava, badSchema)
        .write
        .format("delta")
        .save(incompatiblePaths.streamingProgress)
      val incompatible = new DeltaStreamingBatchJournal[IO](spark, incompatiblePaths, execution)

      val result = incompatible.prepare(prepared).attempt.unsafeRunSync()
      assert(result.left.exists(_.isInstanceOf[AnalyticsError]))
    } finally removeTree(incompatibleRoot)
  }

  override def afterAll(): Unit = {
    if (!spark.sparkContext.isStopped) spark.stop()
    removeTree(rootPath)
  }

  private def right[A](value: Either[String, A]): A = value match {
    case Right(result) => result
    case Left(problem) => fail(s"test fixture is invalid: $problem")
  }

  private def expectSome[A](value: Option[A]): A = value match {
    case Some(result) => result
    case None         => fail("test fixture unexpectedly omitted a value")
  }

  private def removeTree(path: java.nio.file.Path): Unit = {
    if (Files.exists(path)) {
      val paths = Files.walk(path)
      try paths.sorted(Comparator.reverseOrder()).forEach(file => Files.deleteIfExists(file))
      finally paths.close()
    }
  }
}
