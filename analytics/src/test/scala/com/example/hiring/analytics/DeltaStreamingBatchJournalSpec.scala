package com.example.hiring.analytics

import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.{DeltaStreamingBatchJournal, SparkBlockingExecution}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsReportReservation}
import com.example.hiring.analytics.service.streaming.*
import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.Files
import java.time.Instant
import java.util.Comparator
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class DeltaStreamingBatchJournalSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private lazy val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("DeltaStreamingBatchJournalSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "false")
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
    Vector(StreamingPartitionEndOffset.from("hiring.events", 0, 13L).toEither.toOption.get),
    offsets
  )
  private val decision = StreamingDecisionRevision(
    identity,
    0L,
    "b" * 64,
    Some(Instant.parse("2026-09-29T12:00:00Z")),
    AnalyticsReportReservation(right(RunId.from("stream-test-1")), fingerprint, 1L, 1L)
  )
  private val publishedAt = Instant.parse("2026-09-30T12:01:00.987654321Z")

  test("canonical progress creates and reads with Delta nested constraint enforcement enabled") {
    val strictIdentity =
      StreamingBatchIdentity(right(StreamingLineage.from("hiring-default-delta-schema")), identity.batchId)
    val input = prepared.copy(identity = strictIdentity)
    for {
      _ <- IO(
        assertEquals(spark.conf.get("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled"), "false")
      )
      _ <- journal.prepare(input)
      loaded <- journal.load(strictIdentity)
    } yield {
      assertEquals(
        loaded.map(_.preparation),
        Some(input.copy(observedAt = input.observedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS)))
      )
      val schema = spark.read.format("delta").load(paths.streamingProgress).schema
      Vector("sourceEndOffsets", "deliveredOffsets").foreach { name =>
        val array = schema(name).dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType]
        assert(array.containsNull)
        assert(array.elementType.asInstanceOf[org.apache.spark.sql.types.StructType].fields.forall(_.nullable))
      }
    }
  }

  test("physically nullable nested offsets reject null arrays, elements and every required field") {
    val sourceType = "array<struct<topic:string,partition:int,endOffset:bigint>>"
    val deliveredType =
      "array<struct<topic:string,partition:int,minimumDeliveredOffset:bigint,maximumDeliveredOffset:bigint,deliveredRecordCount:bigint>>"
    val sourceFields =
      Vector("'topic', 'hiring.events'", "'partition', CAST(0 AS INT)", "'endOffset', CAST(13 AS BIGINT)")
    val deliveredFields = Vector(
      "'topic', 'hiring.events'",
      "'partition', CAST(0 AS INT)",
      "'minimumDeliveredOffset', CAST(10 AS BIGINT)",
      "'maximumDeliveredOffset', CAST(12 AS BIGINT)",
      "'deliveredRecordCount', CAST(3 AS BIGINT)"
    )
    def missingFields(column: String, fields: Vector[String]) = fields.indices.toVector.map { index =>
      val selected = fields.updated(index, fields(index).takeWhile(_ != ',') + ", NULL")
      column -> s"array(named_struct(${selected.mkString(", ")}))"
    }
    val cases = Vector(
      "sourceEndOffsets" -> s"CAST(NULL AS $sourceType)",
      "sourceEndOffsets" -> s"CAST(array(NULL) AS $sourceType)",
      "deliveredOffsets" -> s"CAST(NULL AS $deliveredType)",
      "deliveredOffsets" -> s"CAST(array(NULL) AS $deliveredType)"
    ) ++ missingFields("sourceEndOffsets", sourceFields) ++ missingFields("deliveredOffsets", deliveredFields)
    cases.zipWithIndex.traverse_ { case ((column, expression), index) =>
      val invalidIdentity =
        StreamingBatchIdentity(right(StreamingLineage.from(s"hiring-null-offset-$index")), identity.batchId)
      for {
        _ <- journal.prepare(prepared.copy(identity = invalidIdentity))
        _ <- execution {
          io.delta.tables.DeltaTable
            .forPath(spark, paths.streamingProgress)
            .update(
              org.apache.spark.sql.functions.col("lineage") === invalidIdentity.lineage.value,
              Map(column -> org.apache.spark.sql.functions.expr(expression))
            )
        }
        result <- journal.load(invalidIdentity).attempt
      } yield assert(
        result.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]),
        s"$column case $index decoded missing data"
      )
    }
  }

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

      val row = spark.read
        .format("delta")
        .load(paths.streamingProgress)
        .filter(
          org.apache.spark.sql.functions.col("lineage") === identity.lineage.value &&
            org.apache.spark.sql.functions.col("batchId") === identity.batchId.value
        )
        .head()
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
      reservationFailure <- journal
        .appendDecision(
          conflictDecision.copy(publicationReservation = conflictDecision.publicationReservation.copy(generation = 2L))
        )
        .attempt
    } yield {
      assert(preparationFailure.left.exists(_.isInstanceOf[AnalyticsError.InvalidInput]))
      assert(decisionFailure.left.exists(_.isInstanceOf[AnalyticsError.InvalidInput]))
      assert(reservationFailure.left.exists(_.isInstanceOf[AnalyticsError.InvalidInput]))
    }
  }

  test("terminal non-published outcomes do not advance watermark; published null candidate preserves it") {
    val terminalLineage = right(StreamingLineage.from("hiring-terminal-outcomes"))
    val publishedIdentity = identity.copy(lineage = terminalLineage)
    val initialPreparation = prepared.copy(identity = publishedIdentity)
    val initialDecision = decision.copy(identity = publishedIdentity)
    val blockedIdentity = StreamingBatchIdentity(terminalLineage, right(StreamingBatchId.from(3L)))
    val blockedPreparation = prepared.copy(identity = blockedIdentity)
    val emptyCandidateDecision = decision.copy(
      identity = blockedIdentity,
      deletionMarkerFingerprint = "d" * 64,
      candidateWatermark = None
    )
    val emptyPublishedIdentity = StreamingBatchIdentity(terminalLineage, right(StreamingBatchId.from(4L)))
    val emptyPreparation = prepared.copy(identity = emptyPublishedIdentity)
    val emptyDecision = emptyCandidateDecision.copy(identity = emptyPublishedIdentity)
    for {
      _ <- journal.prepare(initialPreparation)
      _ <- journal.appendDecision(initialDecision)
      _ <- journal.markIngestionCommitted(publishedIdentity)
      _ <- journal.commitPublished(initialDecision, publishedAt)
      _ <- journal.prepare(blockedPreparation)
      _ <- journal.appendDecision(emptyCandidateDecision)
      _ <- journal.markIngestionCommitted(blockedIdentity)
      _ <- journal.complete(blockedIdentity, StreamingTerminalOutcome.QualityBlocked, publishedAt)
      _ <- journal.prepare(emptyPreparation)
      _ <- journal.appendDecision(emptyDecision)
      _ <- journal.markIngestionCommitted(emptyPublishedIdentity)
      _ <- journal.commitPublished(emptyDecision, publishedAt.plusSeconds(1))
      watermark <- journal.latestWatermark(terminalLineage)
      blocked <- journal.load(blockedIdentity)
      emptyPublished <- journal.load(emptyPublishedIdentity)
    } yield {
      assertEquals(watermark, decision.candidateWatermark)
      assertEquals(blocked.flatMap(_.terminalOutcome), Some(StreamingTerminalOutcome.QualityBlocked))
      assertEquals(emptyPublished.flatMap(_.terminalOutcome), Some(StreamingTerminalOutcome.Published))
    }
  }

  test("a null pinned publication generation cannot decode as zero") {
    val malformedIdentity =
      StreamingBatchIdentity(right(StreamingLineage.from("hiring-null-reservation")), identity.batchId)
    for {
      _ <- journal.prepare(prepared.copy(identity = malformedIdentity))
      _ <- journal.appendDecision(decision.copy(identity = malformedIdentity))
      _ <- execution {
        io.delta.tables.DeltaTable
          .forPath(spark, paths.streamingDecisions)
          .update(
            org.apache.spark.sql.functions.col("lineage") === malformedIdentity.lineage.value,
            Map("publicationGeneration" -> org.apache.spark.sql.functions.lit(null).cast("long"))
          )
      }
      result <- journal.load(malformedIdentity).attempt
    } yield assert(result.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
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

  test("pruning retains Spark recovery, every unfinished batch, watermark, latest terminal and recent progress") {
    val pruningLineage = right(StreamingLineage.from("hiring-pruning"))
    val at = publishedAt.plusSeconds(10L * 86400L)
    val ids =
      (0L to 7L).toVector.map(value => StreamingBatchIdentity(pruningLineage, right(StreamingBatchId.from(value))))
    val peerIdentity =
      StreamingBatchIdentity(right(StreamingLineage.from("hiring-peer-stream")), right(StreamingBatchId.from(0L)))
    val peerDecision = decision.copy(
      identity = peerIdentity,
      publicationReservation = decision.publicationReservation.copy(runId = right(RunId.from("peer-stream-receipt")))
    )
    for {
      _ <- journal.prepare(prepared.copy(identity = peerIdentity)) *> journal.appendDecision(peerDecision)
      _ <- ids.traverse_ { id =>
        val preparation = prepared.copy(identity = id)
        val revision = decision.copy(
          identity = id,
          publicationReservation =
            decision.publicationReservation.copy(runId = right(RunId.from(s"prune-${id.batchId.value}")))
        )
        journal.prepare(preparation) *> journal.appendDecision(revision) *>
          (if (id.batchId.value == 3L) IO.unit
           else
             journal.markIngestionCommitted(id) *>
               (if (id.batchId.value == 4L) IO.unit
                else if (id.batchId.value == 2L) journal.commitPublished(revision, publishedAt)
                else
                  journal.complete(
                    id,
                    StreamingTerminalOutcome.QualityBlocked,
                    if (id.batchId.value == 6L) at.minusSeconds(60L) else publishedAt
                  )))
      }
      _ <- journal.prune(pruningLineage, Set(ids(1).batchId), at, 7.days)
      states <- ids.traverse(journal.load)
      watermark <- journal.latestWatermark(pruningLineage)
      _ <- journal.prune(pruningLineage, Set(ids(1).batchId), at, 7.days)
      requiredReceipts <- journal.referencedPublicationRunIds(
        (0 to 7).map(i => asRunIdForMaintenance(s"prune-$i")).toSet
      )
      allReceipts <- journal.referencedPublicationRunIds(
        requiredReceipts + asRunIdForMaintenance("peer-stream-receipt")
      )
      subset <- journal.referencedPublicationRunIds(
        Set(
          asRunIdForMaintenance("peer-stream-receipt"),
          asRunIdForMaintenance("prune-1"),
          asRunIdForMaintenance("absent")
        )
      )
    } yield {
      assertEquals(requiredReceipts.map(_.value), Set("prune-1", "prune-2", "prune-3", "prune-4", "prune-6", "prune-7"))
      assert(allReceipts.map(_.value).contains("peer-stream-receipt"))
      assert(requiredReceipts.subsetOf(allReceipts))
      assertEquals(subset.map(_.value), Set("peer-stream-receipt", "prune-1"))
      assertEquals(states.zipWithIndex.collect { case (Some(_), index) => index }.toSet, Set(1, 2, 3, 4, 6, 7))
      assertEquals(watermark, decision.candidateWatermark)
      val decisions = spark.read
        .format("delta")
        .load(paths.streamingDecisions)
        .filter(org.apache.spark.sql.functions.col("lineage") === pruningLineage.value)
        .select("batchId")
        .collect()
        .map(_.getLong(0))
        .toSet
      assertEquals(decisions, Set(1L, 2L, 3L, 4L, 6L, 7L))
    }
  }

  private def asRunIdForMaintenance(value: String): RunId = RunId.from(value).toOption.get

  test("receipt dependency lookup fails closed on whitespace-only retained IDs outside the candidate set") {
    val malformedIdentity = StreamingBatchIdentity(right(StreamingLineage.from("blank-receipt")), identity.batchId)
    val original = decision.publicationReservation.runId
    for {
      _ <- journal.prepare(prepared.copy(identity = malformedIdentity))
      _ <- journal.appendDecision(decision.copy(identity = malformedIdentity))
      _ <- execution {
        io.delta.tables.DeltaTable
          .forPath(spark, paths.streamingDecisions)
          .update(
            org.apache.spark.sql.functions.col("lineage") === malformedIdentity.lineage.value,
            Map("publicationRunId" -> org.apache.spark.sql.functions.lit(" \t\n"))
          )
      }
      result <- journal
        .referencedPublicationRunIds(Set(original))
        .attempt
        .guarantee(execution {
          io.delta.tables.DeltaTable
            .forPath(spark, paths.streamingDecisions)
            .update(
              org.apache.spark.sql.functions.col("lineage") === malformedIdentity.lineage.value,
              Map("publicationRunId" -> org.apache.spark.sql.functions.lit(original.value))
            )
        })
    } yield assert(result.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
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
      try
        paths.sorted(Comparator.reverseOrder()).forEach { file =>
          val _ = Files.deleteIfExists(file)
        }
      finally paths.close()
    }
  }
}
