package com.example.hiring.analytics
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.erasure.*
import AnalyticsBatchTestSupport.{newBatch, newKeyContinuityStage}

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.IO
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

class AnalyticsHmacKeyContinuityTransformsSpec extends AnalyticsBatchSpecFixtures {
  test("versioned HMAC key rings preserve retiring tokens for deletion markers") {
    val oldSecret = hmacKey("old-analytics-key")
    val newSecret = hmacKey("new-analytics-key")
    val oldKey = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", oldSecret, Vector.empty)
    val rotating = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v2", newSecret, Vector("hmac-v1" -> oldSecret))
    assertNotEquals(
      AnalyticsTestSubjectPseudonymizer.tokenValue(rotating, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"),
      AnalyticsTestSubjectPseudonymizer.tokenValue(oldKey, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29")
    )
    assertEquals(
      rotating.matchingTokens("19b46da1-bee5-3018-b5f7-abe9a9fdfb29"),
      Right(
        Vector(
          AnalyticsTestSubjectPseudonymizer.token(rotating, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"),
          AnalyticsTestSubjectPseudonymizer.token(oldKey, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29")
        )
      )
    )
    assertEquals(rotating.matchingTokens(null), Left("subject id must be non-empty"))
    assertEquals(rotating.matchingTokens(""), Left("subject id must be non-empty"))
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e").toString
    assertEquals(
      MongoActiveDeletionMarkerSource
        .tokensFor(AnalyticsMongoRecords.ErasureRequest(subjectId), rotating)
        .map(_.map(_.value).toSet),
      rotating
        .matchingTokens(subjectId)
        .left
        .map(AnalyticsError.InvalidConfiguration(_))
        .map(_.map(_.value).toSet)
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
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-cutover").toUri.toString)
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
          Row(
            AnalyticsTestSubjectPseudonymizer.tokenValue(old, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"),
            Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(old, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29")),
            Timestamp.from(Instant.now().plusSeconds(3600L))
          )
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
              TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L)
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
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-retirement").toUri.toString)
    newKeyContinuityStage(paths, old)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    val rotatingStage = newKeyContinuityStage(paths, rotating)
    rotatingStage.validateKeyMaterialContinuity(spark).unsafeRunSync()
    val stored = spark.createDataFrame(
      Seq(
        Row(
          AnalyticsTestSubjectPseudonymizer.tokenValue(old, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"),
          Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(rotating, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
        )
      ).asJava,
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
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-key-continuity").toUri.toString)
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
    val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-unanchored-key").toUri.toString)
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromSecret(hmacKey("legacy-key-material"))
    spark
      .createDataFrame(
        Seq(
          Row(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
        ).asJava,
        StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
      )
      .write
      .format("delta")
      .save(paths.silver)
    val error = intercept[AnalyticsError.InvalidConfiguration] {
      newKeyContinuityStage(paths, pseudonymizer).validateKeyMaterialContinuity(spark).unsafeRunSync()
    }
    assert(error.getMessage.contains("no HMAC key continuity registry"))
    assert(error.getMessage.contains("reset or rebuild this local lakehouse explicitly"))
  }

  test("a configured key ID with stored rows but no registry anchor fails closed") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-partial-key-registry").toUri.toString)
    val current =
      AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v2", hmacKey("current-registry-key"), Vector.empty)
    newKeyContinuityStage(paths, current)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    val old = AnalyticsTestSubjectPseudonymizer.fromKeyRing("hmac-v1", hmacKey("old-unanchored-key"), Vector.empty)
    spark
      .createDataFrame(
        Seq(Row(AnalyticsTestSubjectPseudonymizer.tokenValue(old, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))).asJava,
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
      TestAnalyticsLakehousePaths.unsafe(
        Files.createTempDirectory("analytics-marker-before-key-registry").toUri.toString
      )
    val unavailableMarkers = new ActiveDeletionMarkerSource[IO] {
      override def activeDeletionMarkers: IO[ActiveDeletionMarkers] =
        IO.raiseError(AnalyticsError.MissingMarkerCollection)
    }
    val batch = newBatch(paths, pseudonymizer, unavailableMarkers)
    val source =
      DataFrameBatchSource[IO](
        records(
          Seq(
            ("hiring.operational-events", 0, 0L, event("41196390-92e6-3c55-aa8b-e348e4d9260d", "APPLICATION_CREATED"))
          )
        )
      )
    intercept[AnalyticsError.MissingMarkerCollection.type] {
      batch
        .run(
          spark,
          source,
          validatedManifest(
            "marker-before-key-registry",
            Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
          )
        )
        .unsafeRunSync()
    }
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("lazy marker evaluation failure does not create key registry or reserve publication") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(
        Files.createTempDirectory("analytics-marker-action-before-write").toUri.toString
      )
    val reserved = new AtomicBoolean(false)
    val failingMarkers = spark
      .range(1L)
      .select(org.apache.spark.sql.functions.expr("raise_error('marker evaluation failed')").as("subjectToken"))
    val publisher = new AnalyticsReportPublisher[IO] {
      override def publicationReceipt(
          reservation: AnalyticsReportReservation
      ): IO[AnalyticsReportPublicationReceipt] = IO.pure(AnalyticsReportPublicationReceipt.Absent)

      override def reservePinned(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        reserve(runId, rangeFingerprint, now)

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
        records(
          Seq(
            ("hiring.operational-events", 0, 0L, event("41196390-92e6-3c55-aa8b-e348e4d9260d", "APPLICATION_CREATED"))
          )
        )
      )
    intercept[AnalyticsError.LakehouseFailure] {
      batch
        .run(
          spark,
          source,
          validatedManifest(
            "marker-action-before-key-registry",
            Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
          )
        )
        .unsafeRunSync()
    }
    assert(!reserved.get())
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.hmacKeyRegistry))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }
}
