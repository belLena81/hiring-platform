package com.example.hiring.analytics
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.erasure.*
import AnalyticsBatchTestSupport.newBatch

import com.example.hiring.analytics.adapter.spark.*

import cats.effect.IO
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.col

import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

class AnalyticsBatchPublicationSpec extends AnalyticsBatchSpecFixtures {
  test("bounded batch persists replayable layers and quality-blocks Gold on malformed or conflicting records") {
    val lakehouse = lakehouseDirectory("hiring-analytics-batch").toUri.toString.stripSuffix("/")
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            AnalyticsOperationalEventFixtures.id(s"created-$index"),
            "APPLICATION_CREATED",
            aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$index"),
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      ) ++ Seq(
        ("hiring.operational-events", 0, 11L, event("bae60998-ffe4-323b-931e-3d6e4c19993e", "APPLICATION_CREATED")),
        (
          "hiring.operational-events",
          0,
          12L,
          event("bae60998-ffe4-323b-931e-3d6e4c19993e", "APPLICATION_STATUS_CHANGED")
        ),
        ("hiring.operational-events", 0, 13L, event("c3016361-5770-38b2-b887-3288e828de41", "APPLICATION_CREATED"))
      )
    )
    val manifest =
      validatedManifest("run-1", Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 13L)))
    val publication = newBatch(
      TestAnalyticsLakehousePaths.unsafe(lakehouse),
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
    assertEquals(spark.read.format("delta").load(TestAnalyticsLakehousePaths.unsafe(lakehouse).silver).count(), 10L)
    assertEquals(
      spark.read
        .format("delta")
        .load(TestAnalyticsLakehousePaths.unsafe(lakehouse).manifests)
        .filter(col("status") === "QUALITY_BLOCKED")
        .count(),
      1L
    )
  }

  test("batch metrics preserve the conflicting event and row counts") {
    val lakehouse = lakehouseDirectory("hiring-analytics-conflict-metrics").toUri.toString.stripSuffix("/")
    val input = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("f218881d-88d8-3ba4-bb2a-c095006f866f", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 2L, event("f218881d-88d8-3ba4-bb2a-c095006f866f", "APPLICATION_CREATED")),
        (
          "hiring.operational-events",
          0,
          3L,
          event("f218881d-88d8-3ba4-bb2a-c095006f866f", "APPLICATION_STATUS_CHANGED")
        )
      )
    )
    val manifest = validatedManifest(
      "conflict-metrics",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 4L))
    )

    val publication = newBatch(
      TestAnalyticsLakehousePaths.unsafe(lakehouse),
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    ).run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()

    assertEquals(publication.conflictingEventIds, 1L)
    assertEquals(publication.quarantinedRecords, 3L)
    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
  }

  test("batch metrics count historical event conflicts and their quarantined rows") {
    val lakehouse = lakehouseDirectory("hiring-analytics-historical-conflict").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
    val original = records(
      Seq(("hiring.operational-events", 0, 1L, event("1885f141-e20c-3605-8e64-57298ba20506", "APPLICATION_CREATED")))
    )
    val changed = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          2L,
          event(
            "1885f141-e20c-3605-8e64-57298ba20506",
            "APPLICATION_CREATED",
            payload = Some(
              """{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"d625db79-80ee-34f9-8fc1-0fb01896355f"}"""
            )
          )
        )
      )
    )

    val first = batch
      .run(
        spark,
        DataFrameBatchSource[IO](original),
        validatedManifest(
          "historical-conflict-seed",
          Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L))
        )
      )
      .unsafeRunSync()
    val replay = batch
      .run(
        spark,
        DataFrameBatchSource[IO](changed),
        validatedManifest(
          "historical-conflict-replay",
          Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 2L, 3L))
        )
      )
      .unsafeRunSync()

    assertEquals(first.conflictingEventIds, 0L)
    assertEquals(replay.conflictingEventIds, 1L)
    assertEquals(replay.quarantinedRecords, 1L)
    assertEquals(replay.outcome, AnalyticsRunOutcome.QualityBlocked)
  }

  test("tombstone quarantine rows deduplicate when a Kafka range is replayed") {
    val lakehouse = lakehouseDirectory("hiring-analytics-tombstone-replay").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(Seq(("hiring.operational-events", 3, 17L, null.asInstanceOf[String])))
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](emptyMarkers),
      fixedClock(Instant.parse("2026-09-22T12:00:00Z"))
    )
    val firstManifest = validatedManifest(
      "tombstone-first-run",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 3, 17L, 18L))
    )
    val replayManifest = validatedManifest(
      "tombstone-replay-run",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 3, 17L, 18L))
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
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val source = records(Seq(("hiring.operational-events", 0, 1L, event("created", "APPLICATION_CREATED"))))
    val markers = new ActiveDeletionMarkerSource[IO] {
      override def activeDeletionMarkers: IO[ActiveDeletionMarkers] =
        IO.raiseError(AnalyticsError.MissingMarkerCollection)
    }
    val batch = newBatch(paths, pseudonymizer, markers)

    val failure = intercept[AnalyticsError.MissingMarkerCollection.type] {
      batch
        .run(
          spark,
          DataFrameBatchSource[IO](source),
          validatedManifest("run-fail", Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L)))
        )
        .unsafeRunSync()
    }
    assertEquals(failure, AnalyticsError.MissingMarkerCollection)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze))
  }

  test("clean bounded batch returns Published and records PUBLISHED in the manifest") {
    val lakehouse = lakehouseDirectory("hiring-analytics-published").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            AnalyticsOperationalEventFixtures.id(s"created-$index"),
            "APPLICATION_CREATED",
            aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$index"),
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val manifest =
      validatedManifest(
        "run-published",
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L))
      )
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

  test("new subject-attributed Delta tables keep tokens and fingerprints out of transaction log statistics") {
    val lakehouse = lakehouseDirectory("hiring-analytics-private-delta-stats").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val sensitiveMarker = AnalyticsOperationalEventFixtures.id("raw-log-personal-sentinel-8f17c2")
    val input = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          0L,
          event(
            "595c7311-0ae1-39fc-b4cf-167c398ea47c",
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
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
      )

    newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](emptyMarkers))
      .run(spark, DataFrameBatchSource[IO](input), manifest)
      .unsafeRunSync()

    val storedBronze = spark.read.format("delta").load(paths.bronze)
    assert(!storedBronze.columns.contains("rawValue"), "Bronze must not retain the raw envelope")
    assert(!storedBronze.toJSON.collect().mkString.contains(sensitiveMarker), "Bronze must not hold raw identifiers")
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
    def columnStatistics(
        actions: Vector[com.fasterxml.jackson.databind.JsonNode]
    ): Vector[com.fasterxml.jackson.databind.JsonNode] =
      actions.flatMap(action => Option(action.get("stats")).filterNot(_.isNull)).map { statsNode =>
        if (statsNode.isTextual) mapper.readTree(statsNode.asText()) else statsNode
      }
    val addActions = addFileActions(logContents.toVector)
    assert(addActions.nonEmpty, "the log fixture must contain AddFile actions")
    val statistics = columnStatistics(addActions)
    assert(
      statistics.forall(stats =>
        !stats.path("minValues").has("eventFingerprint") && !stats.path("maxValues").has("subjectToken")
      ),
      "fingerprint and token statistics must be disabled"
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
    assert(
      columnStatistics(addFileActions(controlContents)).exists(stats => stats.path("minValues").has("eventFingerprint"))
    )
  }

  test("Mongo publication failure never marks the Delta manifest PUBLISHED") {
    val lakehouse = lakehouseDirectory("hiring-analytics-publication-failure").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"publication-failure-$index",
            "APPLICATION_CREATED",
            aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$index"),
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val reportPublisher = new AnalyticsReportPublisher[IO] {
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
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L))
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

  test("HAL-10.1 same-range retry completes the manifest after Mongo publication succeeds") {
    val lakehouse = lakehouseDirectory("hiring-analytics-manifest-retry").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val input = records(
      (1 to 10).map(index =>
        (
          "hiring.operational-events",
          0,
          index.toLong,
          event(
            s"manifest-retry-$index",
            "APPLICATION_CREATED",
            aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$index"),
            payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
          )
        )
      )
    )
    val manifest =
      validatedManifest(
        "run-manifest-retry",
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 11L))
      )
    val published = new AtomicInteger(0)
    val failPublishedManifestOnce = new AtomicBoolean(true)
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
    val realManifestStore = new DeltaManifestStore[IO](spark, paths, sparkExecution)
    val manifestStore = new AnalyticsRunManifestStore[IO] {
      override def persist(
          run: AnalyticsRunManifest,
          status: AnalyticsManifestStatus,
          at: Instant
      ): IO[Unit] =
        if (status == AnalyticsManifestStatus.Published && failPublishedManifestOnce.compareAndSet(true, false))
          IO.raiseError(AnalyticsError.LakehouseFailure(new IllegalStateException("injected manifest failure")))
        else realManifestStore.persist(run, status, at)
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
      override def activeDeletionMarkers: IO[ActiveDeletionMarkers] = IO.delay {
        assert(!reserved.get(), "the marker snapshot must be read before publication is reserved")
        ActiveDeletionMarkers.none
      }
    }
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
      TestAnalyticsLakehousePaths.unsafe(lakehouseDirectory("analytics-reserve-order").toUri.toString),
      pseudonymizer,
      markers,
      fixedClock(Instant.parse("2026-09-22T12:00:00Z")),
      reportPublisher = publisher
    )
    val manifest =
      validatedManifest(
        "reserve-before-markers",
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L))
      )

    val error = intercept[AnalyticsError.SourceReadFailure](batch.run(spark, source, manifest).unsafeRunSync())

    assertEquals(error.getMessage, "analytics source read failed")
    assert(reserved.get())
  }
}
