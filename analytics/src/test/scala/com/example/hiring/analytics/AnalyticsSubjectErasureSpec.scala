package com.example.hiring.analytics
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import AnalyticsBatchTestSupport.{newBatch, newKeyContinuityStage}

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.IO
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.*

class AnalyticsSubjectErasureSpec extends AnalyticsBatchSpecFixtures {
  test("active deletion markers purge stored Silver and rebuild Gold before a quality-blocked range") {
    val lakehouse = lakehouseDirectory("hiring-analytics-silver-erasure").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val initialEvents = (1 to 10).map(index =>
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
    val seedParsed = OperationalEventTransforms.parseKafkaRecords(records(initialEvents))
    val seedAt = Instant.parse("2026-09-22T12:00:00Z")
    val seedSilver = silverFrame(OperationalEventTransforms.validEvents(seedParsed), pseudonymizer, emptyMarkers)
      .withColumn("ingestedAt", lit(Timestamp.from(seedAt)))
      .withColumn("expiresAt", lit(Timestamp.from(seedAt.plusSeconds(30L * 24L * 60L * 60L))))
    newKeyContinuityStage(paths, pseudonymizer)
      .validateKeyMaterialContinuity(spark)
      .unsafeRunSync()
    com.example.hiring.analytics.adapter.spark.AnalyticsTableSchemas.createOrValidate(
      spark,
      paths.silver,
      com.example.hiring.analytics.adapter.spark.AnalyticsDeltaCatalog.Silver
    )
    seedSilver.write.format("delta").mode("append").save(paths.silver)
    com.example.hiring.analytics.adapter.spark.AnalyticsTableSchemas.createOrValidate(
      spark,
      paths.funnelGold,
      com.example.hiring.analytics.adapter.spark.AnalyticsDeltaCatalog.FunnelGold
    )
    HiringGoldTransforms
      .wideFunnelDay(seedSilver)
      .toOption
      .get
      .write
      .format("delta")
      .mode("append")
      .save(paths.funnelGold)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 10L)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).count(), 1L)

    val replayAndMalformed = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          11L,
          event(
            "f2e7cb06-206a-3ab5-902a-1458ee7d0fe2",
            "APPLICATION_CREATED",
            aggregateId = "a053804b-8231-3cb4-8074-834a69aaccab",
            payload = Some(
              """{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"19b46da1-bee5-3018-b5f7-abe9a9fdfb29"}"""
            )
          )
        ),
        ("hiring.operational-events", 0, 12L, "not-json")
      )
    )
    val token = SubjectToken
      .fromHmac(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
      .toOption
      .get
    val markers = markerFrame(Seq(token.value))
    val deletionBatch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](markers),
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    )
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(
      spark,
      paths,
      pseudonymizer,
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    )
    intercept[AnalyticsError.MarkedSubjectRetained] {
      maintenance.verifyMarkedSubjectsAbsent(Vector(token)).unsafeRunSync()
    }
    val deletionRun = validatedManifest(
      "erasure-with-bad-range",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 11L, 13L))
    )

    val publication =
      deletionBatch.run(spark, DataFrameBatchSource[IO](replayAndMalformed), deletionRun).unsafeRunSync()
    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assertEquals(publication.suppressedRecords, 1L)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 9L)
    assertEquals(spark.read.format("delta").load(paths.quarantine).count(), 0L)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).count(), 0L)
    maintenance.verifyMarkedSubjectsAbsent(Vector(token)).unsafeRunSync()
  }

  test("AEC-07.1 a Complete marker drops its subject's replayed rows and the report is still published") {
    val lakehouse = lakehouseDirectory("hiring-analytics-completed-marker").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val erasedCandidate = "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"
    val keptCandidate = "2e6a1d7c-4d0b-3d58-9b7a-0c7a1f2f6a11"
    val input = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          21L,
          event(
            "f2e7cb06-206a-3ab5-902a-1458ee7d0fe2",
            "APPLICATION_CREATED",
            aggregateId = "a053804b-8231-3cb4-8074-834a69aaccab",
            payload =
              Some(s"""{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"$erasedCandidate"}""")
          )
        ),
        (
          "hiring.operational-events",
          0,
          22L,
          event(
            "0b0fb4a4-8f0c-3d6b-8d51-5c1f7d0c9a77",
            "APPLICATION_CREATED",
            aggregateId = "b1d0a3f2-2f6e-3f55-9d8c-6f3a3b7f1c22",
            payload =
              Some(s"""{"applicationId":"b1d0a3f2-2f6e-3f55-9d8c-6f3a3b7f1c22","candidateId":"$keptCandidate"}""")
          )
        )
      )
    )
    val token = SubjectToken
      .fromHmac(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, erasedCandidate))
      .toOption
      .get
    val manifest = validatedManifest(
      "run-complete-marker",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 21L, 23L))
    )
    val publication = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](markerFrame(Seq(token.value)), erasureInProgress = false),
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    ).run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()

    assertEquals(publication.outcome, AnalyticsRunOutcome.Published)
    assertEquals(publication.suppressedRecords, 1L)
    assertEquals(spark.read.format("delta").load(paths.silver).count(), 1L)
    assertEquals(spark.read.format("delta").load(paths.manifests).filter(col("status") === "PUBLISHED").count(), 1L)
  }

  test("AEC-07.2 a marker whose request is still in progress hides the batch report") {
    val lakehouse = lakehouseDirectory("hiring-analytics-in-progress-marker").toUri.toString.stripSuffix("/")
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouse)
    val erasedCandidate = "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"
    val input = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          21L,
          event(
            "f2e7cb06-206a-3ab5-902a-1458ee7d0fe2",
            "APPLICATION_CREATED",
            aggregateId = "a053804b-8231-3cb4-8074-834a69aaccab",
            payload =
              Some(s"""{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"$erasedCandidate"}""")
          )
        )
      )
    )
    val token = SubjectToken
      .fromHmac(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, erasedCandidate))
      .toOption
      .get
    val manifest = validatedManifest(
      "run-processing-marker",
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 21L, 22L))
    )
    val publication = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](markerFrame(Seq(token.value)), erasureInProgress = true),
      fixedClock(Instant.parse("2026-09-22T13:00:00Z"))
    ).run(spark, DataFrameBatchSource[IO](input), manifest).unsafeRunSync()

    assertEquals(publication.outcome, AnalyticsRunOutcome.ErasurePending)
    assertEquals(
      spark.read.format("delta").load(paths.manifests).filter(col("status") === "ERASURE_PENDING").count(),
      1L
    )
  }

  test("Mongo erasure request subject IDs map to HMAC tokens and malformed IDs fail closed") {
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e")
    assertEquals(
      MongoActiveDeletionMarkerSource
        .tokenFor(AnalyticsMongoRecords.ErasureRequest(subjectId.toString), pseudonymizer)
        .toOption,
      SubjectToken.fromHmac(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, subjectId.toString)).toOption
    )
    assertEquals(
      MongoActiveDeletionMarkerSource.tokenFor(AnalyticsMongoRecords.ErasureRequest("not-a-uuid"), pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(
      MongoActiveDeletionMarkerSource.tokenFor(AnalyticsMongoRecords.ErasureRequest("17"), pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
  }

  test("completed deletion markers fail closed when their retention expiry is missing or malformed") {
    val subjectId = java.util.UUID.fromString("d31d0f7b-0abf-4e47-94da-b2f52cb5dd2e").toString
    val now = Instant.parse("2026-09-25T12:00:00Z")
    val missingExpiry = AnalyticsMongoRecords.ErasureRequest(subjectId, state = Some("Complete"))
    val expired = AnalyticsMongoRecords.ErasureRequest(
      subjectId,
      state = Some("Complete"),
      expiresAt = Some(now.minusSeconds(1))
    )

    assertEquals(
      MongoActiveDeletionMarkerSource.activeMarker(missingExpiry, now, pseudonymizer),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(MongoActiveDeletionMarkerSource.activeMarker(expired, now, pseudonymizer), Right(None))
  }

  test("active deletion tokens are excluded before Silver persistence without retaining raw identities") {
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          (
            "hiring.operational-events",
            0,
            1L,
            event(
              "da602f0b-162f-3cbf-ab15-0cfcfc7a7379",
              "APPLICATION_CREATED",
              aggregateId = "49c89f22-c51f-33e7-a79e-8a532c7a402f"
            )
          ),
          (
            "hiring.operational-events",
            0,
            2L,
            event(
              "4d8b6084-f3d1-37b7-acac-66a22a91be02",
              "APPLICATION_CREATED",
              aggregateId = "b0558b6a-c929-3254-9528-526129ef32e9",
              payload = Some(
                "{\"applicationId\":\"b0558b6a-c929-3254-9528-526129ef32e9\",\"candidateId\":\"d625db79-80ee-34f9-8fc1-0fb01896355f\",\"jobId\":\"ac15a52e-59f3-33d6-9344-2a598ffab484\"}"
              )
            )
          )
        )
      )
    )
    val markers = markerFrame(
      Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
    )
    val silver =
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, markers)

    assertEquals(
      silver.select("eventId").collect().map(_.getString(0)).toSet,
      Set("4d8b6084-f3d1-37b7-acac-66a22a91be02")
    )
    assertEquals(
      silver.select("subjectToken").collect().map(_.getString(0)).toSet,
      Set(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "d625db79-80ee-34f9-8fc1-0fb01896355f"))
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
            "cec344a4-e505-3e72-8e87-ed35941dd79a",
            "APPLICATION_CREATED",
            payload = Some(
              """{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"d625db79-80ee-34f9-8fc1-0fb01896355f"}"""
            )
          )
        ),
        (
          "hiring.operational-events",
          0,
          2L,
          event(
            "0086e2ca-ae2c-3261-94d1-2d92fb27a252",
            "SEARCH_PERFORMED",
            aggregateType = "Search",
            aggregateId = "0df3d37e-b815-30ee-98f3-08fb0c065aef",
            payload = Some(
              """{"searchKind":"candidateMatches","results":[{"resultId":"0c3c3b04-71bf-3560-a3a5-50b1169c5876"},{"resultId":"0c3c3b04-71bf-3560-a3a5-50b1169c5876"}]}"""
            ),
            actorId = "0c3c3b04-71bf-3560-a3a5-50b1169c5876"
          )
        )
      )
    )
    val valid = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(source))
    val withTokens = AnalyticsSubjectPrivacy.withSubjectToken(valid.repartition(2), pseudonymizer)
    val tokenRows = withTokens
      .select("eventId", "subjectTokens")
      .collect()
      .map { row =>
        row.getString(0) -> row.getSeq[String](1).toList
      }
      .toMap

    assertEquals(
      tokenRows("cec344a4-e505-3e72-8e87-ed35941dd79a").toSet,
      Set(
        AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"),
        AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "d625db79-80ee-34f9-8fc1-0fb01896355f")
      )
    )
    assertEquals(
      tokenRows("0086e2ca-ae2c-3261-94d1-2d92fb27a252"),
      List(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "0c3c3b04-71bf-3560-a3a5-50b1169c5876"))
    )
    assertEquals(
      silverFrame(
        valid,
        pseudonymizer,
        markerFrame(
          Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"))
        )
      ).count(),
      1L
    )
    assertEquals(
      silverFrame(
        valid,
        pseudonymizer,
        markerFrame(
          Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "0c3c3b04-71bf-3560-a3a5-50b1169c5876"))
        )
      ).select("eventId")
        .collect()
        .map(_.getString(0))
        .toSet,
      Set("cec344a4-e505-3e72-8e87-ed35941dd79a")
    )
  }

  test("subject-token mapping represents missing actor and candidate identities as nulls") {
    val source = records(
      Seq(
        (
          "hiring.operational-events",
          0,
          3L,
          event(
            "57a7c4f3-03b4-3b62-8f64-f3d007693074",
            "APPLICATION_CREATED",
            payload = Some("{\"candidateId\":\"\"}"),
            actorId = ""
          )
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
            "2a6934c7-1841-3fd9-9c62-70106f69dcea",
            "APPLICATION_CREATED",
            payload = Some(
              """{"applicationId":"da28936a-8a76-3972-879c-180dbbce1261","candidateId":"ebba39fc-4f0c-31e6-ac11-0a190dd060ce"}"""
            )
          )
        )
      )
    )
    val valid = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(source))
    val attributed = AnalyticsSubjectPrivacy.withSubjectToken(valid, current).first()
    val tokens = attributed.getAs[scala.collection.Seq[String]]("subjectTokens").toSet
    assertEquals(
      attributed.getAs[String]("subjectToken"),
      AnalyticsTestSubjectPseudonymizer.tokenValue(current, "ebba39fc-4f0c-31e6-ac11-0a190dd060ce")
    )
    assertEquals(
      tokens,
      Set(
        AnalyticsTestSubjectPseudonymizer.tokenValue(current, "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"),
        AnalyticsTestSubjectPseudonymizer.tokenValue(current, "ebba39fc-4f0c-31e6-ac11-0a190dd060ce")
      )
    )
    assertEquals(
      current.matchingTokens("ebba39fc-4f0c-31e6-ac11-0a190dd060ce"),
      Right(
        Vector(
          AnalyticsTestSubjectPseudonymizer.token(current, "ebba39fc-4f0c-31e6-ac11-0a190dd060ce"),
          AnalyticsTestSubjectPseudonymizer.token(
            AnalyticsTestSubjectPseudonymizer.fromSecret(oldSecret),
            "ebba39fc-4f0c-31e6-ac11-0a190dd060ce"
          )
        )
      )
    )
  }

  test("batch filters marked subjects before Bronze and does not persist unattributable malformed quarantine") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-erasure-before-bronze").toUri.toString)
    val batch = newBatch(
      paths,
      pseudonymizer,
      DataFrameDeletionMarkerSource[IO](
        markerFrame(
          Seq(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
        )
      )
    )
    val source = DataFrameBatchSource[IO](
      records(
        Seq(
          ("hiring.operational-events", 0, 0L, event("da602f0b-162f-3cbf-ab15-0cfcfc7a7379", "APPLICATION_CREATED")),
          (
            "hiring.operational-events",
            0,
            1L,
            event(
              "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2",
              "APPLICATION_CREATED",
              aggregateId = "db64e3b2-4dab-3291-b11a-8a6a8ae6d7a9",
              payload = Some(
                """{"applicationId":"db64e3b2-4dab-3291-b11a-8a6a8ae6d7a9","candidateId":"d625db79-80ee-34f9-8fc1-0fb01896355f"}"""
              )
            )
          ),
          ("hiring.operational-events", 0, 2L, "{\"eventId\":\"malformed\"}")
        )
      )
    )
    val manifest =
      validatedManifest(
        "erasure-before-bronze",
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 3L))
      )

    val publication = batch.run(spark, source, manifest).unsafeRunSync()
    val bronze = spark.read.format("delta").load(paths.bronze)
    val quarantine = spark.read.format("delta").load(paths.quarantine)

    assertEquals(publication.outcome, AnalyticsRunOutcome.QualityBlocked)
    assertEquals(bronze.count(), 1L)
    assertEquals(bronze.select("eventId").head().getString(0), "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2")
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
    val paths = TestAnalyticsLakehousePaths.unsafe(lakehouseDirectory("analytics-marker-cleanup").toUri.toString)
    val batch = newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](markers))
    val source = new BoundedOperationalEventSource[IO] {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(AnalyticsError.SourceReadFailure(new IllegalStateException("injected read failure")))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.unit
    }
    val manifest = validatedManifest("cleanup", Vector(TestPartitionOffsetRange.unsafe("topic", 0, 0L, 1L)))

    intercept[AnalyticsError.SourceReadFailure](batch.run(spark, source, manifest).unsafeRunSync())
    assertEquals(markers.storageLevel, StorageLevel.NONE)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
  }

  test("logical subject maintenance preserves retained files across repeated ticks") {
    val root = Files.createTempDirectory("analytics-subject-maintenance")
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toString)
    val at = Instant.parse("2026-09-22T12:00:00Z")
    val deletedToken = pseudonymizer.typedToken("be5aec4f-aa27-3196-bb0b-913181e79e8c").toOption.get
    val retainedToken = pseudonymizer.typedToken("aba639fc-ca10-35ce-9f4b-7754593ce359").toOption.get
    val rawSchema = StructType(
      Seq(
        StructField("rawValue", StringType, nullable = false),
        StructField("subjectTokens", ArrayType(StringType), nullable = true),
        StructField("expiresAt", TimestampType, nullable = false)
      )
    )
    val rawPaths = Vector(paths.bronze, paths.quarantine, paths.lateFacts)
    rawPaths.foreach { path =>
      spark
        .createDataFrame(
          Vector(
            Row("da602f0b-162f-3cbf-ab15-0cfcfc7a7379", Seq(deletedToken.value), Timestamp.from(at.plusSeconds(86400))),
            Row(
              "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2",
              Seq(retainedToken.value),
              Timestamp.from(at.plusSeconds(86400))
            ),
            Row("expired", Seq(retainedToken.value), Timestamp.from(at)),
            Row("57a7c4f3-03b4-3b62-8f64-f3d007693074", Seq.empty[String], Timestamp.from(at.plusSeconds(86400)))
          ).asJava,
          rawSchema
        )
        .coalesce(1)
        .write
        .format("delta")
        .save(path)
    }
    val retainedNames = (1 to 10).map(index => s"retained-$index")
    val input = records((Seq("deleted", "expired") ++ retainedNames).zipWithIndex.map { case (name, index) =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(
          s"maintenance-$name",
          "APPLICATION_CREATED",
          aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$name"),
          payload = Some(s"""{"applicationId":"application-$name","candidateId":"candidate-$name"}""")
        )
      )
    })
    val silver = silverFrame(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(input)),
      pseudonymizer,
      emptyMarkers
    )
      .withColumn("ingestedAt", lit(Timestamp.from(at)))
      .withColumn(
        "expiresAt",
        org.apache.spark.sql.functions
          .when(col("eventId") === AnalyticsOperationalEventFixtures.id("maintenance-expired"), lit(Timestamp.from(at)))
          .otherwise(lit(Timestamp.from(at.plusSeconds(86400))))
      )
    silver.coalesce(1).write.format("delta").save(paths.silver)
    com.example.hiring.analytics.adapter.spark.AnalyticsTableSchemas.createOrValidate(
      spark,
      paths.funnelGold,
      com.example.hiring.analytics.adapter.spark.AnalyticsDeltaCatalog.FunnelGold
    )
    HiringGoldTransforms.wideFunnelDay(silver).toOption.get.write.format("delta").mode("append").save(paths.funnelGold)
    assertEquals(spark.read.format("delta").load(paths.funnelGold).select("created").first().getLong(0), 12L)
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer, fixedClock(at))
    val markers = Vector(deletedToken)
    def tick(): Unit = {
      maintenance.expireStored(at).unsafeRunSync()
      maintenance.purgeMarkedSubjects(markers).unsafeRunSync()
    }
    def currentFiles: Map[String, Set[String]] =
      (rawPaths :+ paths.silver)
        .map(path =>
          path ->
            org.apache.spark.sql.delta.DeltaLog.forTable(spark, path).update().allFiles.collect().map(_.path).toSet
        )
        .toMap

    tick()
    rawPaths.foreach(path =>
      assertEquals(
        spark.read.format("delta").load(path).select("rawValue").collect().map(_.getString(0)).toVector,
        Vector("59fcc6f9-5cc4-37f5-ab1f-f33f516582d2")
      )
    )
    assertEquals(
      spark.read.format("delta").load(paths.silver).select("eventId").collect().map(_.getString(0)).toVector.sorted,
      retainedNames.map(name => AnalyticsOperationalEventFixtures.id(s"maintenance-$name")).toVector.sorted
    )
    assertEquals(spark.read.format("delta").load(paths.funnelGold).select("created").first().getLong(0), 10L)
    val before = currentFiles
    tick()
    assertEquals(currentFiles, before, "a repeated logical purge must preserve retained subject data files")
    assertEquals(maintenance.countMarkedRows(markers).unsafeRunSync(), 0L)
  }

  test("physical erasure verification is tied to captured Delta file paths") {
    val root = Files.createTempDirectory("analytics-erasure-files")
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toString)
    val deletedToken = subjectToken("subject-deleted")
    val retainedToken = subjectToken("subject-retained")
    val schema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    spark
      .createDataFrame(Vector(Row(deletedToken.value), Row(retainedToken.value)).asJava, schema)
      .coalesce(1)
      .write
      .format("delta")
      .save(paths.silver)
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer)

    val affectedFiles = maintenance.captureMarkedFiles(Vector(deletedToken)).unsafeRunSync()
    val dataPaths = affectedFiles.filter(_.endsWith(".parquet"))
    val logPaths = affectedFiles.filter(_.contains("/_delta_log/"))
    assertEquals(dataPaths.size, 1)
    assert(logPaths.nonEmpty, "Silver transaction logs must be captured with affected data")
    assertEquals((dataPaths ++ logPaths).toSet, affectedFiles.toSet)
    val affectedPath = new org.apache.hadoop.fs.Path(dataPaths.head)
    val fileSystem = affectedPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assert(fileSystem.exists(affectedPath))
    intercept[AnalyticsError.PhysicalReclamationUnverified.type](
      maintenance.verifyFilesAbsent(affectedFiles).unsafeRunSync()
    )

    assert(fileSystem.delete(affectedPath, false))
    intercept[AnalyticsError.PhysicalReclamationUnverified.type](
      maintenance.verifyFilesAbsent(affectedFiles).unsafeRunSync()
    )
    logPaths.foreach(path => assert(fileSystem.delete(new org.apache.hadoop.fs.Path(path), false)))
    maintenance.verifyFilesAbsent(affectedFiles).unsafeRunSync()
  }

  test("erasure marker matching shares array, scalar, and unattributed row semantics") {
    val marker = spark.createDataFrame(
      Vector(
        Row("da602f0b-162f-3cbf-ab15-0cfcfc7a7379"),
        Row("da602f0b-162f-3cbf-ab15-0cfcfc7a7379"),
        Row(null)
      ).asJava,
      StructType(Seq(StructField("subjectToken", StringType, nullable = true)))
    )
    val arrayRows = spark.createDataFrame(
      Vector(
        Row("array-hit", Seq("da602f0b-162f-3cbf-ab15-0cfcfc7a7379", "alias")),
        Row("array-miss", Seq("59fcc6f9-5cc4-37f5-ab1f-f33f516582d2")),
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
      Vector(
        Row("scalar-hit", "da602f0b-162f-3cbf-ab15-0cfcfc7a7379"),
        Row("scalar-miss", "59fcc6f9-5cc4-37f5-ab1f-f33f516582d2")
      ).asJava,
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
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toString)
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
    val markerRows = Vector(subjectToken("subject-not-present"))
    val initialMaintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer)
    val firstVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assertEquals(initialMaintenance.countMarkedRows(markerRows).unsafeRunSync(), 0L)

    val initialEvidence =
      initialMaintenance.captureMarkedFiles(markerRows).unsafeRunSync().filter(_.contains("/_delta_log/"))
    assert(initialEvidence.nonEmpty)
    initialMaintenance.applyDeletionMarkers(markerRows).unsafeRunSync()
    val firstRetired = initialMaintenance.checkpointPurgedRawLogs.unsafeRunSync()
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
    val restartedMaintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer)
    val retryEvidence =
      restartedMaintenance.captureMarkedFiles(markerRows).unsafeRunSync().filter(_.contains("/_delta_log/"))
    val retryJson = retryEvidence.filter(_.endsWith(".json"))
    assert(retryJson.nonEmpty, "retry capture should include the current baseline commit")
    restartedMaintenance.applyDeletionMarkers(markerRows).unsafeRunSync()
    val retryRetired = restartedMaintenance.checkpointPurgedRawLogs.unsafeRunSync()
    val retryCheckpointVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assert(retryCheckpointVersion > firstCheckpointVersion, "a restarted retry must establish a later boundary")
    assert(retryJson.forall(retryRetired.contains), "retry baseline JSON must be older than the new clean checkpoint")
  }

  test("legacy raw log paths are checkpointed and removed once they age beyond log retention") {
    val root = Files.createTempDirectory("analytics-legacy-log-cleanup")
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toString)
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
    val markerRows = Vector(subjectToken("subject-deleted"))
    val maintenance = AnalyticsBatchTestSupport.newMaintenance(spark, paths, pseudonymizer)

    val beforePurge = maintenance.captureMarkedFiles(markerRows).unsafeRunSync()
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

    maintenance.applyDeletionMarkers(markerRows).unsafeRunSync()
    val retiredLogs = maintenance.checkpointPurgedRawLogs.unsafeRunSync()
    assert(retiredLogs.contains(legacyJson))
    val baselineVersion = org.apache.spark.sql.delta.DeltaLog.forTable(spark, rawPath).update().version
    val baselineFiles = fileSystem
      .listStatus(new org.apache.hadoop.fs.Path(s"$rawPath/_delta_log"))
      .filter(status => status.getPath.getName.startsWith(f"$baselineVersion%020d"))
    assert(
      baselineFiles.exists(_.getPath.getName.contains("checkpoint")),
      "purge must establish a clean checkpoint baseline"
    )
    val oldTimestamp = System.currentTimeMillis() - 31L * 24L * 60L * 60L * 1000L
    (legacyLogs ++ retiredLogs ++ baselineFiles.map(_.getPath.toString)).distinct.foreach { value =>
      val logPath = new org.apache.hadoop.fs.Path(value)
      fileSystem.setTimes(logPath, oldTimestamp, -1L)
    }
    spark
      .createDataFrame(
        Vector(Row("retained-after-erasure", Timestamp.from(Instant.parse("2026-09-24T12:00:00Z")))).asJava,
        rawSchema
      )
      .write
      .format("delta")
      .mode("append")
      .save(rawPath)
    maintenance.reclaimExpiredFiles.unsafeRunSync()
    maintenance.verifyFilesAbsent(retiredLogs).unsafeRunSync()
    assert(fileSystem.exists(new org.apache.hadoop.fs.Path(legacyJson)) == false)
  }
}
