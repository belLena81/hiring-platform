package com.example.hiring.analytics
import cats.effect.unsafe.implicits.global
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.kafka.*
import AnalyticsBatchTestSupport.newBatch

import com.example.hiring.analytics.adapter.spark.*

import cats.effect.{Deferred, IO}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.file.Files
import java.time.Instant

class AnalyticsEventTransformsSpec extends AnalyticsBatchSpecFixtures {
  test("bronze deduplicates Kafka delivery by topic partition and offset") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("fdc3f116-9f99-3ce6-b429-3eac7ddbba73", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 1L, event("981f76e8-8247-32ae-a9db-598865bf6ba5", "APPLICATION_CREATED"))
      )
    )
    val tokenized = AnalyticsSubjectPrivacy.withSubjectToken(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(source)),
      pseudonymizer
    )
    assertEquals(OperationalEventTransforms.bronze(tokenized).count(), 1L)
  }

  test("AEC-09.1 Bronze keeps tokens, fingerprint and normalized ids but no raw actor, candidate or envelope") {
    val paths = newLakehouse("aec-09-1-")
    val actor = "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"
    val candidate = AnalyticsOperationalEventFixtures.id("aec-09-1-candidate")
    val value = event(
      "610cb653-75fc-3d31-b2c6-3cd8cee7cd4c",
      "APPLICATION_CREATED",
      actorId = actor,
      payload = Some(s"""{"applicationId":"aec-09-1-application","candidateId":"$candidate","jobId":"aec-09-1-job"}""")
    )
    val bronze = ingestIntoBronze(paths, Seq(("hiring.operational-events", 0, 3L, value)))
    assertEquals(bronze.columns.toVector, AnalyticsTableSchemas.bronze.map(_._1))
    val row = bronze.head()
    assertEquals(
      row.getAs[String]("eventFingerprint"),
      java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes("UTF-8")).map("%02x".format(_)).mkString
    )
    assertEquals(row.getAs[String]("applicationId"), AnalyticsOperationalEventFixtures.id("aec-09-1-application"))
    assertEquals(row.getAs[String]("jobId"), AnalyticsOperationalEventFixtures.id("aec-09-1-job"))
    assert(row.getAs[String]("subjectToken").nonEmpty)
    assert(row.getAs[scala.collection.Seq[String]]("subjectTokens").nonEmpty)
    val stored = bronze.toJSON.collect().mkString
    Vector(actor, candidate, "candidateId", "actorId").foreach(sensitive =>
      assert(!stored.contains(sensitive), s"Bronze must not hold $sensitive")
    )
    val storedTokens = row.getAs[scala.collection.Seq[String]]("subjectTokens").toSet
    val eventFingerprintInSilver = silverFrame(
      OperationalEventTransforms.validEvents(
        OperationalEventTransforms.parseKafkaRecords(records(Seq(("hiring.operational-events", 0, 3L, value))))
      ),
      pseudonymizer,
      emptyMarkers
    ).head().getAs[String]("eventFingerprint")
    assertEquals(row.getAs[String]("eventFingerprint"), eventFingerprintInSilver)
    assert(storedTokens.contains(row.getAs[String]("subjectToken")))
  }

  test("AEC-09.3 a stored Bronze table that still has rawValue fails closed without being written") {
    val paths = newLakehouse("aec-09-3-")
    val oldShape = StructType(
      AnalyticsTableSchemas.bronze
        .map(_._1)
        .filterNot(_ == "eventFingerprint")
        .map(name => StructField(name, StringType, nullable = true)) :+ StructField(
        "rawValue",
        StringType,
        nullable = true
      )
    )
    spark.createDataFrame(java.util.Collections.emptyList[Row](), oldShape).write.format("delta").save(paths.bronze)
    val before = org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version
    assert(
      !AnalyticsTableSchemas.matches(spark.read.format("delta").load(paths.bronze).schema, AnalyticsTableSchemas.bronze)
    )
    val value = event("610cb653-75fc-3d31-b2c6-3cd8cee7cd4c", "APPLICATION_CREATED")
    val failure = bronzeStage(paths)
      .persistParsed(
        OperationalEventTransforms.parseKafkaRecords(records(Seq(("hiring.operational-events", 0, 1L, value)))),
        emptyMarkers,
        IO.unit,
        IO.pure(Instant.parse("2026-09-30T10:00:00Z")),
        _ => IO.unit
      )
      .attempt
      .unsafeRunSync()
    assertEquals(
      failure.left.toOption.collect { case AnalyticsError.DeltaSchemaMismatch(path) => path },
      Some(paths.bronze)
    )
    assertEquals(org.apache.spark.sql.delta.DeltaLog.forTable(spark, paths.bronze).update().version, before)
  }

  test("AEC-09.4 a re-delivery at a stored Bronze coordinate with different bytes conflicts by stored fingerprint") {
    val paths = newLakehouse("aec-09-4-")
    val original = event("610cb653-75fc-3d31-b2c6-3cd8cee7cd4c", "APPLICATION_CREATED")
    val changed = event(
      "610cb653-75fc-3d31-b2c6-3cd8cee7cd4c",
      "APPLICATION_CREATED",
      payload = Some("""{"applicationId":"other","candidateId":"x","jobId":"j"}""")
    )
    val stored = ingestIntoBronze(paths, Seq(("hiring.operational-events", 0, 5L, original)))
    def incoming(value: String, offset: Long) =
      OperationalEventTransforms.parseKafkaRecords(records(Seq(("hiring.operational-events", 0, offset, value))))
    val id = incoming(original, 5L).head().getAs[String]("eventId")
    assertEquals(
      SparkStreamingBatchStages
        .coordinateConflicts(incoming(changed, 5L), stored)
        .collect()
        .map(_.getString(0))
        .toVector,
      Vector(id)
    )
    assertEquals(SparkStreamingBatchStages.coordinateConflicts(incoming(original, 5L), stored).count(), 0L)
    assertEquals(SparkStreamingBatchStages.coordinateConflicts(incoming(changed, 6L), stored).count(), 0L)
  }

  test("AEC-09.2 Bronze holds candidate search result tokens and no raw result ids, ranks or scores") {
    val paths = newLakehouse("aec-09-2-")
    val results = (1 to 3).map(index => AnalyticsOperationalEventFixtures.id(s"aec-09-2-candidate-$index"))
    val search = event(
      "81bff5a2-0b1d-3f25-9edb-6d9f2b8bd6a4",
      "SEARCH_PERFORMED",
      aggregateType = "Search",
      aggregateId = "aec-09-2-search",
      payload = Some(
        s"""{"searchKind":"candidateMatches","results":[${results
            .map(id => s"""{"resultId":"$id","score":0.987654}""")
            .mkString(",")}]}"""
      )
    )
    val bronze = ingestIntoBronze(paths, Seq(("hiring.operational-events", 0, 0L, search)))
    val row = bronze.head()
    val tokens = row.getAs[scala.collection.Seq[String]]("subjectTokens")
    assert(tokens.size >= 3, tokens.toString)
    val expected = AnalyticsSubjectPrivacy
      .withSubjectToken(
        OperationalEventTransforms.validEvents(
          OperationalEventTransforms.parseKafkaRecords(records(Seq(("hiring.operational-events", 0, 0L, search))))
        ),
        pseudonymizer
      )
      .head()
      .getAs[scala.collection.Seq[String]]("subjectTokens")
    assertEquals(tokens.toSet, expected.toSet)
    assert(Vector("results", "resultId", "rank", "score", "payload").forall(name => !bronze.columns.contains(name)))
    val stored = bronze.toJSON.collect().mkString
    results.foreach(id => assert(!stored.contains(id), "Bronze must not hold a raw result id"))
    assert(!stored.contains("0.987654"), "Bronze must not hold search scores")
  }

  test("parsed-frame ingestion persists Bronze coordinates without a batch manifest") {
    val lakehouseRoot = Files.createTempDirectory("analytics-streaming-bronze-").toUri.toString
    val paths = AnalyticsLakehousePaths.from(lakehouseRoot).toEither.fold(errors => fail(errors.toString), identity)
    val deltaWriter = new DeltaBatchWriter[IO](paths, sparkExecution)
    val manifestStore = new AnalyticsRunManifestStore[IO] {
      override def persist(
          manifest: AnalyticsRunManifest,
          status: AnalyticsManifestStatus,
          updatedAt: Instant
      ): IO[Unit] =
        fail("parsed-frame ingestion must not create a batch manifest")
    }
    val stage = new AnalyticsBatchIngestionStage[IO](
      paths,
      pseudonymizer,
      sparkExecution,
      manifestStore,
      deltaWriter,
      AnalyticsTestOperationalConfig.operational.retention,
      cats.effect.Clock[IO]
    )
    val payload = event("610cb653-75fc-3d31-b2c6-3cd8cee7cd4c", "APPLICATION_CREATED")
    val parsed = OperationalEventTransforms.parseKafkaRecords(
      records(
        Seq(
          ("hiring.operational-events", 0, 7L, payload),
          ("hiring.operational-events", 0, 7L, payload)
        )
      )
    )
    val observedAt = Instant.parse("2026-09-30T10:00:00Z")

    val result = stage
      .persistParsed(parsed, emptyMarkers, IO.unit, IO.pure(observedAt), _ => IO.unit)
      .unsafeRunSync()

    assertEquals(result.records, 2L)
    assertEquals(result.validRecords, 2L)
    assertEquals(spark.read.format("delta").load(paths.bronze).count(), 1L)
    assertEquals(
      spark.read.format("delta").load(paths.bronze).select("partition", "offset").head().getLong(1),
      7L
    )
  }

  test("HAL-02.1 silver keeps identical event retries and quarantines conflicting event ids") {
    val same = event("fdc3f116-9f99-3ce6-b429-3eac7ddbba73", "APPLICATION_CREATED")
    val conflict = event("fdc3f116-9f99-3ce6-b429-3eac7ddbba73", "APPLICATION_STATUS_CHANGED")
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
      records(
        Seq(("hiring.operational-events", 0, 1L, event("fdc3f116-9f99-3ce6-b429-3eac7ddbba73", "APPLICATION_CREATED")))
      )
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
          ("hiring.operational-events", 0, 1L, """{"eventId":"fdc3f116-9f99-3ce6-b429-3eac7ddbba73"}""")
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
          (
            "hiring.operational-events",
            0,
            3L,
            event("b6a8a57b-5dc7-37dd-ba4d-33531056eab7", "APPLICATION_CREATED", aggregateId = "")
          ),
          (
            "hiring.operational-events",
            0,
            4L,
            event("fb6ec5db-c5ce-325a-876d-5eb7d3bd7cfd", "APPLICATION_CREATED", aggregateId = "   ")
          ),
          (
            "hiring.operational-events",
            0,
            5L,
            """{"eventId":"\t","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"Application","aggregateId":"6e8a2abc-74b9-35ea-aebb-6b275bbdfdf7","actorId":"d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686","payload":{}}"""
          ),
          (
            "hiring.operational-events",
            0,
            6L,
            """{"eventId":"f208fd7c-ebf1-3cd6-a38e-6ce1c9e25278","eventType":"APPLICATION_CREATED","occurredAt":"2026-09-22T10:00:00Z","aggregateType":"Application","aggregateId":"\n","actorId":"d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686","payload":{}}"""
          )
        )
      )
    )
    assertEquals(OperationalEventTransforms.validEvents(parsed).count(), 0L)
    assertEquals(OperationalEventTransforms.malformedEvents(parsed).count(), 6L)
  }

  test("HAL-09.1 funnel excludes the duplicate candidate hired event and suppresses groups below ten") {
    val events = (1 to 10).map(index =>
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
    ) ++ Seq(("hiring.operational-events", 0, 11L, event("b88def9c-77b1-3622-8c71-7e68ab9f49bf", "CANDIDATE_HIRED")))
    val silver = silverFrame(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(events))),
      pseudonymizer,
      emptyMarkers
    )
    val result =
      HiringGoldTransforms
        .funnelActivity(silver)
        .toOption
        .get
        .select("eventType", "contributingApplications")
        .collect()
        .toSeq
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
          AnalyticsOperationalEventFixtures.id(s"created-$index"),
          "APPLICATION_CREATED",
          aggregateId = AnalyticsOperationalEventFixtures.id(s"application-$index"),
          payload = Some(s"""{"applicationId":"application-$index","candidateId":"candidate-$index"}""")
        )
      )
    )
    val oneAccepted = (
      "hiring.operational-events",
      0,
      11L,
      event(
        "547c73dd-1fd7-33e3-a346-9380e2c349ce",
        "APPLICATION_STATUS_CHANGED",
        aggregateId = "a053804b-8231-3cb4-8074-834a69aaccab",
        payload = Some(
          """{"applicationId":"a053804b-8231-3cb4-8074-834a69aaccab","candidateId":"19b46da1-bee5-3018-b5f7-abe9a9fdfb29","newStatus":"Accepted"}"""
        )
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(created :+ oneAccepted))
    val silver =
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.wideFunnelDay(silver).toOption.get.count(), 0L)
  }

  test("time-to-hire suppresses eligible and excluded counts when either is below ten") {
    val lifecycle = (1 to 10).flatMap { index =>
      val applicationId = AnalyticsOperationalEventFixtures.id(s"application-$index")
      val candidateId = AnalyticsOperationalEventFixtures.id(s"candidate-$index")
      Seq(
        (
          "hiring.operational-events",
          0,
          index.toLong * 2,
          event(
            AnalyticsOperationalEventFixtures.id(s"created-$index"),
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
            AnalyticsOperationalEventFixtures.id(s"hired-$index"),
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
        "c2413b3d-6b01-32ee-a6c1-ee1a90828706",
        "APPLICATION_CREATED",
        aggregateId = "3d89000a-69ef-382c-b534-bc125b2cf007",
        payload = Some(
          """{"applicationId":"3d89000a-69ef-382c-b534-bc125b2cf007","candidateId":"99aa6ff6-e03c-3532-98b4-80c5b0b0d977"}"""
        )
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(lifecycle))
    val silver =
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(
      HiringGoldTransforms.timeToHireAction[IO](stageTimesOf(silver), sparkExecution).unsafeRunSync().count(),
      0L
    )
  }

  test("ten eligible applications from one subject do not satisfy time-to-hire suppression") {
    val lifecycle = (1 to 10).flatMap { index =>
      val applicationId = AnalyticsOperationalEventFixtures.id(s"application-$index")
      Seq(
        (
          "hiring.operational-events",
          0,
          index.toLong * 2,
          event(
            AnalyticsOperationalEventFixtures.id(s"created-$index"),
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
            AnalyticsOperationalEventFixtures.id(s"hired-$index"),
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

    assertEquals(
      HiringGoldTransforms.timeToHireAction[IO](stageTimesOf(silver), sparkExecution).unsafeRunSync().count(),
      0L
    )
  }

  test("skill posting activity normalizes only created job skills and applies k anonymity") {
    val payload = """{"job":{"skills":[" Scala ","scala","  "]}}"""
    val created = (1 to 10).map(index =>
      (
        "hiring.operational-events",
        0,
        index.toLong,
        event(
          AnalyticsOperationalEventFixtures.id(s"job-$index"),
          "JOB_CREATED",
          "Job",
          AnalyticsOperationalEventFixtures.id(s"job-$index"),
          Some(payload),
          actorId = AnalyticsOperationalEventFixtures.id(s"recruiter-$index")
        )
      )
    )
    val update =
      (
        "hiring.operational-events",
        0,
        20L,
        event(
          "bd71f607-f8d6-3452-9cfd-bcdb312263e0",
          "JOB_UPDATED",
          "Job",
          "8e2744bf-6b88-339b-97ea-fe3290044a7b",
          Some(payload)
        )
      )
    val silver = silverFrame(
      OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(records(created :+ update))),
      pseudonymizer,
      emptyMarkers
    )
    val result = HiringGoldTransforms.skillPostingActivity(silver).toOption.get.collect()
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
        event(
          AnalyticsOperationalEventFixtures.id(s"job-$index"),
          "JOB_CREATED",
          "Job",
          AnalyticsOperationalEventFixtures.id(s"job-$index"),
          Some(payload)
        )
      )
    )
    val parsed = OperationalEventTransforms.parseKafkaRecords(records(created))
    val silver =
      silverFrame(OperationalEventTransforms.validEvents(parsed), pseudonymizer, emptyMarkers)

    assertEquals(HiringGoldTransforms.skillPostingActivity(silver).toOption.get.count(), 0L)
  }

  test("offset manifests reject impossible or duplicated partition ranges") {
    assertEquals(AnalyticsTestOperationalConfig.operational.retention.bronzeDays, 7)
    assertEquals(AnalyticsTestOperationalConfig.operational.retention.silverDays, 30)
    assert(TestPartitionOffsetRange.from("topic", 0, 5L, 4L).isInvalid)
    val errors = TestPartitionOffsetRange
      .manifestOf(
        "run-1",
        Vector(TestPartitionOffsetRange.unsafe("topic", 0, 0L, 1L), TestPartitionOffsetRange.unsafe("topic", 0, 1L, 2L))
      )
      .toEither
      .swap
      .toOption
      .get
      .toNonEmptyList
      .toList
    assert(errors.contains("each topic partition may occur only once"))
    assertEquals(RunId.from(""), Left("run id must be non-empty"))
    val topicErrors = TestPartitionOffsetRange.from("", 0, 0L, 1L).toEither.swap.toOption.get
    assert(topicErrors.toNonEmptyList.toList.contains("topic must be non-empty"))
    val numericErrors = TestPartitionOffsetRange.from("topic", -1, -1L, -2L).toEither.swap.toOption.get
    assert(numericErrors.toNonEmptyList.toList.contains("partition must be non-negative"))
    assert(numericErrors.toNonEmptyList.toList.contains("start offset must be non-negative"))
    assert(numericErrors.toNonEmptyList.toList.contains("end offset must be non-negative"))
  }

  test("Kafka offset JSON escapes topic strings and preserves the configured shape") {
    val ranges = Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 3L, 8L))
    assertEquals(KafkaOffsetRangeSource.assignJson(ranges), "{\"hiring.operational-events\":[0]}")
    assertEquals(KafkaOffsetRangeSource.offsetJson(ranges, _.startOffset), "{\"hiring.operational-events\":{\"0\":3}}")
    assertEquals(
      KafkaOffsetRangeSource.offsetJson(ranges, _.endOffsetExclusive),
      "{\"hiring.operational-events\":{\"0\":8}}"
    )
    val escaped = Vector(TestPartitionOffsetRange.unsafe("topic\"\\\n\u0001", 2, 3L, 8L))
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
    val client = KafkaClientProperties.clientProperties(authenticated).toOption.get
    val spark = KafkaClientProperties.sparkOptions(authenticated).toOption.get
    assertEquals(client.get("security.protocol"), Some("SASL_SSL"))
    assertEquals(client.get("sasl.mechanism"), Some("SCRAM-SHA-512"))
    assert(client.getOrElse("sasl.jaas.config", "").contains("username=\"analytics_reader\""))
    assertEquals(spark.get("kafka.security.protocol"), Some("SASL_SSL"))
    assertEquals(spark.get("kafka.group.id"), Some("hiring-analytics-batch"))
    assertEquals(spark.get("kafka.isolation.level"), Some("read_committed"))
    assertEquals(KafkaConnection.validate(KafkaConnection("kafka:9092", Some("reader"), None)).isInvalid, true)
  }

  test("Kafka SASL_PLAINTEXT requires explicit opt-in and reaches both client property sets") {
    val unapproved = KafkaConnection(
      "kafka:9092",
      Some("reader"),
      Some("secret"),
      KafkaSecurityProtocol.SaslPlaintext
    )
    assert(KafkaConnection.validate(unapproved).isInvalid)
    assert(KafkaClientProperties.clientProperties(unapproved).isLeft)
    assert(KafkaClientProperties.sparkOptions(unapproved).isLeft)

    val approved = unapproved.copy(allowPlaintext = true)
    assert(KafkaConnection.validate(approved).isValid)
    assertEquals(
      KafkaClientProperties.clientProperties(approved).toOption.get.get("security.protocol"),
      Some("SASL_PLAINTEXT")
    )
    assertEquals(
      KafkaClientProperties.sparkOptions(approved).toOption.get.get("kafka.security.protocol"),
      Some("SASL_PLAINTEXT")
    )
  }

  test("Kafka rejects an unsupported security protocol") {
    val invalid = KafkaConnection(
      "broker.example.com:9092",
      Some("reader"),
      Some("secret"),
      KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = true
    )
    assert(KafkaConnection.validate(invalid).isInvalid)
    assert(KafkaClientProperties.clientProperties(invalid).isLeft)
  }

  test("data frame source honors the manifest offset boundary") {
    val source = records(
      Seq(
        ("hiring.operational-events", 0, 1L, event("13b5bfe9-6f3e-3fe4-91c9-f66f4a582adf", "APPLICATION_CREATED")),
        ("hiring.operational-events", 0, 2L, event("c68271a6-3ddb-3431-8307-beb7d2918275", "APPLICATION_CREATED"))
      )
    )
    val manifest =
      validatedManifest("run-1", Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 1L, 2L)))
    assertEquals(DataFrameBatchSource[IO](source).read(spark, manifest).unsafeRunSync().count(), 1L)
  }

  test("data frame source reports missing Kafka columns as a typed schema error") {
    val source = records(Seq.empty).drop("partition", "offset")
    val manifest =
      validatedManifest(
        "run-missing-columns",
        Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 0, 0L, 1L))
      )
    val failure = intercept[AnalyticsError.InvalidSourceSchema] {
      DataFrameBatchSource[IO](source).read(spark, manifest).unsafeRunSync()
    }
    assertEquals(failure.missing, Vector("offset", "partition"))
  }

  test("empty requested ranges fail before reading or writing a manifest") {
    val paths =
      TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-empty-offset-range").toUri.toString)
    val source = DataFrameBatchSource[IO](records(Seq.empty))
    val manifest = validatedManifest("empty-offset-range", Vector(TestPartitionOffsetRange.unsafe("topic", 0, 4L, 4L)))
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
        ("topic", 0, 1L, event("8b04d5e3-775d-398e-b845-5efc5ca404d5", "APPLICATION_CREATED")),
        ("topic", 0, 3L, event("dd5c8bf5-1558-3fcb-a500-7071908e9524", "APPLICATION_CREATED"))
      )
    )
    val scenarios = Vector(
      ("missing-partition", TestPartitionOffsetRange.unsafe("topic", 1, 5L, 6L), 1L, 0L),
      ("missing-interior", TestPartitionOffsetRange.unsafe("topic", 0, 1L, 4L), 3L, 2L)
    )
    scenarios.foreach { case (runId, range, requested, observed) =>
      val paths = TestAnalyticsLakehousePaths.unsafe(Files.createTempDirectory(s"analytics-$runId").toUri.toString)
      val failure = intercept[AnalyticsError.MissingOffsetRange] {
        newBatch(paths, pseudonymizer, DataFrameDeletionMarkerSource[IO](emptyMarkers))
          .run(spark, DataFrameBatchSource[IO](input), validatedManifest(runId, Vector(range)))
          .unsafeRunSync()
      }
      assertEquals(
        failure,
        AnalyticsError.MissingOffsetRange(AnalyticsTopic.unwrap(range.topic), range.partition, requested, observed)
      )
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests))
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze))
    }
  }

  test("broker retention bounds distinguish expired from not yet available offsets") {
    val range = TestPartitionOffsetRange.unsafe("topic", 0, 3L, 8L)
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
    val token = AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29")
    assertEquals(
      token,
      AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "19b46da1-bee5-3018-b5f7-abe9a9fdfb29")
    )
    assertNotEquals(
      token,
      AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, "d625db79-80ee-34f9-8fc1-0fb01896355f")
    )
    assert(!token.contains("19b46da1-bee5-3018-b5f7-abe9a9fdfb29"))
    assert(token.startsWith("hmac-v1_"))
    assertEquals(pseudonymizer.typedToken(null), Left("subject id must be non-empty"))
    assertEquals(pseudonymizer.typedToken(""), Left("subject id must be non-empty"))
  }

  test("pseudonymizer tokens remain deterministic across serialization and concurrent calls") {
    val subjectIds = Vector.tabulate(128)(index => AnalyticsOperationalEventFixtures.id(s"candidate-$index"))
    val expected = subjectIds.map(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, _))
    val bytes = new java.io.ByteArrayOutputStream()
    val output = new java.io.ObjectOutputStream(bytes)
    output.writeObject(pseudonymizer)
    output.close()
    val input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val serializedCopy = input.readObject().asInstanceOf[SubjectPseudonymizer]
    input.close()
    assertEquals(subjectIds.map(AnalyticsTestSubjectPseudonymizer.tokenValue(serializedCopy, _)), expected)

    val executor = java.util.concurrent.Executors.newFixedThreadPool(8)
    try {
      val actual = subjectIds.zip(expected).map { case (subjectId, token) =>
        executor.submit(new java.util.concurrent.Callable[String] {
          override def call(): String = AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, subjectId)
        }) -> token
      }
      assertEquals(actual.map(_._1.get()), expected)
    } finally { val _ = executor.shutdownNow() }
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
    assert(SubjectPseudonymizer.validateFromBase64(encoded, "hmac-v1", None, None).isInvalid)
  }

  test("lakehouse operation boundary preserves typed errors and adapts thrown failures") {
    val sparkExecution = com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
      .forTests[IO](scala.concurrent.ExecutionContext.parasitic)
    val boundary = new LakehouseOperation[IO](sparkExecution)
    val expected = AnalyticsError.InvalidConfiguration("expected lakehouse validation failure")
    val cause = new IllegalStateException("spark failure")

    assertEquals(boundary.either[String](Left(expected)).attempt.unsafeRunSync(), Left(expected))
    val failure = boundary[Unit](throw cause).attempt.unsafeRunSync().swap.toOption
    val ioFailure = boundary.effect(IO.raiseError[Unit](cause)).attempt.unsafeRunSync().swap.toOption
    val adapted = failure.exists {
      case AnalyticsError.LakehouseFailure(underlying) => underlying eq cause
      case _                                           => false
    }
    assert(adapted, "expected the Spark failure to become LakehouseFailure")
    assertEquals(ioFailure, failure)
  }
}
