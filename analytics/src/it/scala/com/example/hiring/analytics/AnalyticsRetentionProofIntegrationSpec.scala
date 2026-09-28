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

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.unsafe.implicits.global
import cats.effect.{Clock, IO}
import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.reactivestreams.client.{
  MongoClient as ReactiveMongoClient,
  MongoClients as ReactiveMongoClients,
  MongoDatabase as ReactiveMongoDatabase
}
import com.mongodb.client.model.{Filters, ReplaceOptions, UpdateOptions, Updates}
import io.delta.tables.DeltaTable
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.{Date, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Opt-in persistent Compose evidence for actual Kafka and Delta erasure-retention horizons. */
final class AnalyticsRetentionProofIntegrationSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 15.minutes
  private val enabled = sys.env.get("HIRING_ANALYTICS_RETENTION_PROOF_ENABLED").contains("true")
  private val mode = sys.env.getOrElse("HIRING_ANALYTICS_RETENTION_PROOF_MODE", "")
  private val nonce = sys.env.getOrElse("HIRING_ANALYTICS_RETENTION_PROOF_NONCE", "")
  private val subjectId = sys.env.getOrElse("HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID", "")
  private val databaseName = sys.env.getOrElse("MONGODB_DATABASE", "")
  private val topic = sys.env.getOrElse("ANALYTICS_TOPIC", "")
  private val bootstrap = sys.env.getOrElse("ANALYTICS_BOOTSTRAP_SERVERS", "kafka:9092")
  private val lakehouseRoot = sys.env.getOrElse("ANALYTICS_LAKEHOUSE_ROOT", "")
  private val retentionMs = 7L * 24L * 60L * 60L * 1000L
  private val segmentBytes = 16 * 1024
  private val producerId = "hiring-publisher-retention-" + nonce
  private def runId(rangeEnd: Long): String = "retention-proof-" + nonce + "-" + rangeEnd
  private val proofCollection = "analytics_retention_proof"
  private val evidenceFile = "/var/lib/hiring-analytics/proof/retention-evidence.json"

  private def required(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(fail(name + " is required for retention proof"))

  private def connection(user: String, password: String): KafkaConnection =
    KafkaConnection(bootstrap, Some(user), Some(password))

  private def properties(user: String, password: String, transactionalId: Option[String] = None): Properties = {
    val result = new Properties()
    result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    result.put(ProducerConfig.ACKS_CONFIG, "all")
    result.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    result.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576")
    transactionalId.foreach(result.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, _))
    result.put("security.protocol", "SASL_PLAINTEXT")
    result.put("sasl.mechanism", "PLAIN")
    result.put(
      "sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + user + "\" password=\"" + password + "\";"
    )
    result
  }

  private def adminProperties: Properties = {
    val result = new Properties()
    result.put("bootstrap.servers", bootstrap)
    result.put("security.protocol", "SASL_PLAINTEXT")
    result.put("sasl.mechanism", "PLAIN")
    result.put(
      "sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"broker\" password=\"" +
        required("KAFKA_BROKER_PASSWORD") + "\";"
    )
    result
  }

  private def configureTopic(): Unit = {
    val admin = Admin.create(adminProperties)
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val configs = Vector(
        new ConfigEntry("retention.ms", retentionMs.toString),
        new ConfigEntry("retention.bytes", "-1"),
        new ConfigEntry("cleanup.policy", "delete"),
        new ConfigEntry("segment.bytes", segmentBytes.toString)
      )
      val changes = configs.map(new AlterConfigOp(_, AlterConfigOp.OpType.SET)).asJava
      admin
        .incrementalAlterConfigs(Map(resource -> changes).asJava)
        .all()
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
      val effective = admin
        .describeConfigs(List(resource).asJava)
        .all()
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
        .get(resource)
        .entries()
        .asScala
        .map(entry => entry.name() -> entry.value())
        .toMap
      assertEquals(effective.get("retention.ms"), Some(retentionMs.toString))
      assertEquals(effective.get("segment.bytes"), Some(segmentBytes.toString))
      assertEquals(effective.get("cleanup.policy"), Some("delete"))
    } finally admin.close()
  }

  private def event(eventId: String, actor: String, padding: String, malformed: Boolean = false): String = {
    val eventKey = if (malformed) " " else eventId
    val at = Instant.now().toString
    "{\"eventId\":\"" + eventKey + "\",\"eventType\":\"JOB_CREATED\",\"occurredAt\":\"" + at +
      "\",\"aggregateType\":\"Job\",\"aggregateId\":\"job-" + eventId + "\",\"actorId\":\"" + actor +
      "\",\"payload\":{\"job\":{\"skills\":[\"Scala\"]},\"padding\":\"" + padding + "\"}}"
  }

  private def sparkSession(): SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("AnalyticsRetentionProofIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

  private def initializeDatabase(db: MongoDatabase): Unit = {
    val collections = Vector(
      "analytics_erasure_requests",
      "analytics_erasure_completions",
      "analytics_erasure_delta_files",
      "analytics_worker_heartbeats",
      "analytics_report_snapshots",
      "analytics_report_control",
      "analytics_report_runs",
      "event_outbox",
      "outbox_subject_fences",
      "users",
      "hiring_migration_ledger",
      proofCollection
    )
    val existing = db.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet
    collections.filterNot(existing).foreach(db.createCollection(_))
    db.getCollection("hiring_migration_ledger")
      .updateOne(
        Filters.eq("_id", "003_event_outbox_subject_references"),
        Updates.setOnInsert("state", "Complete"),
        new UpdateOptions().upsert(true)
      )
    db.getCollection("analytics_report_control")
      .updateOne(
        Filters.eq("_id", "analytics-report"),
        Updates.combine(
          Updates.setOnInsert("generation", 1L),
          Updates.setOnInsert("state", "Hidden"),
          Updates.setOnInsert("nextRevision", 0L),
          Updates.setOnInsert("lastPublishedRevision", 0L),
          Updates.setOnInsert("lastRunId", "")
        ),
        new UpdateOptions().upsert(true)
      )
    db.getCollection(proofCollection)
      .updateOne(
        Filters.eq("_id", nonce),
        Updates.setOnInsert("stage", "Preparing"),
        new UpdateOptions().upsert(true)
      )
  }

  private def prepare(db: MongoDatabase, reactiveDb: ReactiveMongoDatabase): Unit = {
    assert(enabled, "assertion failed")
    assertEquals(mode, "prepare")
    assert(nonce.matches("[a-f0-9]{16}"), "assertion failed")
    assert(UUID.fromString(subjectId).toString == subjectId, "assertion failed")
    assert(databaseName.matches("hiring_retention_[a-f0-9]{16}"), "assertion failed")
    assert(topic.matches("hiring\\.retention\\.[a-f0-9]{16}"), "assertion failed")
    assert(lakehouseRoot.startsWith("file:///var/lib/hiring-analytics/lakehouse"), "assertion failed")
    initializeDatabase(db)
    val fixture = db.getCollection(proofCollection).find(Filters.eq("_id", nonce)).first()
    if (fixture.getString("stage") == "Prepared") {
      println("RETENTION_PROOF_PREPARED_ALREADY topic=" + topic + " request=" + subjectId)
      return
    }
    configureTopic()

    val producer = new KafkaProducer[String, String](
      properties("hiring_publisher_v2", required("KAFKA_PUBLISHER_V2_PASSWORD"), Some(producerId))
    )
    val metadata = try {
      producer.initTransactions()
      producer.beginTransaction()
      val valid = (0 until 24).map { _ =>
        new ProducerRecord(topic, subjectId, event(UUID.randomUUID().toString, subjectId, "x" * 12000))
      }
      val malformed =
        new ProducerRecord(topic, subjectId, event(UUID.randomUUID().toString, subjectId, "", malformed = true))
      val sent =
        (valid :+ malformed).map(record => producer.send(record).get(60, java.util.concurrent.TimeUnit.SECONDS))
      producer.commitTransaction()
      sent.toVector
    } finally producer.close()
    val rangeEnd = metadata.map(_.offset()).max + 1L

    val spark = sparkSession()
    try {
      val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromBase64(required("HIRING_ANALYTICS_HMAC_SECRET_BASE64"))
      val paths = AnalyticsLakehousePaths.unsafe(lakehouseRoot)
      val batch = AnalyticsBatchTestSupport.newBatch(
        paths,
        pseudonymizer,
        new MongoActiveDeletionMarkerSource[IO](reactiveDb, pseudonymizer)
      )
      val manifest = AnalyticsRunManifest
        .validated(
          runId(rangeEnd),
          Vector(PartitionOffsetRange.unsafe(topic, 0, 0L, rangeEnd))
        )
        .toEither
        .fold(errors => fail(errors.toString), identity)
      val result = batch
        .run(
          spark,
          new KafkaOffsetRangeSource(connection("analytics_reader", required("KAFKA_READER_PASSWORD"))),
          manifest
        )
        .unsafeRunSync()
      assertEquals(result.outcome, AnalyticsRunOutcome.QualityBlocked)
      assert(result.validRecords >= 24L, "assertion failed")
      assert(result.quarantinedRecords >= 1L, "assertion failed")

      val tick = 96.toChar.toString
      spark.sql(
        "ALTER TABLE delta." + tick + paths.bronze + tick +
          " SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')"
      )
      spark.read.format("delta").load(paths.bronze).limit(1).write.format("delta").mode("append").save(paths.bronze)
      spark.sql(
        "ALTER TABLE delta." + tick + paths.bronze + tick +
          " SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')"
      )
      assertLegacyRawValueStats(spark, paths.bronze)
      assertEquals(
        DeltaTable
          .forPath(spark, paths.bronze)
          .detail()
          .select("properties")
          .head()
          .getAs[scala.collection.Map[String, String]]("properties")
          .get("delta.dataSkippingNumIndexedCols"),
        Some("0")
      )
      assert(DeltaTable.isDeltaTable(spark, paths.quarantine), "assertion failed")
      assert(spark.read.format("delta").load(paths.bronze).count() > 0L, "assertion failed")
      assert(spark.read.format("delta").load(paths.quarantine).count() > 0L, "assertion failed")
    } finally spark.stop()

    val now = Instant.now()
    db.getCollection("analytics_erasure_requests")
      .replaceOne(
        Filters.eq("_id", subjectId),
        new Document("_id", subjectId)
          .append("state", "Pending")
          .append("requestedAt", Date.from(now))
          .append("fencingVersion", 1)
          .append("transactionalIds", java.util.List.of(producerId)),
        new ReplaceOptions().upsert(true)
      )
    db.getCollection("users")
      .replaceOne(
        Filters.eq("_id", subjectId),
        new Document("_id", subjectId).append("accountStatus", "Deleted"),
        new ReplaceOptions().upsert(true)
      )
    db.getCollection("outbox_subject_fences")
      .replaceOne(
        Filters.eq("_id", subjectId),
        new Document("_id", subjectId)
          .append("deleted", true)
          .append("transactionalIds", java.util.List.of(producerId)),
        new ReplaceOptions().upsert(true)
      )
    db.getCollection("event_outbox").deleteMany(Filters.in("subjectIds", subjectId))
    db.getCollection("event_outbox")
      .insertOne(
        new Document("_id", "retention-proof-outbox-" + nonce)
          .append("state", "Retryable")
          .append("subjectIds", java.util.List.of(subjectId))
          .append("subjectRefsVersion", 1)
      )
    persistEvidenceFile(rangeEnd)
    db.getCollection(proofCollection)
      .updateOne(
        Filters.eq("_id", nonce),
        Updates.combine(
          Updates.set("stage", "Prepared"),
          Updates.set("runId", runId(rangeEnd)),
          Updates.set("subjectId", subjectId),
          Updates.set("topic", topic),
          Updates.set("retentionMs", retentionMs),
          Updates.set("segmentBytes", segmentBytes),
          Updates.set("seedEndOffsetExclusive", rangeEnd),
          Updates.set("createdAt", Date.from(now))
        )
      )
    println(
      "RETENTION_PROOF_PREPARED topic=" + topic + " database=" + databaseName + " request=" + subjectId +
        " seedEndOffsetExclusive=" + rangeEnd + " validEvents=24 quarantinedEvents=1"
    )
  }

  private def assertLegacyRawValueStats(spark: SparkSession, tablePath: String): Unit = {
    val logPath = new org.apache.hadoop.fs.Path(tablePath + "/_delta_log")
    val fs = logPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files = fs.listStatus(logPath).filter(_.getPath.getName.endsWith(".json"))
    val mapper = new ObjectMapper()
    val found = files.exists { file =>
      val input = fs.open(file.getPath)
      val source = scala.io.Source.fromInputStream(input, "UTF-8")
      try
        source.getLines().exists { line =>
          val root = mapper.readTree(line)
          if (!root.has("add") || !root.get("add").has("stats")) false
          else {
            val stats = mapper.readTree(root.get("add").get("stats").asText())
            stats.path("minValues").has("rawValue") || stats.path("maxValues").has("rawValue")
          }
        }
      finally source.close()
    }
    assert(found, "legacy Delta fixture must contain rawValue statistics in an AddFile action")
  }

  private def appendRetentionTail(
      reactiveClient: ReactiveMongoClient,
      reactiveDb: ReactiveMongoDatabase,
      db: MongoDatabase
  ): Unit = {
    assertEquals(mode, "append-retention-tail")
    val staged = db.getCollection(proofCollection).find(Filters.eq("_id", nonce)).first()
    if (staged != null && staged.get("rolloverTailOffset", classOf[java.lang.Long]) != null) {
      println(
        "RETENTION_PROOF_TAIL_ALREADY_STAGED topic=" + topic + " barrierOffsetExclusive=" +
          staged.getLong("barrierOffsetExclusive") + " firstTailOffset=" + staged.getLong("tailOffset") +
          " rolloverTailOffset=" + staged.getLong("rolloverTailOffset")
      )
      return
    }
    val request = db.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first()
    assert(request != null, "assertion failed")
    assertEquals(request.getString("phase"), ErasurePhase.DeltaPurged.toString)
    val barrier = new MongoAnalyticsErasureWorkerStore[IO](reactiveClient, reactiveDb)
      .readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(subjectId))
      .unsafeRunSync()
      .getOrElse(fail("worker must persist its Kafka barrier"))
    assertEquals(barrier.topic, topic)
    assertEquals(barrier.partitions.size, 1)
    val barrierOffset = barrier.partitions.head.endOffsetExclusive
    val unrelated = UUID.randomUUID().toString
    val producer = new KafkaProducer[String, String](
      properties(
        "hiring_publisher_v2",
        required("KAFKA_PUBLISHER_V2_PASSWORD"),
        Some("hiring-publisher-retention-tail-" + nonce)
      )
    )
    val sent = try {
      producer.initTransactions()
      val payloads = Vector.fill(2)(event(UUID.randomUUID().toString, unrelated, "t" * (segmentBytes / 2)))
      assert(payloads.forall(_.getBytes(StandardCharsets.UTF_8).length < segmentBytes - 1024), "assertion failed")
      payloads.map { payload =>
        producer.beginTransaction()
        val result = producer
          .send(new ProducerRecord(topic, unrelated, payload))
          .get(60, java.util.concurrent.TimeUnit.SECONDS)
        producer.commitTransaction()
        result
      }
    } finally producer.close()
    assert(
      sent.head.offset() >= barrierOffset,
      "post-barrier tail must begin at or after the exclusive barrier, allowing aborted-offset gaps"
    )
    assert(sent.last.offset() > sent.head.offset(), "assertion failed")
    db.getCollection(proofCollection)
      .updateOne(
        Filters.eq("_id", nonce),
        Updates.combine(
          Updates.set("barrierOffsetExclusive", barrierOffset),
          Updates.set("tailOffset", sent.head.offset()),
          Updates.set("rolloverTailOffset", sent.last.offset()),
          Updates.set("tailAppendedAt", Date.from(Instant.now()))
        )
      )
    println(
      "RETENTION_PROOF_TAIL topic=" + topic + " barrierOffsetExclusive=" + barrierOffset +
        " firstTailOffset=" + sent.head.offset() + " rolloverTailOffset=" + sent.last.offset()
    )
  }

  private def inspect(
      reactiveClient: ReactiveMongoClient,
      reactiveDb: ReactiveMongoDatabase,
      db: MongoDatabase,
      requireKafka: Boolean,
      requireAll: Boolean,
      finalCheck: Boolean
  ): Unit = {
    val request = db.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first()
    assert(request != null, "assertion failed")
    val purgedAt =
      Option(request.getDate("deltaPurgedAt")).map(_.toInstant).getOrElse(fail("DeltaPurged timestamp is missing"))
    val barrier = new MongoAnalyticsErasureWorkerStore[IO](reactiveClient, reactiveDb)
      .readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(subjectId))
      .unsafeRunSync()
      .getOrElse(fail("Kafka barrier is missing"))
    val kafkaPassed = KafkaRetentionAdapter
      .liveRetention[IO]
      .retentionPassed(
        connection("analytics_reader", required("KAFKA_READER_PASSWORD")),
        barrier
      )
      .unsafeRunSync()
    val deltaDeadline = purgedAt.plusSeconds(AnalyticsRetention.DeltaLogRetentionDays.toLong * 86400L)
    val deltaPassed = !Instant.now().isBefore(deltaDeadline)
    val evidence = db
      .getCollection("analytics_erasure_delta_files")
      .find(Filters.eq("requestId", subjectId))
      .into(new java.util.ArrayList[Document]())
      .asScala
      .toVector
      .map(_.getString("filePath"))
    val dataFiles = evidence.filterNot(_.contains("/_delta_log/"))
    val logFiles = evidence.filter(_.contains("/_delta_log/"))
    assert(dataFiles.nonEmpty, "worker must persist a pre-purge Delta data-file path")
    assert(logFiles.nonEmpty, "worker must persist a pre-purge Delta log-file path")
    println(
      "RETENTION_PROOF_STATUS state=" + request.getString("state") + " phase=" + request.getString("phase") +
        " deltaPurgedAt=" + purgedAt + " deltaDeadline=" + deltaDeadline + " kafkaBarrierPassed=" + kafkaPassed +
        " dataFileEvidence=" + dataFiles.size + " logFileEvidence=" + logFiles.size
    )
    if (requireKafka) assert(kafkaPassed, "Kafka earliest offsets have not passed the persisted barrier")
    if (requireAll) {
      assert(kafkaPassed, "assertion failed")
      assert(deltaPassed, "the full 30-day Delta log horizon has not elapsed")
      if (request.getString("state") == "Processing")
        assertEquals(request.getString("phase"), ErasurePhase.DeltaPurged.toString)
      else {
        assertEquals(request.getString("state"), "Complete")
        assertEquals(request.getString("phase"), ErasurePhase.ReportPublished.toString)
      }
    }
    if (finalCheck) {
      assert(kafkaPassed && deltaPassed, "assertion failed")
      assertEquals(request.getString("state"), "Complete")
      assertEquals(request.getString("phase"), ErasurePhase.ReportPublished.toString)
      val spark = sparkSession()
      try {
        val paths = AnalyticsLakehousePaths.unsafe(lakehouseRoot)
        val pseudonymizer =
          AnalyticsTestSubjectPseudonymizer.fromBase64(required("HIRING_ANALYTICS_HMAC_SECRET_BASE64"))
        val batch = AnalyticsBatchTestSupport.newBatch(
          paths,
          pseudonymizer,
          new MongoActiveDeletionMarkerSource[IO](reactiveDb, pseudonymizer)
        )
        val markers =
          new MongoActiveDeletionMarkerSource[IO](reactiveDb, pseudonymizer).activeSubjectTokens(spark).unsafeRunSync()
        batch.verifyMarkedSubjectsAbsent(spark, markers).unsafeRunSync()
        batch.verifyFilesAbsent(spark, evidence).unsafeRunSync()
      } finally spark.stop()
      assertEquals(db.getCollection("analytics_erasure_completions").countDocuments(Filters.eq("_id", subjectId)), 1L)
      assertEquals(db.getCollection("event_outbox").countDocuments(Filters.in("subjectIds", subjectId)), 0L)
      assertEquals(
        db.getCollection("analytics_report_control")
          .find(Filters.eq("_id", "analytics-report"))
          .first()
          .getString("state"),
        "Published"
      )
      assert(db.getCollection("analytics_report_snapshots").find(Filters.eq("_id", "current")).first() != null, "assertion failed")
      println(
        "RETENTION_PROOF_VERIFIED request=" + subjectId + " exactCapturedPathsAbsent=" + evidence.size +
          " completionCount=1 report=Published"
      )
    }
  }

  private def persistEvidenceFile(seedEnd: Long): Unit = {
    val path = Paths.get(evidenceFile)
    Files.createDirectories(path.getParent)
    val body = "{\"runId\":\"" + runId(seedEnd) + "\",\"database\":\"" + databaseName + "\",\"topic\":\"" + topic +
      "\",\"subjectId\":\"" + subjectId + "\",\"seedEndOffsetExclusive\":" + seedEnd +
      ",\"kafkaRetentionMs\":" + retentionMs + ",\"segmentBytes\":" + segmentBytes +
      ",\"createdAt\":\"" + Instant.now() + "\"}"
    Files.writeString(path, body, StandardCharsets.UTF_8)
  }

  test("actual local retention proof stages, checks, and verifies durable erasure") {
    if (enabled) {
      assert(nonce.matches("[a-f0-9]{16}"), "assertion failed")
      val client = MongoClients.create(required("MONGODB_URI"))
      val reactiveClient = ReactiveMongoClients.create(required("MONGODB_URI"))
      try {
        val db = client.getDatabase(databaseName)
        val reactiveDb = reactiveClient.getDatabase(databaseName)
        mode match {
          case "prepare"               => prepare(db, reactiveDb)
          case "append-retention-tail" => appendRetentionTail(reactiveClient, reactiveDb, db)
          case "inspect"               =>
            inspect(reactiveClient, reactiveDb, db, requireKafka = false, requireAll = false, finalCheck = false)
          case "require-kafka-retention" =>
            inspect(reactiveClient, reactiveDb, db, requireKafka = true, requireAll = false, finalCheck = false)
          case "require-all-retention" =>
            inspect(reactiveClient, reactiveDb, db, requireKafka = true, requireAll = true, finalCheck = false)
          case "verify" =>
            inspect(reactiveClient, reactiveDb, db, requireKafka = true, requireAll = true, finalCheck = true)
          case other => fail("unsupported retention proof mode: " + other)
        }
      } finally {
        reactiveClient.close()
        client.close()
      }
    }
  }
}
