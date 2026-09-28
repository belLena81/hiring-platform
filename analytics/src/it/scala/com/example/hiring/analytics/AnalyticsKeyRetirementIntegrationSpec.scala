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

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import cats.effect.{Clock, Deferred, IO}
import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.reactivestreams.client.{
  MongoClient as ReactiveMongoClient,
  MongoClients as ReactiveMongoClients,
  MongoDatabase as ReactiveMongoDatabase
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.compiletime.uninitialized
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Opt-in read-only audit proof using local Delta files and a disposable replica-set database. */
final class AnalyticsKeyRetirementIntegrationSpec extends munit.FunSuite {
  private val enabled = sys.env.get("ANALYTICS_KEY_RETIREMENT_MONGO_URI").exists(_.nonEmpty)
  private var mongo: MongoClient = uninitialized
  private var peerMongo: MongoClient = uninitialized
  private var reactiveMongo: ReactiveMongoClient = uninitialized
  private var reactivePeerMongo: ReactiveMongoClient = uninitialized
  private var database: MongoDatabase = uninitialized
  private var reactiveDatabase: ReactiveMongoDatabase = uninitialized
  private var spark: SparkSession = uninitialized
  private var lakehouseRoot: Path = uninitialized
  private val now = Instant.parse("2026-10-26T00:00:00Z")
  private val oldKeyId = "retiring-key"

  override def beforeAll(): Unit = if (enabled) {
    mongo = MongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    peerMongo = MongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    reactiveMongo = ReactiveMongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    reactivePeerMongo = ReactiveMongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    database = mongo.getDatabase(s"analytics_key_retirement_${java.util.UUID.randomUUID().toString.replace('-', '_')}")
    reactiveDatabase = reactiveMongo.getDatabase(database.getName)
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("AnalyticsKeyRetirementIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    lakehouseRoot = Files.createTempDirectory("analytics-key-retirement-")
    val required = Vector(
      "analytics_report_snapshots",
      "analytics_report_runs",
      "analytics_report_control",
      "analytics_erasure_requests",
      "analytics_erasure_completions",
      "analytics_erasure_delta_files",
      "event_outbox",
      "hiring_migration_ledger",
      "outbox_subject_fences"
    )
    required.foreach(database.createCollection)
    database
      .getCollection("hiring_migration_ledger")
      .insertOne(
        new Document("_id", "003_event_outbox_subject_references").append("state", "Complete")
      )
    seedRegistry(AnalyticsLakehousePaths.unsafe(lakehouseRoot.toUri.toString))
  }

  override def afterAll(): Unit = if (enabled) {
    Try(database.drop())
    Try(mongo.close())
    Try(peerMongo.close())
    Try(reactiveMongo.close())
    Try(reactivePeerMongo.close())
    Try(spark.stop())
    if (lakehouseRoot != null) deleteTree(lakehouseRoot)
  }

  if (enabled) test("audit passes empty verified surfaces and blocks a live marker or old-key Delta row") {
    val paths = AnalyticsLakehousePaths.unsafe(lakehouseRoot.toUri.toString)
    val passed = audit(paths)
    assert(passed.isRight, passed.swap.toOption.toString)

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-1").append("state", "Pending")
      )
    val activeMarker = audit(paths)
    assert(activeMarker.swap.toOption.get.exists(_.contains("active or unexpired erasure marker")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    database
      .getCollection("analytics_report_snapshots")
      .insertOne(
        new Document("_id", "snapshot-1")
          .append("report", new Document("subjectToken", s"${oldKeyId}_report-reference"))
      )
    val oldReportReference = audit(paths)
    assert(oldReportReference.swap.toOption.get.exists(_.contains("analytics_report_snapshots")), "assertion failed")
    database.getCollection("analytics_report_snapshots").deleteMany(new Document())

    val tokenSchema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    spark
      .createDataFrame(List(Row(s"${oldKeyId}_unsafe-reference")).asJava, tokenSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(paths.silver)
    val oldToken = audit(paths)
    assert(oldToken.swap.toOption.get.contains("a current or retained Delta data file references the retiring key"), "assertion failed")
  }

  if (enabled) test("Mongo lakehouse mutex excludes a second independent client until owner release") {
    val root = s"s3a://analytics-test/${java.util.UUID.randomUUID()}"
    val firstLock = new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](reactiveDatabase, Clock[IO])
    val secondLock =
      new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
        reactivePeerMongo.getDatabase(database.getName),
        Clock[IO]
      )
    val result = (for {
      firstEntered <- Deferred[IO, Unit]
      releaseFirst <- Deferred[IO, Unit]
      secondEntered <- Deferred[IO, Unit]
      first <- firstLock.resource(root).use(_ => firstEntered.complete(()) *> releaseFirst.get).start
      _ <- firstEntered.get
      second <- secondLock.resource(root).use(_ => secondEntered.complete(())).start
      beforeRelease <- IO.sleep(1.second) *> secondEntered.tryGet
      _ <- releaseFirst.complete(())
      _ <- secondEntered.get.timeout(10.seconds)
      _ <- first.joinWithNever
      _ <- second.joinWithNever
    } yield beforeRelease).unsafeRunSync()
    assertEquals(result, None)
  }

  if (enabled) test("audit fails closed on malformed erasure state and permits unrelated outbox replay") {
    database
      .getCollection("event_outbox")
      .insertOne(
        new Document("_id", "event-1")
          .append("subjectIds", java.util.List.of("11111111-1111-4111-8111-111111111111"))
          .append("subjectRefsVersion", 1)
          .append("state", "Retryable")
      )
    val paths = AnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("second").toUri.toString)
    seedRegistry(paths)
    val result = audit(paths)
    assert(result.isRight, result.swap.toOption.toString)
    assertEquals(
      result.toOption.get.operatorEvidence,
      "DIAGNOSTIC ONLY: operator-attested; not an authorization to retire a key"
    )

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-unknown-state").append("state", "Unexpected")
      )
    val unknownState = audit(paths).swap.toOption.get
    assert(unknownState.exists(_.contains("erasure request has an unknown state")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-complete-without-expiry").append("state", "Complete")
      )
    val missingExpiry = audit(paths).swap.toOption.get
    assert(missingExpiry.exists(_.contains("missing or invalid retention expiry")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    val missingRegistry = AnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("third").toUri.toString)
    val registryReasons = audit(missingRegistry).swap.toOption.get
    assert(registryReasons.exists(_.contains("continuity registry is missing")), "assertion failed")
    database.getCollection("event_outbox").deleteMany(new Document())
  }

  private def audit(paths: AnalyticsLakehousePaths) =
    AnalyticsKeyRetirement
      .audit(
        spark,
        paths,
        reactiveDatabase,
        oldKeyId,
        AnalyticsKeyRetirement.RetentionEvidence(
          kafka = AnalyticsKeyRetirement.KafkaRetentionEvidence(Some(10L), Some(10L), "test-kafka-barrier"),
          deltaData = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-delta-data"),
          deltaLogs = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-delta-logs"),
          reports = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-reports")
        ),
        AnalyticsKeyRetirement.WriterInventory(
          observedAt = now.minusSeconds(1),
          coverageReference = "test-operator-inventory",
          managed = Vector(
            AnalyticsKeyRetirement
              .WriterRecord("analytics-batch", AnalyticsKeyRetirement.WriterDisposition.Stopped, "test-stop-record")
          ),
          unmanaged = Vector(
            AnalyticsKeyRetirement.WriterRecord(
              "unmanaged-writer",
              AnalyticsKeyRetirement.WriterDisposition.AccessRevoked,
              "test-access-revocation"
            )
          )
        ),
        now,
        new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](reactiveDatabase, Clock[IO])
      )
      .unsafeRunSync()

  private def seedRegistry(paths: AnalyticsLakehousePaths): Unit = {
    val schema = StructType(
      Seq(
        StructField("keyId", StringType, nullable = false),
        StructField("verifier", StringType, nullable = false)
      )
    )
    spark
      .createDataFrame(List(Row(oldKeyId, "A" * 43)).asJava, schema)
      .write
      .format("delta")
      .mode("errorifexists")
      .save(paths.hmacKeyRegistry)
  }

  private def deleteTree(root: Path): Unit = {
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()
  }
}
