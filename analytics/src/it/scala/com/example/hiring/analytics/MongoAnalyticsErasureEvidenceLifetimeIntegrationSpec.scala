package com.example.hiring.analytics
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.IO
import com.mongodb.ExplainVerbosity
import com.mongodb.client.{MongoClient, MongoClients}
import org.bson.Document

import java.time.Instant
import java.util.Date
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoAnalyticsErasureEvidenceLifetimeIntegrationSpec extends AnalyticsMongoIntegrationSuite {
  override protected def dedicatedMongo: Boolean = true
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def asRunId(value: String): RunId = RunId.from(value).toOption.get
  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
  private def asFingerprint(value: String): RangeFingerprint =
    RangeFingerprint
      .from(AnalyticsDigest.sha256Hex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
      .toOption
      .get

  private def evidenceRows(requestId: String, count: Int): java.util.List[Document] =
    (0 until count).map { index =>
      new Document("_id", UUID.nameUUIDFromBytes(s"$requestId:$index".getBytes("UTF-8")).toString)
        .append("requestId", requestId)
        .append("filePath", s"file:/analytics/silver/part-$index.parquet")
    }.asJava

  test("MSC-02.3 delta-file evidence for one request is read through the requestId index, not a collection scan") {
    val client: MongoClient = MongoClients.create(endpointUri)
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(endpointUri)
    try {
      val database = client.getDatabase(testDatabaseName)
      val files = database.getCollection(AnalyticsCollections.ErasureDeltaFiles)
      val requestIds = Vector.fill(10)(UUID.randomUUID().toString)
      requestIds.foreach(id => files.insertMany(evidenceRows(id, 1000)))
      assertEquals(files.countDocuments(), 10000L)

      def store = AnalyticsErasureWorkerTestSupport.stores(
        reactiveClient,
        AnalyticsMongo4catsTestSupport.database(reactiveClient, testDatabaseName)
      )
      // The first evidence access provisions the index; a second process doing so is idempotent.
      val loaded = store.progress.readDeltaFiles(asAccountSubjectId(requestIds.head)).unsafeRunSync()
      assertEquals(loaded.size, 1000)
      assertEquals(store.progress.readDeltaFiles(asAccountSubjectId(requestIds(1))).unsafeRunSync().size, 1000)
      val indexNames = files.listIndexes().asScala.map(_.getString("name")).toSet
      assert(indexNames.contains(MongoAnalyticsErasureProgress.DeltaEvidenceRequestIndex), indexNames)

      val explain = files
        .find(new Document("requestId", requestIds.head))
        .explain(ExplainVerbosity.EXECUTION_STATS)
      val json = explain.toJson
      assert(json.contains("IXSCAN"), json)
      assert(!json.contains("COLLSCAN"), json)
      assert(json.contains(MongoAnalyticsErasureProgress.DeltaEvidenceRequestIndex), json)
      val stats = explain.get("executionStats", classOf[Document])
      assertEquals(stats.getInteger("totalDocsExamined").intValue, 1000)
      assertEquals(stats.getInteger("nReturned").intValue, 1000)
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }

  test("MSC-02.3 completing an erasure request deletes only that request's delta-file evidence, atomically") {
    val client: MongoClient = MongoClients.create(endpointUri)
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(endpointUri)
    try {
      val database = client.getDatabase(testDatabaseName)
      val files = database.getCollection(AnalyticsCollections.ErasureDeltaFiles)
      val subjectId = UUID.randomUUID().toString
      val otherId = UUID.randomUUID().toString
      val token = UUID.randomUUID().toString
      val now = Instant.now()
      val key = ErasurePhase.ReadyToPublish.ordinal.toLong * ErasurePhase.ProgressPerPhase
      database
        .getCollection("analytics_report_control")
        .insertOne(
          new Document("_id", "analytics-report")
            .append("generation", 2L)
            .append("state", "Hidden")
            .append("nextRevision", 0L)
            .append("lastPublishedRevision", 0L)
            .append("lastRunId", "")
        )
      database.getCollection("users").insertOne(new Document("_id", subjectId).append("accountStatus", "Deleted"))
      database.getCollection("outbox_subject_fences").insertOne(new Document("_id", subjectId).append("deleted", true))
      database
        .getCollection("analytics_erasure_requests")
        .insertOne(
          new Document("_id", subjectId)
            .append("state", "Processing")
            .append("leaseToken", token)
            .append("leaseUntil", Date.from(now.plusSeconds(90)))
            .append("phase", ErasurePhase.ReadyToPublish.toString)
            .append("progress", 0)
            .append("progressKey", key)
        )
      files.insertMany(evidenceRows(subjectId, 25))
      files.insertMany(evidenceRows(otherId, 7))
      val publisher = new MongoAnalyticsReportPublisher[IO](
        reactiveClient,
        AnalyticsMongo4catsTestSupport.database(reactiveClient, testDatabaseName),
        operational = AnalyticsTestOperationalConfig.operational
      )
      val claim =
        ErasureClaim(asAccountSubjectId(subjectId), token, now.plusSeconds(90), ErasurePhase.ReadyToPublish, 0, key)
      val report = AnalyticsReportOutput(now, Vector.empty, None, Vector.empty)

      // A rejected publication (wrong lease token) rolls back with the evidence untouched.
      val stale = claim.copy(leaseToken = UUID.randomUUID().toString)
      (for {
        reservation <- publisher.reserve(asRunId("evidence-" + subjectId), asFingerprint("fp-" + subjectId), now)
        attempt <- publisher.publishErasure(reservation, report, now.plusSeconds(3600), stale, now).attempt
        _ = assert(attempt.isLeft, "publication with a stale lease must be rejected")
        _ = assertEquals(files.countDocuments(new Document("requestId", subjectId)), 25L)
        _ <- publisher.publishErasure(reservation, report, now.plusSeconds(3600), claim, now)
      } yield ()).unsafeRunSync()

      assertEquals(files.countDocuments(new Document("requestId", subjectId)), 0L)
      // The publisher provisioned the index itself (no progress access happened), and the completion delete's
      // filter plans as IXSCAN.
      val deletePlan = database.runCommand(
        new Document(
          "explain",
          new Document("delete", AnalyticsCollections.ErasureDeltaFiles)
            .append(
              "deletes",
              java.util.List.of(new Document("q", new Document("requestId", otherId)).append("limit", 0))
            )
        )
      )
      val planJson = deletePlan.toJson
      assert(planJson.contains("IXSCAN"), planJson)
      assert(!planJson.contains("COLLSCAN"), planJson)
      assertEquals(files.countDocuments(new Document("requestId", otherId)), 7L)
      val request = database.getCollection("analytics_erasure_requests").find(new Document("_id", subjectId)).first()
      assertEquals(request.getString("state"), "Complete")
      assert(request.getDate("expiresAt").after(Date.from(now)), "completed request keeps its retention expiry")
      assert(
        database.getCollection("analytics_erasure_completions").find(new Document("_id", subjectId)).first() != null
      )
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }
}
