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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.client.model.Updates
import org.bson.Document

import java.time.Instant
import java.util.Date
import java.util.UUID
import scala.concurrent.duration.*

class MongoAnalyticsReportPublisherIntegrationSpec extends AnalyticsMongoIntegrationSuite {
  override protected def dedicatedMongo: Boolean = true

  private def asRunId(value: String): RunId = RunId.from(value).toOption.get
  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
  private def asFingerprint(value: String): RangeFingerprint =
    RangeFingerprint
      .from(AnalyticsDigest.sha256Hex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
      .toOption
      .get

  override val munitIOTimeout: FiniteDuration = 5.minutes

  test("production report publisher reserves revisions, rejects hidden publication, and restores expired payload") {
    val client: MongoClient = MongoClients.create(
      endpointUri
    )
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(
      endpointUri
    )
    try {
      val database = client.getDatabase(testDatabaseName)
      database
        .getCollection("analytics_report_control")
        .insertOne(
          new Document("_id", "analytics-report")
            .append("generation", 0L)
            .append("state", "Unpublished")
            .append("nextRevision", 0L)
            .append("lastPublishedRevision", 0L)
            .append("lastRunId", "")
        )
      val publisher =
        new MongoAnalyticsReportPublisher[IO](
          reactiveClient,
          AnalyticsMongo4catsTestSupport.database(reactiveClient, database.getName),
          operational = AnalyticsTestOperationalConfig.operational
        )
      val now = Instant.now()
      val expiry = now.plusSeconds(3600L)
      val report = AnalyticsReportOutput(now, Vector.empty, None, Vector.empty)

      val result = for {
        older <- publisher.reserve(asRunId("older"), asFingerprint("range-older"), now)
        sameRange <- publisher.reserve(asRunId("older"), asFingerprint("range-older"), now.plusMillis(1))
        _ <- IO.raiseWhen(sameRange != older)(new AssertionError("same-range reservation was not idempotent"))
        changedRange <- publisher.reserve(asRunId("older"), asFingerprint("range-changed"), now.plusMillis(2)).attempt
        _ <- IO.raiseWhen(!changedRange.swap.exists(_.isInstanceOf[AnalyticsError.RunIdRangeConflict]))(
          new AssertionError("a run ID was reused with a different range")
        )
        revisionAfterConflict <- IO.blocking(
          database
            .getCollection("analytics_report_control")
            .find(new Document("_id", "analytics-report"))
            .first()
            .getLong("nextRevision")
        )
        _ <- IO.raiseWhen(revisionAfterConflict != older.revision)(
          new AssertionError("idempotent/conflicting reservations leaked a revision increment")
        )
        newer <- publisher.reserve(asRunId("newer"), asFingerprint("range-newer"), now)
        _ <- publisher.publish(newer, report, expiry)
        currentReceipt <- publisher.publicationReceipt(newer)
        _ <- IO.raiseWhen(currentReceipt != AnalyticsReportPublicationReceipt.CurrentGeneration)(
          new AssertionError("a visible guarded publication did not produce a current receipt")
        )
        conflictingReceipt <- publisher
          .publicationReceipt(newer.copy(rangeFingerprint = asFingerprint("other-range")))
          .attempt
        _ <- IO.raiseWhen(!conflictingReceipt.left.exists(_.isInstanceOf[AnalyticsError.RunIdRangeConflict]))(
          new AssertionError("a publication receipt accepted a conflicting range fingerprint")
        )
        stale <- publisher.publish(older, report.copy(asOf = now.plusMillis(1)), expiry).attempt
        _ <- IO.raiseWhen(!stale.left.exists(_.isInstanceOf[AnalyticsError.RunIdRangeConflict]))(
          new AssertionError("an older revision replaced a newer publication")
        )
        retryAfterNewer <- publisher.reserve(asRunId("older"), asFingerprint("range-older"), now.plusMillis(1))
        _ <- IO.raiseWhen(retryAfterNewer.generation != newer.generation || retryAfterNewer.revision <= newer.revision)(
          new AssertionError("an unpublished retry did not advance past the accepted revision")
        )
        _ <- IO.blocking {
          database
            .getCollection("analytics_report_control")
            .updateOne(
              new Document("_id", "analytics-report"),
              Updates.combine(Updates.set("generation", 1L), Updates.set("state", "Hidden"))
            )
        }
        hiddenRun <- publisher.reserve(asRunId("hidden-run"), asFingerprint("range-hidden"), now)
        hidden <- publisher.publish(hiddenRun, report.copy(asOf = now.plusMillis(2)), expiry).attempt
        _ <- IO.raiseWhen(!hidden.left.exists(_ == AnalyticsError.GuardedErasurePublicationRejected))(
          new AssertionError("normal publication was accepted while report control was Hidden")
        )
        supersededReceipt <- publisher.publicationReceipt(newer)
        _ <- IO.raiseWhen(supersededReceipt != AnalyticsReportPublicationReceipt.Superseded)(
          new AssertionError("a receipt from a hidden prior generation was treated as current")
        )
        refreshed <- publisher.reserve(asRunId("older"), asFingerprint("range-older"), now.plusMillis(2))
        _ <- IO.raiseWhen(refreshed.generation != 1L || refreshed.revision <= retryAfterNewer.revision)(
          new AssertionError("an unpublished retry did not reserve a new revision in the current generation")
        )
        staleAfterRefresh <- publisher.publish(older, report.copy(asOf = now.plusMillis(2)), expiry).attempt
        _ <- IO.raiseWhen(!staleAfterRefresh.left.exists(_.isInstanceOf[AnalyticsError.RunIdRangeConflict]))(
          new AssertionError("an old reservation remained usable after it was refreshed")
        )
        _ <- IO.blocking {
          database
            .getCollection("analytics_report_control")
            .updateOne(
              new Document("_id", "analytics-report"),
              Updates.combine(
                Updates.set("generation", 0L),
                Updates.set("state", "Published"),
                Updates.set("lastPublishedRevision", newer.revision),
                Updates.set("lastRunId", newer.runId)
              )
            )
          database
            .getCollection("analytics_report_snapshots")
            .updateOne(
              new Document("_id", "current"),
              Updates.combine(
                Updates.set("expiresAt", Date.from(now.minusSeconds(1L))),
                Updates.set("operatorExtension", "preserved")
              )
            )
        }
        replay <- publisher.reserve(asRunId("newer"), asFingerprint("range-newer"), now)
        _ <- publisher.publish(replay, report.copy(asOf = now.plusMillis(3)), expiry)
        snapshot <- IO.blocking(
          database.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first()
        )
      } yield snapshot

      val snapshot = result.unsafeRunSync()
      assertEquals(snapshot.getLong("revision"), Long.box(2L))
      assertEquals(snapshot.getString("runId"), "newer")
      assert(snapshot.getDate("expiresAt").after(Date.from(now)))
      assertEquals(snapshot.getString("operatorExtension"), "preserved")
      database
        .getCollection("analytics_report_runs")
        .insertOne(
          new Document("_id", "malformed")
            .append("rangeFingerprint", "range-malformed")
            .append("generation", "invalid")
            .append("revision", 3L)
            .append("state", "Reserved")
        )
      val malformed =
        publisher.reserve(asRunId("malformed"), asFingerprint("range-malformed"), now).attempt.unsafeRunSync()
      assert(malformed.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
      val invalidStateRun = publisher
        .reserve(asRunId("invalid-state"), asFingerprint("range-invalid-state"), now)
        .unsafeRunSync()
      database
        .getCollection("analytics_report_runs")
        .updateOne(new Document("_id", "invalid-state"), Updates.set("state", "Unexpected"))
      val invalidState =
        publisher.publish(invalidStateRun, report, expiry).attempt.unsafeRunSync()
      assert(invalidState.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }

  test("cancelling a contended lakehouse mutex leaves its owner intact and releases the waiter promptly") {
    val uri = endpointUri
    val sync = MongoClients.create(uri)
    val reactive = AnalyticsMongo4catsTestSupport.client(uri)
    try {
      val database = sync.getDatabase(s"mutex_cancellation_${UUID.randomUUID()}")
      val db = AnalyticsMongo4catsTestSupport.database(reactive, database.getName)
      val root = "file:///tmp/mutex-cancellation-" + UUID.randomUUID()
      val mutex = new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
        db,
        AnalyticsTestOperationalConfig.streams
      )
      (for {
        attempted <- cats.effect.Deferred[IO, Unit]
        waiting = new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
          db,
          AnalyticsTestOperationalConfig.streams,
          nowOverride = Some(attempted.complete(()).void *> IO.realTimeInstant)
        )
        _ <- mutex.resource(root).use { _ =>
          for {
            original <- IO.blocking(
              database.getCollection("analytics_lakehouse_mutexes").find().first().getString("ownerToken")
            )
            waiter <- waiting.resource(root).use(_ => IO.never[Unit]).start
            _ <- attempted.get
            _ <- waiter.cancel.timeout(5.seconds)
            current <- IO.blocking(
              database.getCollection("analytics_lakehouse_mutexes").find().first().getString("ownerToken")
            )
            _ <- IO(assertEquals(current, original))
          } yield ()
        }
        _ <- mutex.resource(root).use(_ => IO.unit).timeout(5.seconds)
        remaining <- IO.blocking(database.getCollection("analytics_lakehouse_mutexes").countDocuments())
        _ <- IO(assertEquals(remaining, 0L))
      } yield ()).unsafeRunSync()
    } finally {
      sync.close()
      AnalyticsMongo4catsTestSupport.close(reactive)
    }
  }

  test("pinned reservation survives TTL policy and never refreshes across a completed deletion generation") {
    val uri = endpointUri
    val client = MongoClients.create(uri)
    val reactive = AnalyticsMongo4catsTestSupport.client(uri)
    try {
      val database = client.getDatabase(testDatabaseName)
      val control = database.getCollection("analytics_report_control")
      control.insertOne(
        new Document("_id", "analytics-report")
          .append("generation", 0L)
          .append("state", "Unpublished")
          .append("nextRevision", 0L)
          .append("lastPublishedRevision", 0L)
          .append("lastRunId", "")
      )
      val publisher = new MongoAnalyticsReportPublisher[IO](
        reactive,
        AnalyticsMongo4catsTestSupport.database(reactive, database.getName),
        AnalyticsTestOperationalConfig.operational
      )
      publisher.ensurePinnedRetentionIndex.unsafeRunSync()
      publisher.ensurePinnedRetentionIndex.unsafeRunSync()
      val now = Instant.now()
      val run = asRunId("stream-pinned")
      val fingerprint = asFingerprint("stream-input")
      val reservation = publisher.reservePinned(run, fingerprint, now).unsafeRunSync()
      val stored = database.getCollection("analytics_report_runs").find(new Document("_id", run.value)).first()
      assert(
        !stored.containsKey("expiresAt") || stored.get("expiresAt") == null,
        "unfinished pinned receipts must not be removed by the existing TTL index"
      )
      control.updateOne(
        new Document("_id", "analytics-report"),
        Updates.combine(Updates.set("generation", 1L), Updates.set("state", "Unpublished"))
      )
      val retry = publisher.reservePinned(run, fingerprint, now.plusSeconds(1000)).unsafeRunSync()
      assertEquals(retry, reservation)
      assertEquals(publisher.publicationReceipt(retry).unsafeRunSync(), AnalyticsReportPublicationReceipt.Superseded)
      val stale = publisher
        .publish(retry, AnalyticsReportOutput(now, Vector.empty, None, Vector.empty), now.plusSeconds(3600))
        .attempt
        .unsafeRunSync()
      assertEquals(stale.left.toOption, Some(AnalyticsError.GuardedErasurePublicationRejected))
      assertEquals(database.getCollection("analytics_report_snapshots").countDocuments(), 0L)
      val next = publisher.reservePinned(asRunId("stream-pinned-next"), fingerprint, now).unsafeRunSync()
      publisher
        .publish(next, AnalyticsReportOutput(now, Vector.empty, None, Vector.empty), now.plusSeconds(3600))
        .unsafeRunSync()
      assertEquals(
        publisher.publicationReceipt(next).unsafeRunSync(),
        AnalyticsReportPublicationReceipt.CurrentGeneration
      )
      val published =
        database.getCollection("analytics_report_runs").find(new Document("_id", next.runId.value)).first()
      assert(!published.containsKey("expiresAt") || published.get("expiresAt") == null)
      val runs = database.getCollection("analytics_report_runs")
      val old = Date.from(now.minusSeconds(40L * 86400L))
      runs.updateMany(new Document(), Updates.set("createdAt", old))
      def oldPublished(id: String): Document = new Document("_id", id)
        .append("rangeFingerprint", fingerprint.value)
        .append("generation", 1L)
        .append("revision", 0L)
        .append("state", "Published")
        .append("createdAt", old)
      runs.insertOne(oldPublished("removable-completed"))
      runs.insertOne(oldPublished("protected-replay"))
      runs.insertOne(oldPublished("stream-maintenance-abandoned").append("state", "Reserved"))
      val removed = publisher.compactPublished(Set(asRunId("protected-replay")), now).unsafeRunSync()
      assertEquals(removed, 2L)
      assertEquals(runs.countDocuments(new Document("_id", "removable-completed")), 0L)
      assertEquals(runs.countDocuments(new Document("_id", "protected-replay")), 1L)
      assertEquals(runs.countDocuments(new Document("_id", run.value)), 1L) // unfinished reservation
      assertEquals(runs.countDocuments(new Document("_id", next.runId.value)), 1L) // current snapshot
      assertEquals(publisher.compactPublished(Set(asRunId("protected-replay")), now).unsafeRunSync(), 0L)
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactive)
    }
  }

  test("report revision reservation retries transient Mongo transaction and uncertain commit without duplicates") {
    val client = MongoClients.create(
      endpointUri
    )
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(
      endpointUri
    )
    try {
      val database = client.getDatabase(testDatabaseName)
      val control = database.getCollection("analytics_report_control")
      control.insertOne(
        new Document("_id", "analytics-report")
          .append("generation", 0L)
          .append("state", "Unpublished")
          .append("nextRevision", 0L)
          .append("lastPublishedRevision", 0L)
          .append("lastRunId", "")
      )
      val publisher =
        new MongoAnalyticsReportPublisher[IO](
          reactiveClient,
          AnalyticsMongo4catsTestSupport.database(reactiveClient, database.getName),
          operational = AnalyticsTestOperationalConfig.operational
        )
      val admin = client.getDatabase("admin")
      def failOnce(command: String, errorCode: Int, label: String): Unit = {
        admin.runCommand(
          new Document("configureFailPoint", "failCommand")
            .append("mode", new Document("times", 1))
            .append(
              "data",
              new Document("failCommands", java.util.List.of(command))
                .append("errorCode", errorCode)
                .append("errorLabels", java.util.List.of(label))
            )
        )
        ()
      }
      val now = Instant.now()
      failOnce("update", 112, "TransientTransactionError")
      val first = publisher.reserve(asRunId("transient"), asFingerprint("range-transient"), now).unsafeRunSync()
      assertEquals(first.revision, 1L)
      failOnce("commitTransaction", 91, "UnknownTransactionCommitResult")
      val second = publisher.reserve(asRunId("uncertain"), asFingerprint("range-uncertain"), now).unsafeRunSync()
      assertEquals(second.revision, 2L)
      assertEquals(control.find(new Document("_id", "analytics-report")).first().getLong("nextRevision"), Long.box(2L))
      assertEquals(database.getCollection("analytics_report_runs").countDocuments(), 2L)
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }

  test("erasure publication reveals the report and writes the TTL independent completion ledger atomically") {
    val client = MongoClients.create(
      endpointUri
    )
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(
      endpointUri
    )
    try {
      val database = client.getDatabase(testDatabaseName)
      val subjectId = UUID.randomUUID().toString
      val token = UUID.randomUUID().toString
      val secondSubjectId = UUID.randomUUID().toString
      val secondToken = UUID.randomUUID().toString
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
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", subjectId).append("deleted", true)
        )
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
      database.getCollection("users").insertOne(new Document("_id", secondSubjectId).append("accountStatus", "Deleted"))
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", secondSubjectId).append("deleted", true)
        )
      database
        .getCollection("analytics_erasure_requests")
        .insertOne(
          new Document("_id", secondSubjectId)
            .append("state", "Processing")
            .append("leaseToken", secondToken)
            .append("leaseUntil", Date.from(now.plusSeconds(90)))
            .append("phase", ErasurePhase.ReadyToPublish.toString)
            .append("progress", 0)
            .append("progressKey", key)
        )
      val publisher =
        new MongoAnalyticsReportPublisher[IO](
          reactiveClient,
          AnalyticsMongo4catsTestSupport.database(reactiveClient, database.getName),
          operational = AnalyticsTestOperationalConfig.operational
        )
      val claim =
        ErasureClaim(asAccountSubjectId(subjectId), token, now.plusSeconds(90), ErasurePhase.ReadyToPublish, 0, key)
      val report = AnalyticsReportOutput(now, Vector.empty, None, Vector.empty)
      val result = for {
        reservation <- publisher.reserve(
          asRunId("analytics-erasure-" + subjectId),
          asFingerprint("fingerprint-" + subjectId),
          now
        )
        _ <- publisher.publishErasure(reservation, report, now.plusSeconds(3600), claim, now)
        secondReservation <- publisher.reserve(
          asRunId("analytics-erasure-" + secondSubjectId),
          asFingerprint("fingerprint-" + secondSubjectId),
          now
        )
        secondClaim = ErasureClaim(
          asAccountSubjectId(secondSubjectId),
          secondToken,
          now.plusSeconds(90),
          ErasurePhase.ReadyToPublish,
          0,
          key
        )
        _ <- publisher.publishErasure(secondReservation, report, now.plusSeconds(3600), secondClaim, now)
        state <- IO.blocking(
          database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first()
        )
        request <- IO.blocking(
          database.getCollection("analytics_erasure_requests").find(new Document("_id", subjectId)).first()
        )
        completion <- IO.blocking(
          database.getCollection("analytics_erasure_completions").find(new Document("_id", subjectId)).first()
        )
        secondCompletion <- IO.blocking(
          database.getCollection("analytics_erasure_completions").find(new Document("_id", secondSubjectId)).first()
        )
        snapshot <- IO.blocking(
          database.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first()
        )
      } yield (state, request, completion, secondCompletion, snapshot)
      val (state, request, completion, secondCompletion, snapshot) = result.unsafeRunSync()
      assertEquals(state.getString("state"), "Published")
      assertEquals(request.getString("state"), "Complete")
      assert(request.getDate("expiresAt").after(Date.from(now)))
      assert(completion != null)
      assert(secondCompletion != null)
      assertEquals(snapshot.getString("state"), "Published")
      assertEquals(snapshot.getLong("generation"), Long.box(2L))
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }

  test("guarded erasure publication rejects another request that has not passed retention") {
    val client = MongoClients.create(
      endpointUri
    )
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(
      endpointUri
    )
    try {
      val database = client.getDatabase(testDatabaseName)
      val subjectId = UUID.randomUUID().toString
      val otherId = UUID.randomUUID().toString
      val token = UUID.randomUUID().toString
      val now = Instant.now()
      val key = ErasurePhase.ReadyToPublish.ordinal.toLong * ErasurePhase.ProgressPerPhase
      database
        .getCollection("analytics_report_control")
        .insertOne(
          new Document("_id", "analytics-report")
            .append("generation", 3L)
            .append("state", "Hidden")
            .append("nextRevision", 0L)
            .append("lastPublishedRevision", 0L)
            .append("lastRunId", "")
        )
      database.getCollection("users").insertOne(new Document("_id", subjectId).append("accountStatus", "Deleted"))
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", subjectId).append("deleted", true)
        )
      database
        .getCollection("analytics_erasure_requests")
        .insertMany(
          java.util.List.of(
            new Document("_id", subjectId)
              .append("state", "Processing")
              .append("leaseToken", token)
              .append("leaseUntil", Date.from(now.plusSeconds(90)))
              .append("phase", ErasurePhase.ReadyToPublish.toString)
              .append("progress", 0)
              .append("progressKey", key),
            new Document("_id", otherId).append("state", "Pending")
          )
        )
      val publisher =
        new MongoAnalyticsReportPublisher[IO](
          reactiveClient,
          AnalyticsMongo4catsTestSupport.database(reactiveClient, database.getName),
          operational = AnalyticsTestOperationalConfig.operational
        )
      val claim =
        ErasureClaim(asAccountSubjectId(subjectId), token, now.plusSeconds(90), ErasurePhase.ReadyToPublish, 0, key)
      val report = AnalyticsReportOutput(now, Vector.empty, None, Vector.empty)
      val result = for {
        reservation <- publisher.reserve(
          asRunId("analytics-erasure-" + subjectId),
          asFingerprint("fingerprint-" + subjectId),
          now
        )
        attempt <- publisher.publishErasure(reservation, report, now.plusSeconds(3600), claim, now).attempt
        state <- IO.blocking(
          database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first()
        )
        request <- IO.blocking(
          database.getCollection("analytics_erasure_requests").find(new Document("_id", subjectId)).first()
        )
        snapshot <- IO.blocking(
          database.getCollection("analytics_report_snapshots").find(new Document("_id", "current")).first()
        )
      } yield (attempt, state, request, snapshot)
      val (attempt, state, request, snapshot) = result.unsafeRunSync()
      assert(attempt.swap.toOption.exists(_.isInstanceOf[AnalyticsError.GuardedErasurePublicationRejected.type]))
      assertEquals(state.getString("state"), "Hidden")
      assertEquals(request.getString("state"), "Processing")
      assertEquals(snapshot, null)
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }

  test("worker claims resume durable checkpoints, reject stale tokens, and persist broker barriers") {
    val client = MongoClients.create(
      endpointUri
    )
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(
      endpointUri
    )
    try {
      val database = client.getDatabase(testDatabaseName)
      val subjectId = UUID.randomUUID().toString
      val first = "hiring-publisher-" + UUID.randomUUID().toString
      val second = "hiring-publisher-" + UUID.randomUUID().toString
      val now = Instant.now()
      val requests = database.getCollection("analytics_erasure_requests")
      requests.insertOne(
        new Document("_id", subjectId)
          .append("state", "Pending")
          .append("requestedAt", Date.from(now))
          .append("fencingVersion", 1)
          .append("transactionalIds", java.util.List.of(first, second))
      )
      database
        .getCollection("hiring_migration_ledger")
        .insertOne(
          new Document("_id", "003_event_outbox_subject_references").append("state", "Complete")
        )
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", subjectId)
            .append("deleted", true)
            .append("leaseToken", "old")
            .append("leaseUntil", Date.from(now.plusSeconds(30)))
        )
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("subjectIds", java.util.List.of(subjectId))
            .append("subjectRefsVersion", 1)
        )
      val store = AnalyticsErasureWorkerTestSupport.stores(
        reactiveClient,
        AnalyticsMongo4catsTestSupport.database(reactiveClient, database.getName)
      )
      val result = for {
        claim <- store.queue.claim(now, now.plusSeconds(360), 1).map(_.head)
        typedSubjectId = asAccountSubjectId(subjectId)
        early <- store.queue.publisherDrainReady(typedSubjectId, now, 30.seconds)
        drained <- store.queue.publisherDrainReady(typedSubjectId, now.plusSeconds(61), 30.seconds)
        purged <- store.queue.purgeOutbox(typedSubjectId, now.plusSeconds(61), 30.seconds)
        firstAdvance <- store.progress.advance(claim, ErasurePhase.PublisherDrained, 0, now.plusSeconds(62))
        staleAdvance <- store.progress.advance(claim, ErasurePhase.PublisherDrained, 0, now.plusSeconds(62))
        advanced = claim.copy(
          phase = ErasurePhase.PublisherDrained,
          progress = 0,
          progressKey = ErasurePhase.PublisherDrained.ordinal.toLong * ErasurePhase.ProgressPerPhase
        )
        barrier = KafkaRetentionBarrier
          .from("hiring.operational-events", Vector(0 -> 21L, 1 -> 14L))
          .toOption
          .get
        saved <- store.barrier.persistBarrier(advanced, barrier, now.plusSeconds(63))
        loaded <- store.barrier.readBarrier(asAccountSubjectId(subjectId))
        _ <- store.progress.releaseForOtherRequests(advanced, now.plusSeconds(64))
        reclaimed <- store.queue.claim(now.plusSeconds(65), now.plusSeconds(125), 1).map(_.head)
        staleRenew <- store.progress.renew(advanced, now.plusSeconds(66), now.plusSeconds(126))
        currentRenew <- store.progress.renew(reclaimed, now.plusSeconds(66), now.plusSeconds(126))
        deferred <- store.progress.defer(reclaimed, now.plusSeconds(120), now.plusSeconds(66))
        beforeResume <- store.queue.claim(now.plusSeconds(100), now.plusSeconds(160), 1)
        resumed <- store.queue.claim(now.plusSeconds(121), now.plusSeconds(181), 1).map(_.head)
      } yield (
        claim,
        early,
        drained,
        purged,
        firstAdvance,
        staleAdvance,
        saved,
        loaded,
        reclaimed,
        staleRenew,
        currentRenew,
        deferred,
        beforeResume,
        resumed
      )

      val (
        claim,
        early,
        drained,
        purged,
        firstAdvance,
        staleAdvance,
        saved,
        loaded,
        reclaimed,
        staleRenew,
        currentRenew,
        deferred,
        beforeResume,
        resumed
      ) =
        result.unsafeRunSync()
      assertEquals(early, false)
      assertEquals(drained, true)
      assertEquals(purged, true)
      assertEquals(firstAdvance, ErasureUpdate.Applied)
      assertEquals(staleAdvance, ErasureUpdate.LeaseLost)
      assertEquals(saved, ErasureUpdate.Applied)
      assertEquals(
        loaded,
        Some(
          KafkaRetentionBarrier
            .from("hiring.operational-events", Vector(0 -> 21L, 1 -> 14L))
            .toOption
            .get
        )
      )
      assertNotEquals(reclaimed.leaseToken, claim.leaseToken)
      assertEquals(staleRenew, ErasureUpdate.LeaseLost)
      assertEquals(currentRenew, ErasureUpdate.Applied)
      assertEquals(deferred, ErasureUpdate.Applied)
      assertEquals(beforeResume, Vector.empty)
      val typedSubjectId = asAccountSubjectId(subjectId)
      assertEquals(resumed.requestId, typedSubjectId)
      assertNotEquals(resumed.leaseToken, reclaimed.leaseToken)
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("subjectIds", "malformed")
            .append("subjectRefsVersion", 1)
        )
      val malformedRefs =
        store.queue.purgeOutbox(typedSubjectId, now.plusSeconds(130), 30.seconds).attempt.unsafeRunSync()
      assert(malformedRefs.swap.toOption.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration]))
    } finally {
      client.close()
      AnalyticsMongo4catsTestSupport.close(reactiveClient)
    }
  }
}
