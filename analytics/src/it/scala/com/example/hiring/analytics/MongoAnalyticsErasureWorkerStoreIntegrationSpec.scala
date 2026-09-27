package com.example.hiring.analytics

import com.example.hiring.analytics.batch.*
import com.example.hiring.analytics.erasure.*
import com.example.hiring.analytics.mongo.*

import cats.Applicative
import cats.effect.{Clock, Deferred, IO}
import cats.effect.unsafe.implicits.global
import com.mongodb.{ConnectionString, MongoClientSettings}
import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import org.bson.Document
import org.apache.spark.sql.SparkSession
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.nio.file.Files
import java.util.Date
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

class MongoAnalyticsErasureWorkerStoreIntegrationSpec extends munit.FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(image))

  private def replicaSet(): ReplicaSet = {
    val container = (new ReplicaSet)
      .withExposedPorts(27017)
      .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0")
      .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
    container.start()
    val initiated = container.execInContainer(
      "mongosh",
      "--quiet",
      "--eval",
      "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
    )
    if (initiated.getExitCode != 0) {
      container.stop()
      throw new AssertionError(s"Mongo replica-set initiation failed: ${initiated.getStderr}")
    }
    var remaining = 60
    var primary = false
    while (remaining > 0 && !primary) {
      val result = container.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary")
      primary = result.getExitCode == 0 && result.getStdout.trim == "true"
      if (!primary) Thread.sleep(250L)
      remaining -= 1
    }
    if (!primary) {
      container.stop()
      throw new AssertionError("Mongo replica set did not elect a primary")
    }
    container
  }

  private def fixedClock(at: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: Applicative[IO] = Applicative[IO]
    override def monotonic: IO[FiniteDuration] = IO.pure(0L.nanoseconds)
    override def realTime: IO[FiniteDuration] = IO.pure(FiniteDuration(at.toEpochMilli, MILLISECONDS))
  }

  test("expired worker lease resumes its checkpoint across client restart and fences in-flight evidence writes") {
    val container = replicaSet()
    val connectionString =
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    val databaseName = s"analytics_erasure_recovery_${UUID.randomUUID()}"
    val pauseEvidenceWrite = new AtomicBoolean(false)
    val evidenceWriteStarted = new CountDownLatch(1)
    val competingClaimStarted = new CountDownLatch(1)
    val resumeEvidenceWrite = new CountDownLatch(1)
    val evidencePauseTimedOut = new AtomicBoolean(false)
    val listener = new CommandListener {
      override def commandStarted(event: CommandStartedEvent): Unit = {
        val command = event.getCommand
        val targetsEvidence = event.getCommandName match {
          case "update"    => Option(command.getString("update")).exists(_.getValue == "analytics_erasure_delta_files")
          case "bulkWrite" => command.toJson.contains("analytics_erasure_delta_files")
          case _           => false
        }
        if (pauseEvidenceWrite.get() && event.getDatabaseName == databaseName && targetsEvidence) {
          evidenceWriteStarted.countDown()
          if (!resumeEvidenceWrite.await(2, TimeUnit.MINUTES)) evidencePauseTimedOut.set(true)
        }
        if (
          pauseEvidenceWrite.get() && event.getDatabaseName == databaseName && event.getCommandName == "findAndModify"
        )
          competingClaimStarted.countDown()
      }

      override def commandSucceeded(event: CommandSucceededEvent): Unit = ()
      override def commandFailed(event: CommandFailedEvent): Unit = ()
    }
    val settings = MongoClientSettings
      .builder()
      .applyConnectionString(new ConnectionString(connectionString))
      .addCommandListener(listener)
      .build()
    var client: MongoClient = null
    var restartedClient: MongoClient = null
    try {
      client = MongoClients.create(settings)
      val database = client.getDatabase(databaseName)
      val requestId = UUID.randomUUID().toString
      val requestedAt = Instant.parse("2026-01-01T00:00:00Z")
      database
        .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("transactionalIds", java.util.List.of("hiring-publisher-test"))
            .append("requestedAt", Date.from(requestedAt))
        )
      val store = new MongoAnalyticsErasureWorkerStore(client, database)
      val barrier = KafkaRetentionBarrier(
        "hiring.operational-events",
        Vector(
          KafkaRetentionBarrier.Partition(0, 21L),
          KafkaRetentionBarrier.Partition(1, 14L)
        )
      )
      val fileEvidence = Vector("file:/analytics/silver/part-00001.parquet", "file:/analytics/gold/part-00003.parquet")
      val inFlightFileEvidence = "file:/analytics/quarantine/part-00004.parquet"
      val staleWorkerNow = requestedAt.plusSeconds(9)

      val result = for {
        initial <- store.claim(requestedAt, requestedAt.plusSeconds(10), 1).map(_.head)
        transactionalIds <- store.transactionalIds(requestId)
        _ = assertEquals(transactionalIds, Vector("hiring-publisher-test"))
        _ <- store.advance(initial, ErasurePhase.PublisherDrained, 0, requestedAt.plusSeconds(1))
        publisherDrained = initial.copy(
          phase = ErasurePhase.PublisherDrained,
          progress = 0,
          progressKey = ErasurePhase.PublisherDrained.ordinal.toLong * ErasurePhase.ProgressPerPhase
        )
        _ <- store.persistBarrier(publisherDrained, barrier, requestedAt.plusSeconds(2))
        _ <- store.persistDeltaFiles(publisherDrained, fileEvidence, requestedAt.plusSeconds(2))
        advanced <- store.advance(publisherDrained, ErasurePhase.PublisherDrained, 17, requestedAt.plusSeconds(3))
        _ <- IO.raiseWhen(!advanced)(new AssertionError("initial worker could not persist its checkpoint"))
        checkpoint = publisherDrained.copy(
          phase = ErasurePhase.PublisherDrained,
          progress = 17,
          progressKey = ErasurePhase.PublisherDrained.ordinal.toLong * ErasurePhase.ProgressPerPhase + 17L
        )
        _ <- IO.delay(pauseEvidenceWrite.set(true))
        inFlightWrite <- store.persistDeltaFiles(checkpoint, Vector(inFlightFileEvidence), staleWorkerNow).start
        _ <- IO.blocking {
          if (!evidenceWriteStarted.await(90, TimeUnit.SECONDS))
            throw new AssertionError("old worker did not reach the file evidence write")
        }
        _ <- IO.blocking { restartedClient = MongoClients.create(settings) }
        restartedDatabase = restartedClient.getDatabase(databaseName)
        restartedStore = new MongoAnalyticsErasureWorkerStore(restartedClient, restartedDatabase)
        claimResult <- Deferred[IO, Either[Throwable, Vector[ErasureClaim]]]
        claimFiber <- restartedStore
          .claim(requestedAt.plusSeconds(11), requestedAt.plusSeconds(61), 1)
          .attempt
          .flatTap(claimResult.complete)
          .start
        _ <- IO.blocking {
          if (!competingClaimStarted.await(30, TimeUnit.SECONDS))
            throw new AssertionError("competing claim did not reach Mongo while the evidence transaction was paused")
        }
        _ <- IO.sleep(300.millis)
        claimSucceededBeforeRelease <- claimResult.tryGet.map(_.exists {
          case Right(claims) => claims.nonEmpty
          case Left(_)       => false
        })
        _ = assert(
          !claimSucceededBeforeRelease,
          "new worker claimed the request while the old transaction held its fence"
        )
        _ <- IO.blocking(resumeEvidenceWrite.countDown())
        inFlightEvidenceSaved <- inFlightWrite.joinWithNever
        recoveryAttempt <- claimFiber.joinWithNever
        recovered <- recoveryAttempt match {
          case Right(claims) => IO.pure(claims.head)
          case Left(_) => restartedStore.claim(requestedAt.plusSeconds(11), requestedAt.plusSeconds(61), 1).map(_.head)
        }
        _ = assert(!evidencePauseTimedOut.get(), "test synchronization expired before releasing the file write")
        staleBarrier <- store.persistBarrier(checkpoint, barrier.copy(topic = "wrong-topic"), staleWorkerNow)
        staleFiles <- store.persistDeltaFiles(checkpoint, Vector("file:/analytics/forged.parquet"), staleWorkerNow)
        staleDeferred <- store.defer(checkpoint, requestedAt.plusSeconds(40), staleWorkerNow)
        staleAdvanced <- store.advance(checkpoint, ErasurePhase.PublisherDrained, 18, staleWorkerNow)
        barrierReplay <- restartedStore.persistBarrier(recovered, barrier, requestedAt.plusSeconds(13))
        fileReplay <- restartedStore.persistDeltaFiles(recovered, fileEvidence, requestedAt.plusSeconds(13))
        loadedBarrier <- restartedStore.readBarrier(requestId)
        loadedFiles <- restartedStore.readDeltaFiles(requestId)
        request <- IO.blocking(
          restartedDatabase
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .find(new Document("_id", requestId))
            .first()
        )
      } yield (
        initial,
        checkpoint,
        recovered,
        inFlightEvidenceSaved,
        staleBarrier,
        staleFiles,
        staleDeferred,
        staleAdvanced,
        barrierReplay,
        fileReplay,
        loadedBarrier,
        loadedFiles,
        request
      )

      val (
        initial,
        checkpoint,
        recovered,
        inFlightEvidenceSaved,
        staleBarrier,
        staleFiles,
        staleDeferred,
        staleAdvanced,
        barrierReplay,
        fileReplay,
        loadedBarrier,
        loadedFiles,
        request
      ) = result.unsafeRunSync()
      assertEquals(recovered.requestId, initial.requestId)
      assertNotEquals(recovered.leaseToken, initial.leaseToken)
      assertEquals(recovered.phase, checkpoint.phase)
      assertEquals(recovered.progress, checkpoint.progress)
      assertEquals(recovered.progressKey, checkpoint.progressKey)
      assertEquals(inFlightEvidenceSaved, true)
      assertEquals(staleBarrier, false)
      assertEquals(staleFiles, false)
      assertEquals(staleDeferred, false)
      assertEquals(staleAdvanced, false)
      assertEquals(barrierReplay, true)
      assertEquals(fileReplay, true)
      assertEquals(loadedBarrier, Some(barrier))
      assertEquals(loadedFiles.sorted, (fileEvidence :+ inFlightFileEvidence).sorted)
      assertEquals(request.getString("state"), "Processing")
      assertEquals(request.getString("phase"), ErasurePhase.PublisherDrained.toString)
      assertEquals(request.getInteger("progress"), Int.box(17))
      assertEquals(request.getLong("progressKey"), Long.box(checkpoint.progressKey))
      assertEquals(request.getDate("resumeAfter"), null)
      assertEquals(request.getString("leaseToken"), recovered.leaseToken)
    } finally {
      resumeEvidenceWrite.countDown()
      if (restartedClient != null) restartedClient.close()
      if (client != null) client.close()
      container.stop()
    }
  }

  test("erasure processing restarts from the durable Delta checkpoint and publishes completion") {
    val container = replicaSet()
    val connectionString =
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    val databaseName = s"erasure_restart_${UUID.randomUUID()}"
    val requestId = UUID.randomUUID().toString
    val receiptId = UUID.randomUUID().toString
    val startedAt = Instant.now()
    val afterRetention = startedAt.plusSeconds(31L * 24L * 60L * 60L)
    val barrier = KafkaRetentionBarrier(
      "hiring.operational-events",
      Vector(KafkaRetentionBarrier.Partition(0, 0L))
    )
    val retentionHasPassed = new AtomicBoolean(false)
    val retention = new KafkaRetention {
      override def capture(connection: KafkaConnection, topic: String): IO[KafkaRetentionBarrier] = IO.pure(barrier)
      override def retentionPassed(connection: KafkaConnection, value: KafkaRetentionBarrier): IO[Boolean] =
        IO.pure(retentionHasPassed.get())
    }
    val producerFencer = new TransactionalProducerFencer {
      override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] = IO.unit
    }
    val lakehouseRoot = Files.createTempDirectory("analytics-erasure-process-restart").toUri.toString.stripSuffix("/")
    val paths = AnalyticsLakehousePaths(lakehouseRoot)
    val secret = "analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8")
    var client: MongoClient = null
    var restartedClient: MongoClient = null
    var spark: SparkSession = null
    try {
      client = MongoClients.create(connectionString)
      val database = client.getDatabase(databaseName)
      database
        .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("transactionalIds", java.util.Collections.emptyList[String]())
            .append("requestedAt", Date.from(startedAt))
            .append("receiptId", receiptId)
        )
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", requestId).append("deleted", true)
        )
      database
        .getCollection("users")
        .insertOne(
          new Document("_id", requestId).append("accountStatus", "Deleted")
        )
      database
        .getCollection("hiring_migration_ledger")
        .insertOne(
          new Document("_id", "003_event_outbox_subject_references").append("state", "Complete")
        )
      database
        .getCollection("analytics_report_control")
        .insertOne(
          new Document("_id", "analytics-report")
            .append("state", "Hidden")
            .append("generation", 1L)
            .append("nextRevision", 0L)
            .append("lastPublishedRevision", 0L)
            .append("lastRunId", "")
        )
      spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("MongoAnalyticsErasureWorkerRestartIntegrationSpec")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

      val firstStore = new MongoAnalyticsErasureWorkerStore(client, database)
      val firstPublisher = new MongoAnalyticsReportPublisher(client, database)
      val firstWorker = new AnalyticsErasureWorker(
        spark,
        database,
        firstStore,
        KafkaConnection("unused:9092"),
        KafkaConnection("unused:9092"),
        "hiring.operational-events",
        paths,
        AnalyticsTestSubjectPseudonymizer.fromSecret(secret),
        firstPublisher,
        clock = fixedClock(startedAt),
        leaseDuration = 60.days,
        producerFencer = producerFencer,
        kafkaRetention = retention
      )
      val runId = "analytics-erasure-" + requestId
      val rangeFingerprint = "erasure:" + requestId

      val result = for {
        initialClaim <- firstStore.claim(startedAt, startedAt.plusSeconds(60L * 86400L), 1).map(_.head)
        reservation <- firstPublisher.reserve(runId, rangeFingerprint, startedAt)
        firstAttempt <- firstWorker.process(initialClaim, reservation).attempt
        checkpoint <- IO.blocking(
          database
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .find(new Document("_id", requestId))
            .first()
        )
        _ = assertEquals(checkpoint.getString("phase"), ErasurePhase.DeltaPurged.toString)
        _ = assert(checkpoint.getDate("resumeAfter").toInstant.isAfter(startedAt))
        _ = assert(firstAttempt.swap.toOption.exists(_ == AnalyticsError.ErasureDeferred))
        _ <- IO.blocking {
          client.close()
          client = null
          restartedClient = MongoClients.create(connectionString)
        }
        restartedDatabase = restartedClient.getDatabase(databaseName)
        restartedStore = new MongoAnalyticsErasureWorkerStore(restartedClient, restartedDatabase)
        restartedPublisher = new MongoAnalyticsReportPublisher(restartedClient, restartedDatabase)
        _ <- IO.delay(retentionHasPassed.set(true))
        resumedClaim <- restartedStore.claim(afterRetention, afterRetention.plusSeconds(86400L), 1).map(_.head)
        _ = assertEquals(resumedClaim.phase, ErasurePhase.DeltaPurged)
        resumedReservation <- restartedPublisher.reserve(runId, rangeFingerprint, afterRetention)
        restartedWorker = new AnalyticsErasureWorker(
          spark,
          restartedDatabase,
          restartedStore,
          KafkaConnection("unused:9092"),
          KafkaConnection("unused:9092"),
          "hiring.operational-events",
          paths,
          AnalyticsTestSubjectPseudonymizer.fromSecret(secret),
          restartedPublisher,
          clock = fixedClock(afterRetention),
          leaseDuration = 60.days,
          producerFencer = producerFencer,
          kafkaRetention = retention
        )
        _ <- restartedWorker.process(resumedClaim, resumedReservation)
        readyToPublish <- IO.blocking(
          restartedDatabase
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .find(new Document("_id", requestId))
            .first()
        )
        _ = assertEquals(readyToPublish.getString("phase"), ErasurePhase.ReadyToPublish.toString)
        _ <- IO.blocking(
          restartedDatabase
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .updateOne(
              new Document("_id", requestId),
              new Document("$set", new Document("leaseUntil", Date.from(afterRetention.minusSeconds(1L))))
            )
        )
        finalizerClaim <- restartedStore.claim(afterRetention, afterRetention.plusSeconds(86400L), 1).map(_.head)
        _ = assertEquals(finalizerClaim.phase, ErasurePhase.ReadyToPublish)
        finalizerReservation <- restartedPublisher.reserve(runId, rangeFingerprint, afterRetention)
        finalizerWorker = new AnalyticsErasureWorker(
          spark,
          restartedDatabase,
          restartedStore,
          KafkaConnection("unused:9092"),
          KafkaConnection("unused:9092"),
          "hiring.operational-events",
          paths,
          AnalyticsTestSubjectPseudonymizer.fromSecret(secret),
          restartedPublisher,
          clock = fixedClock(afterRetention),
          leaseDuration = 60.days,
          producerFencer = producerFencer,
          kafkaRetention = retention
        )
        _ <- finalizerWorker.process(finalizerClaim, finalizerReservation)
        request <- IO.blocking(
          restartedDatabase
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .find(new Document("_id", requestId))
            .first()
        )
        completion <- IO.blocking(
          restartedDatabase
            .getCollection("analytics_erasure_completions")
            .find(new Document("_id", requestId))
            .first()
        )
        control <- IO.blocking(
          restartedDatabase
            .getCollection("analytics_report_control")
            .find(new Document("_id", "analytics-report"))
            .first()
        )
        snapshot <- IO.blocking(
          restartedDatabase
            .getCollection("analytics_report_snapshots")
            .find(new Document("_id", "current"))
            .first()
        )
      } yield (resumedClaim, request, completion, control, snapshot)

      val (resumedClaim, request, completion, control, snapshot) = result.unsafeRunSync()
      assert(resumedClaim.leaseToken.nonEmpty)
      assertEquals(request.getString("state"), "Complete")
      assertEquals(request.getString("phase"), ErasurePhase.ReportPublished.toString)
      assertEquals(completion.getString("receiptId"), receiptId)
      assertEquals(control.getString("state"), "Published")
      assertEquals(snapshot.getLong("generation"), Long.box(1L))
      assertEquals(snapshot.getString("runId"), runId)
    } finally {
      if (spark != null) spark.stop()
      if (restartedClient != null) restartedClient.close()
      if (client != null) client.close()
      container.stop()
    }
  }

  test("a failed publisher fence leaves deletion pending without a barrier or outbox purge") {
    val container = replicaSet()
    val client: MongoClient = MongoClients.create(
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    )
    try {
      val database = client.getDatabase(s"analytics_fence_failure_${UUID.randomUUID()}")
      val requestId = UUID.randomUUID().toString
      val transactionalId = "hiring-publisher-" + UUID.randomUUID().toString
      val now = Instant.now()
      database
        .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("transactionalIds", java.util.List.of(transactionalId))
            .append("requestedAt", Date.from(now))
        )
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("state", "Retryable")
            .append("subjectIds", java.util.List.of(requestId))
            .append("subjectRefsVersion", 1)
        )
      val store = new MongoAnalyticsErasureWorkerStore(client, database)
      val failedFencer = new TransactionalProducerFencer {
        override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
          IO.raiseError(new IllegalStateException("simulated broker fencing failure"))
      }
      val worker = new AnalyticsErasureWorker(
        null,
        database,
        store,
        KafkaConnection("unused:9092"),
        KafkaConnection("unused:9092"),
        "hiring.operational-events",
        AnalyticsLakehousePaths(Files.createTempDirectory("analytics-fence-failure").toUri.toString),
        AnalyticsTestSubjectPseudonymizer.fromSecret("analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8")),
        new MongoAnalyticsReportPublisher(client, database),
        producerFencer = failedFencer
      )
      val reservation = AnalyticsReportReservation("erasure-" + requestId, "range-fingerprint", 1L, 1L)

      val result = for {
        claim <- store.claim(now, now.plusSeconds(60L), 1).map(_.head)
        failed <- worker.process(claim, reservation).attempt
        barrier <- store.readBarrier(requestId)
        request <- IO.blocking(
          database
            .getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
            .find(new Document("_id", requestId))
            .first()
        )
        outboxCount <- IO.blocking(database.getCollection("event_outbox").countDocuments())
      } yield (claim, failed, barrier, request, outboxCount)

      val (claim, failed, barrier, request, outboxCount) = result.unsafeRunSync()
      assertEquals(claim.phase, ErasurePhase.Requested)
      assert(failed.swap.toOption.exists(_.getMessage == "simulated broker fencing failure"))
      assertEquals(barrier, None)
      assertEquals(request.getString("state"), "Processing")
      assertEquals(request.getString("phase"), null)
      assertEquals(outboxCount, 1L)
    } finally {
      client.close()
      container.stop()
    }
  }

  test("repair-required erasure stays pending and can be requeued only against observed attempt and lease state") {
    val container = replicaSet()
    val client = MongoClients.create(
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    )
    try {
      val database = client.getDatabase(s"analytics_erasure_repair_${UUID.randomUUID()}")
      val collection = database.getCollection(MongoAnalyticsErasureWorkerStore.RequestCollection)
      val requestId = UUID.randomUUID().toString
      val now = Instant.now()
      collection.insertOne(
        new Document("_id", requestId)
          .append("state", "Pending")
          .append("fencingVersion", 1)
          .append("transactionalIds", java.util.List.of())
          .append("requestedAt", Date.from(now))
      )
      val store = new MongoAnalyticsErasureWorkerStore(client, database)
      val claim = store.claim(now, now.plusSeconds(60L), 1).unsafeRunSync().head
      val saved = store
        .recordFailure(claim, ErasureFailureCategory.InvalidState, 1, None, now.plusMillis(1L))
        .unsafeRunSync()
      assert(saved)
      val repair = store.inspectRepairRequests(10).unsafeRunSync().head
      assertEquals(repair.requestId, requestId)
      assertEquals(repair.phase, ErasurePhase.Requested.persistedName)
      assertEquals(repair.attemptCount, 1)
      assertEquals(repair.failureCategory, ErasureFailureCategory.InvalidState.persistedName)
      assertEquals(store.claim(now.plusSeconds(2L), now.plusSeconds(62L), 1).unsafeRunSync(), Vector.empty)
      assert(!store.requeueRepair(requestId, 2, now.plusSeconds(3L)).unsafeRunSync())
      assert(store.requeueRepair(requestId, 1, now.plusSeconds(3L)).unsafeRunSync())
      val resumed = store.claim(now.plusSeconds(4L), now.plusSeconds(64L), 1).unsafeRunSync().head
      assertEquals(resumed.phase, ErasurePhase.Requested)
      assertEquals(resumed.progress, 0)
      assertEquals(resumed.attemptCount, 1)
      val persisted = collection.find(new Document("_id", requestId)).first()
      assertEquals(persisted.getString("state"), "Processing")
      assertEquals(persisted.getString("failureCategory"), null)
      assertEquals(persisted.getBoolean("repairRequired"), Boolean.box(false))
    } finally {
      client.close()
      container.stop()
    }
  }
}
