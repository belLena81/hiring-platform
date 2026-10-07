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

class MongoAnalyticsErasureAdaptersIntegrationSpec extends munit.FunSuite {
  private def assert(condition: Boolean, clue: => Any): Unit =
    if (!condition) throw new AssertionError(clue.toString)

  private def asAccountSubjectId(value: String): AccountSubjectId = AccountSubjectId.from(value).toOption.get
  override val munitTimeout: FiniteDuration = 5.minutes

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(image))

  private val mongo4catsBySync = new java.util.IdentityHashMap[MongoClient, mongo4cats.client.MongoClient[IO]]()
  private def syncClient(uri: String): MongoClient = {
    val sync = MongoClients.create(uri)
    mongo4catsBySync.put(sync, AnalyticsMongo4catsTestSupport.client(uri))
    sync
  }
  private def syncClient(settings: MongoClientSettings, uri: String): MongoClient = {
    val sync = MongoClients.create(settings)
    mongo4catsBySync.put(sync, AnalyticsMongo4catsTestSupport.client(settings))
    sync
  }
  private def mongo4catsClient(sync: MongoClient): mongo4cats.client.MongoClient[IO] = mongo4catsBySync.get(sync)
  private def mongo4catsDatabase(sync: MongoClient, name: String): mongo4cats.database.MongoDatabase[IO] =
    AnalyticsMongo4catsTestSupport.database(mongo4catsClient(sync), name)
  override def afterEach(context: AfterEach): Unit = {
    val clients = mongo4catsBySync.values().iterator()
    while (clients.hasNext) AnalyticsMongo4catsTestSupport.close(clients.next())
    mongo4catsBySync.clear()
    super.afterEach(context)
  }

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
      client = syncClient(settings, connectionString)
      val database = client.getDatabase(databaseName)
      val requestId = UUID.randomUUID().toString
      val requestedAt = Instant.parse("2026-01-01T00:00:00Z")
      database
        .getCollection(AnalyticsCollections.ErasureRequests)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("producerRegistry", true)
            .append("requestedAt", Date.from(requestedAt))
        )
      database
        .getCollection("producer_registrations")
        .insertOne(
          new Document("_id", s"$requestId:hiring-publisher-test")
            .append("subjectId", requestId)
            .append("transactionalId", "hiring-publisher-test")
            .append("kind", "Operational")
            .append("state", "Active")
            .append("registeredAt", Date.from(requestedAt))
        )
      val store =
        AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, databaseName))
      val barrier = KafkaRetentionBarrier
        .from("hiring.operational-events", Vector(0 -> 21L, 1 -> 14L))
        .toOption
        .get
      val fileEvidence = Vector("file:/analytics/silver/part-00001.parquet", "file:/analytics/gold/part-00003.parquet")
      val inFlightFileEvidence = "file:/analytics/quarantine/part-00004.parquet"
      val staleWorkerNow = requestedAt.plusSeconds(9)

      val result = for {
        initial <- store.queue.claim(requestedAt, requestedAt.plusSeconds(10), 1).map(_.head)
        transactionalIds <- store.queue.transactionalIds(asAccountSubjectId(requestId))
        _ = assertEquals(transactionalIds, Vector("hiring-publisher-test"))
        _ <- store.progress.advance(initial, ErasurePhase.PublisherDrained, 0, requestedAt.plusSeconds(1))
        publisherDrained = initial.copy(
          phase = ErasurePhase.PublisherDrained,
          progress = 0,
          progressKey = ErasurePhase.PublisherDrained.ordinal.toLong * ErasurePhase.ProgressPerPhase
        )
        _ <- store.barrier.persistBarrier(publisherDrained, barrier, requestedAt.plusSeconds(2))
        _ <- store.progress.persistDeltaFiles(publisherDrained, fileEvidence, requestedAt.plusSeconds(2))
        advanced <- store.progress.advance(
          publisherDrained,
          ErasurePhase.PublisherDrained,
          17,
          requestedAt.plusSeconds(3)
        )
        _ <- IO.raiseWhen(advanced != ErasureUpdate.Applied)(
          new AssertionError("initial worker could not persist its checkpoint")
        )
        checkpoint = publisherDrained.copy(
          phase = ErasurePhase.PublisherDrained,
          progress = 17,
          progressKey = ErasurePhase.PublisherDrained.ordinal.toLong * ErasurePhase.ProgressPerPhase + 17L
        )
        _ <- IO.delay(pauseEvidenceWrite.set(true))
        inFlightWrite <- store.progress
          .persistDeltaFiles(checkpoint, Vector(inFlightFileEvidence), staleWorkerNow)
          .start
        _ <- IO.blocking {
          if (!evidenceWriteStarted.await(90, TimeUnit.SECONDS))
            throw new AssertionError("old worker did not reach the file evidence write")
        }
        _ <- IO.blocking { restartedClient = syncClient(settings, connectionString) }
        restartedDatabase = restartedClient.getDatabase(databaseName)
        restartedStore = AnalyticsErasureWorkerTestSupport.stores(
          mongo4catsClient(restartedClient),
          mongo4catsDatabase(restartedClient, databaseName)
        )
        claimResult <- Deferred[IO, Either[Throwable, Vector[ErasureClaim]]]
        claimFiber <- restartedStore.queue
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
          case Left(_)       =>
            restartedStore.queue.claim(requestedAt.plusSeconds(11), requestedAt.plusSeconds(61), 1).map(_.head)
        }
        _ = assert(!evidencePauseTimedOut.get(), "test synchronization expired before releasing the file write")
        staleBarrier <- store.barrier.persistBarrier(
          checkpoint,
          barrier.copy(topic = AnalyticsTopic.from("wrong-topic").toOption.get),
          staleWorkerNow
        )
        staleFiles <- store.progress.persistDeltaFiles(
          checkpoint,
          Vector("file:/analytics/forged.parquet"),
          staleWorkerNow
        )
        staleDeferred <- store.progress.defer(checkpoint, requestedAt.plusSeconds(40), staleWorkerNow)
        staleAdvanced <- store.progress.advance(checkpoint, ErasurePhase.PublisherDrained, 18, staleWorkerNow)
        barrierReplay <- restartedStore.barrier.persistBarrier(recovered, barrier, requestedAt.plusSeconds(13))
        fileReplay <- restartedStore.progress.persistDeltaFiles(recovered, fileEvidence, requestedAt.plusSeconds(13))
        loadedBarrier <- restartedStore.barrier.readBarrier(asAccountSubjectId(requestId))
        loadedFiles <- restartedStore.progress.readDeltaFiles(asAccountSubjectId(requestId))
        request <- IO.blocking(
          restartedDatabase
            .getCollection(AnalyticsCollections.ErasureRequests)
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
      assertEquals(inFlightEvidenceSaved, ErasureUpdate.Applied)
      assertEquals(staleBarrier, ErasureUpdate.LeaseLost)
      assertEquals(staleFiles, ErasureUpdate.LeaseLost)
      assertEquals(staleDeferred, ErasureUpdate.LeaseLost)
      assertEquals(staleAdvanced, ErasureUpdate.LeaseLost)
      assertEquals(barrierReplay, ErasureUpdate.Applied)
      assertEquals(fileReplay, ErasureUpdate.Applied)
      assertEquals(loadedBarrier, Some(barrier))
      assertEquals(loadedFiles.sorted, (fileEvidence :+ inFlightFileEvidence).sorted)
      assertEquals(request.getString("state"), "Processing")
      assertEquals(request.getString("phase"), ErasurePhase.PublisherDrained.toString)
      assertEquals(request.getInteger("progress"), Int.box(17))
      assertEquals(request.getLong("progressKey"), Long.box(checkpoint.progressKey))
      assertEquals(request.getDate("resumeAfter"), null)
      assertEquals(request.getString("leaseToken"), recovered.leaseToken)
      restartedClient
        .getDatabase(databaseName)
        .getCollection(AnalyticsCollections.ErasureRequests)
        .updateOne(
          new Document("_id", requestId),
          new Document(
            "$set",
            new Document(
              "kafkaRetentionBarrier",
              new Document("topic", "hiring.operational-events")
                .append(
                  "partitions",
                  java.util.List.of(new Document("number", "invalid").append("endOffsetExclusive", 21L))
                )
            )
          )
        )
      assertEquals(
        AnalyticsErasureWorkerTestSupport
          .stores(mongo4catsClient(restartedClient), mongo4catsDatabase(restartedClient, databaseName))
          .barrier
          .readBarrier(asAccountSubjectId(requestId))
          .attempt
          .unsafeRunSync(),
        Left(AnalyticsError.MalformedMarker)
      )
    } finally {
      resumeEvidenceWrite.countDown()
      if (restartedClient != null) restartedClient.close()
      if (client != null) client.close()
      container.stop()
    }
  }

  test("erasure preflight rejects an int registry proof and recovers after restoring its long version") {
    val container = replicaSet()
    val uri = s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    val client = syncClient(uri)
    val name = s"registry_proof_${UUID.randomUUID()}"
    try {
      val database = client.getDatabase(name)
      List(
        AnalyticsCollections.ErasureRequests,
        AnalyticsCollections.ErasureCompletions,
        AnalyticsCollections.ErasureHeartbeats,
        AnalyticsCollections.ReportSnapshots,
        AnalyticsCollections.ReportControl,
        AnalyticsCollections.ReportRuns,
        AnalyticsCollections.EventOutbox,
        AnalyticsCollections.OutboxSubjectFences,
        AnalyticsCollections.Users,
        "producer_registrations"
      ).foreach(database.createCollection)
      val ledger = database.getCollection("hiring_migration_ledger")
      ledger.insertOne(new Document("_id", "003_event_outbox_subject_references").append("state", "Complete"))
      ledger.insertOne(
        new Document("_id", "012_attributable_producer_registrations")
          .append("state", "Complete")
          .append("version", Int.box(1))
      )
      val stores = AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, name))
      assert(stores.queue.preflight.attempt.unsafeRunSync().isLeft, "BSON int proof must fail closed")
      ledger.updateOne(
        new Document("_id", "012_attributable_producer_registrations"),
        new Document("$set", new Document("version", Long.box(1L)))
      )
      assert(stores.queue.preflight.attempt.unsafeRunSync().isRight, "restored BSON long proof must recover")
    } finally {
      client.close()
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
    val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 0L)).toOption.get
    val retentionHasPassed = new AtomicBoolean(false)
    val retention = new KafkaRetention[IO] {
      override def capture(): IO[KafkaRetentionBarrier] = IO.pure(barrier)
      override def retentionPassed(value: KafkaRetentionBarrier): IO[Boolean] =
        IO.pure(retentionHasPassed.get())
    }
    val producerFencer = new TransactionalProducerFencer[IO] {
      override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] = IO.unit
    }
    val lakehouseRoot = Files.createTempDirectory("analytics-erasure-process-restart").toUri.toString.stripSuffix("/")
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot)
    val secret = "analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8")
    var client: MongoClient = null
    var restartedClient: MongoClient = null
    var spark: SparkSession = null
    try {
      client = syncClient(connectionString)
      val database = client.getDatabase(databaseName)
      database
        .getCollection(AnalyticsCollections.ErasureRequests)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("producerRegistry", true)
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
      database.createCollection("producer_registrations")
      database
        .getCollection("hiring_migration_ledger")
        .insertOne(
          new Document("_id", "012_attributable_producer_registrations")
            .append("state", "Complete")
            .append("version", Long.box(1L))
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

      val firstStore =
        AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, databaseName))
      val firstPublisher =
        new MongoAnalyticsReportPublisher[IO](
          mongo4catsClient(client),
          mongo4catsDatabase(client, databaseName),
          operational = AnalyticsTestOperationalConfig.operational
        )
      val firstWorker = AnalyticsErasureWorkerTestSupport.worker(
        spark,
        mongo4catsDatabase(client, databaseName),
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
        kafkaRetention = Some(retention)
      )
      val runId = AnalyticsErasureWorkerTestSupport.runId("analytics-erasure-" + requestId)
      val rangeFingerprint = AnalyticsErasureWorkerTestSupport.fingerprint("erasure:" + requestId)

      val result = for {
        initialClaim <- firstStore.queue.claim(startedAt, startedAt.plusSeconds(60L * 86400L), 1).map(_.head)
        reservation <- firstPublisher.reserve(runId, rangeFingerprint, startedAt)
        firstAttempt <- firstWorker.process(initialClaim, reservation)
        checkpoint <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        _ = assertEquals(checkpoint.getString("phase"), ErasurePhase.DeltaPurged.toString)
        _ = assert(checkpoint.getDate("resumeAfter").toInstant.isAfter(startedAt), "assertion failed")
        _ = assertEquals(firstAttempt, ErasureClaimOutcome.Deferred)
        _ <- IO.blocking {
          client.close()
          client = null
          restartedClient = syncClient(connectionString)
        }
        restartedDatabase = restartedClient.getDatabase(databaseName)
        restartedStore = AnalyticsErasureWorkerTestSupport.stores(
          mongo4catsClient(restartedClient),
          mongo4catsDatabase(restartedClient, databaseName)
        )
        restartedPublisher = new MongoAnalyticsReportPublisher[IO](
          mongo4catsClient(restartedClient),
          mongo4catsDatabase(restartedClient, databaseName),
          operational = AnalyticsTestOperationalConfig.operational
        )
        _ <- IO.delay(retentionHasPassed.set(true))
        resumedClaim <- restartedStore.queue.claim(afterRetention, afterRetention.plusSeconds(86400L), 1).map(_.head)
        _ = assertEquals(resumedClaim.phase, ErasurePhase.DeltaPurged)
        resumedReservation <- restartedPublisher.reserve(runId, rangeFingerprint, afterRetention)
        restartedWorker = AnalyticsErasureWorkerTestSupport.worker(
          spark,
          mongo4catsDatabase(restartedClient, databaseName),
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
          kafkaRetention = Some(retention)
        )
        resumedOutcome <- restartedWorker.process(resumedClaim, resumedReservation)
        _ = assertEquals(resumedOutcome, ErasureClaimOutcome.Completed)
        readyToPublish <- IO.blocking(
          restartedDatabase
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        _ = assertEquals(readyToPublish.getString("phase"), ErasurePhase.ReadyToPublish.toString)
        _ <- IO.blocking(
          restartedDatabase
            .getCollection(AnalyticsCollections.ErasureRequests)
            .updateOne(
              new Document("_id", requestId),
              new Document("$set", new Document("leaseUntil", Date.from(afterRetention.minusSeconds(1L))))
            )
        )
        finalizerClaim <- restartedStore.queue.claim(afterRetention, afterRetention.plusSeconds(86400L), 1).map(_.head)
        _ = assertEquals(finalizerClaim.phase, ErasurePhase.ReadyToPublish)
        finalizerReservation <- restartedPublisher.reserve(runId, rangeFingerprint, afterRetention)
        finalizerWorker = AnalyticsErasureWorkerTestSupport.worker(
          spark,
          mongo4catsDatabase(restartedClient, databaseName),
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
          kafkaRetention = Some(retention)
        )
        finalizerOutcome <- finalizerWorker.process(finalizerClaim, finalizerReservation)
        _ = assertEquals(finalizerOutcome, ErasureClaimOutcome.Completed)
        request <- IO.blocking(
          restartedDatabase
            .getCollection(AnalyticsCollections.ErasureRequests)
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
      assert(resumedClaim.leaseToken.nonEmpty, "assertion failed")
      assertEquals(request.getString("state"), "Complete")
      assertEquals(request.getString("phase"), ErasurePhase.ReportPublished.toString)
      assertEquals(completion.getString("receiptId"), receiptId)
      assertEquals(control.getString("state"), "Published")
      assertEquals(snapshot.getLong("generation"), Long.box(1L))
      assertEquals(snapshot.getString("runId"), runId.value)
    } finally {
      if (spark != null) spark.stop()
      if (restartedClient != null) restartedClient.close()
      if (client != null) client.close()
      container.stop()
    }
  }

  test("a failed publisher fence leaves deletion pending without a barrier or outbox purge") {
    val container = replicaSet()
    val client: MongoClient = syncClient(
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    )
    try {
      val database = client.getDatabase(s"analytics_fence_failure_${UUID.randomUUID()}")
      val requestId = UUID.randomUUID().toString
      val transactionalId = "hiring-publisher-" + UUID.randomUUID().toString
      val now = Instant.now()
      database
        .getCollection(AnalyticsCollections.ErasureRequests)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("producerRegistry", true)
            .append("requestedAt", Date.from(now))
        )
      database
        .getCollection("producer_registrations")
        .insertOne(
          new Document("_id", s"$requestId:$transactionalId")
            .append("subjectId", requestId)
            .append("transactionalId", transactionalId)
            .append("kind", "Operational")
            .append("state", "Active")
            .append("registeredAt", Date.from(now))
        )
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("state", "Retryable")
            .append("subjectIds", java.util.List.of(requestId))
            .append("subjectRefsVersion", 1)
        )
      val store =
        AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, database.getName))
      val failedFencer = new TransactionalProducerFencer[IO] {
        override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
          IO.raiseError(new IllegalStateException("simulated broker fencing failure"))
      }
      val worker = AnalyticsErasureWorkerTestSupport.worker(
        null,
        mongo4catsDatabase(client, database.getName),
        store,
        KafkaConnection("unused:9092"),
        KafkaConnection("unused:9092"),
        "hiring.operational-events",
        IntegrationAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-fence-failure").toUri.toString),
        AnalyticsTestSubjectPseudonymizer.fromSecret("analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8")),
        new AnalyticsReportPublisher[IO] {
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
            IO.pure(AnalyticsReportReservation(runId, rangeFingerprint, 1L, 1L))
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
        },
        producerFencer = failedFencer
      )
      val result = for {
        claim <- store.queue.claim(now, now.plusSeconds(60L), 1).map(_.head)
        outcome <- worker.runClaim(claim)
        barrier <- store.barrier.readBarrier(asAccountSubjectId(requestId))
        request <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        outboxCount <- IO.blocking(database.getCollection("event_outbox").countDocuments())
      } yield (claim, outcome, barrier, request, outboxCount)

      val (claim, outcome, barrier, request, outboxCount) = result.unsafeRunSync()
      assert(outcome match {
        case ErasureClaimOutcome.Failed(_: Throwable) => true
        case _                                        => false
      })
      assertEquals(claim.phase, ErasurePhase.Requested)
      assertEquals(barrier, None)
      assertEquals(request.getString("state"), "Processing")
      assertEquals(request.getString("phase"), null)
      assertEquals(request.getString("failureCategory"), ErasureFailureCategory.Unknown.persistedName)
      assertEquals(request.getInteger("attemptCount"), Integer.valueOf(1))
      assertEquals(request.getBoolean("repairRequired"), Boolean.box(false))
      assert(request.getDate("resumeAfter").toInstant.isAfter(now), "assertion failed")
      assertEquals(outboxCount, 1L)
    } finally {
      client.close()
      container.stop()
    }
  }

  test("repair-required erasure stays pending and can be requeued only against observed attempt and lease state") {
    val container = replicaSet()
    val client = syncClient(
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    )
    try {
      val database = client.getDatabase(s"analytics_erasure_repair_${UUID.randomUUID()}")
      val collection = database.getCollection(AnalyticsCollections.ErasureRequests)
      val requestId = UUID.randomUUID().toString
      val now = Instant.now()
      collection.insertOne(
        new Document("_id", requestId)
          .append("state", "Pending")
          .append("fencingVersion", 1)
          .append("producerRegistry", true)
          .append("requestedAt", Date.from(now))
      )
      val store =
        AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, database.getName))
      val claim = store.queue.claim(now, now.plusSeconds(60L), 1).unsafeRunSync().head
      val saved = store.progress
        .recordFailure(claim, ErasureFailureCategory.InvalidState, 1, None, now.plusMillis(1L))
        .unsafeRunSync()
      assertEquals(saved, ErasureUpdate.Applied)
      val repair = store.queue.inspectRepairRequests(10).unsafeRunSync().head
      assertEquals(repair.requestId, asAccountSubjectId(requestId))
      assertEquals(repair.phase, ErasurePhase.Requested.persistedName)
      assertEquals(repair.attemptCount, 1)
      assertEquals(repair.failureCategory, ErasureFailureCategory.InvalidState.persistedName)
      assertEquals(store.queue.claim(now.plusSeconds(2L), now.plusSeconds(62L), 1).unsafeRunSync(), Vector.empty)
      assertEquals(
        store.queue.requeueRepair(asAccountSubjectId(requestId), 2, now.plusSeconds(3L)).unsafeRunSync(),
        ErasureUpdate.LeaseLost
      )
      assertEquals(
        store.queue.requeueRepair(asAccountSubjectId(requestId), 1, now.plusSeconds(3L)).unsafeRunSync(),
        ErasureUpdate.Applied
      )
      val resumed = store.queue.claim(now.plusSeconds(4L), now.plusSeconds(64L), 1).unsafeRunSync().head
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

  test("a failure write paused before Mongo cannot overwrite a reclaimed lease or its repair state") {
    val container = replicaSet()
    val connectionString =
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    val databaseName = s"erasure_failure_race_${UUID.randomUUID()}"
    val staleWriteStarted = new CountDownLatch(1)
    val releaseStaleWrite = new CountDownLatch(1)
    val pauseStaleWrite = new AtomicBoolean(true)
    val listener = new CommandListener {
      override def commandStarted(event: CommandStartedEvent): Unit = {
        if (
          event.getDatabaseName == databaseName &&
          event.getCommandName == "update" &&
          event.getCommand.toJson.contains("failureCategory") &&
          pauseStaleWrite.compareAndSet(true, false)
        ) {
          staleWriteStarted.countDown()
          if (!releaseStaleWrite.await(90, TimeUnit.SECONDS))
            throw new AssertionError("stale failure write was not released")
        }
      }
      override def commandSucceeded(event: CommandSucceededEvent): Unit = ()
      override def commandFailed(event: CommandFailedEvent): Unit = ()
    }
    val settings = MongoClientSettings
      .builder()
      .applyConnectionString(new ConnectionString(connectionString))
      .addCommandListener(listener)
      .build()
    val staleClient = syncClient(settings, connectionString)
    val currentClient = syncClient(connectionString)
    try {
      val database = currentClient.getDatabase(databaseName)
      val requestId = UUID.randomUUID().toString
      val now = Instant.now()
      val requests = database.getCollection(AnalyticsCollections.ErasureRequests)
      requests.insertOne(
        new Document("_id", requestId)
          .append("state", "Pending")
          .append("fencingVersion", 1)
          .append("producerRegistry", true)
          .append("requestedAt", Date.from(now))
      )
      val staleStore =
        AnalyticsErasureWorkerTestSupport.stores(
          mongo4catsClient(staleClient),
          mongo4catsDatabase(staleClient, databaseName)
        )
      val currentStore =
        AnalyticsErasureWorkerTestSupport.stores(
          mongo4catsClient(currentClient),
          mongo4catsDatabase(currentClient, databaseName)
        )
      val staleClaim = currentStore.queue.claim(now, now.plusSeconds(1), 1).unsafeRunSync().head
      val result = for {
        delayed <- staleStore.progress
          .recordFailure(
            staleClaim,
            ErasureFailureCategory.Unknown,
            1,
            None,
            now.plusMillis(500)
          )
          .start
        _ <- IO.blocking(assert(staleWriteStarted.await(30, TimeUnit.SECONDS), "assertion failed"))
        currentClaim <- currentStore.queue.claim(now.plusSeconds(2), now.plusSeconds(62), 1).map(_.head)
        staleRequeue <- staleStore.queue.requeueRepair(asAccountSubjectId(requestId), 1, now.plusSeconds(3))
        _ <- IO.blocking(releaseStaleWrite.countDown())
        staleSaved <- delayed.joinWithNever
        currentSaved <- currentStore.progress.recordFailure(
          currentClaim,
          ErasureFailureCategory.InvalidState,
          1,
          None,
          now.plusSeconds(3)
        )
        observed <- IO.blocking(requests.find(new Document("_id", requestId)).first())
      } yield (currentClaim, staleSaved, currentSaved, staleRequeue, observed)
      val (currentClaim, staleSaved, currentSaved, staleRequeue, observed) = result.unsafeRunSync()
      assertNotEquals(currentClaim.leaseToken, staleClaim.leaseToken)
      assertEquals(staleSaved, ErasureUpdate.LeaseLost)
      assertEquals(currentSaved, ErasureUpdate.Applied)
      assertEquals(staleRequeue, ErasureUpdate.LeaseLost)
      assertEquals(observed.getInteger("attemptCount"), Integer.valueOf(1))
      assertEquals(observed.getString("failureCategory"), ErasureFailureCategory.InvalidState.persistedName)
      assertEquals(observed.getBoolean("repairRequired"), Boolean.box(true))
      assertEquals(observed.getString("leaseToken"), null)
    } finally {
      releaseStaleWrite.countDown()
      currentClient.close()
      staleClient.close()
      container.stop()
    }
  }

  test("concurrent repair requeues and worker claims preserve one lease and the durable checkpoint") {
    val container = replicaSet()
    val client = syncClient(
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    )
    try {
      val database = client.getDatabase(s"erasure_repair_race_${UUID.randomUUID()}")
      val collection = database.getCollection(AnalyticsCollections.ErasureRequests)
      val users = database.getCollection("users")
      val reportControl = database.getCollection("analytics_report_control")
      val requestId = UUID.randomUUID().toString
      val now = Instant.now()
      val phase = ErasurePhase.OutboxPurged
      val progress = 37
      val progressKey = phase.ordinal.toLong * ErasurePhase.ProgressPerPhase + progress.toLong
      collection.insertOne(
        new Document("_id", requestId)
          .append("state", "Pending")
          .append("fencingVersion", 1)
          .append("producerRegistry", true)
          .append("requestedAt", Date.from(now))
          .append("phase", phase.persistedName)
          .append("progress", progress)
          .append("progressKey", progressKey)
          .append("attemptCount", 0)
      )
      users.insertOne(new Document("_id", requestId).append("accountStatus", "Deleted"))
      reportControl.insertOne(
        new Document("_id", "analytics-report")
          .append("state", "Hidden")
          .append("generation", 7L)
          .append("lastPublishedRevision", 3L)
      )
      val store =
        AnalyticsErasureWorkerTestSupport.stores(mongo4catsClient(client), mongo4catsDatabase(client, database.getName))
      val result = for {
        initial <- store.queue.claim(now, now.plusSeconds(60L), 1).map(_.head)
        saved <- store.progress.recordFailure(initial, ErasureFailureCategory.InvalidState, 1, None, now.plusMillis(1L))
        _ <- IO.raiseWhen(saved != ErasureUpdate.Applied)(new AssertionError("repair failure was not recorded"))
        _ <- IO.blocking {
          collection.updateOne(
            new Document("_id", requestId),
            new Document(
              "$set",
              new Document("leaseToken", "competing-live-lease").append("leaseUntil", Date.from(now.plusSeconds(30L)))
            )
          )
        }
        liveLeaseRequeue <- store.queue.requeueRepair(asAccountSubjectId(requestId), 1, now.plusSeconds(2L))
        _ = assertEquals(liveLeaseRequeue, ErasureUpdate.LeaseLost)
        _ <- IO.blocking {
          collection.updateOne(
            new Document("_id", requestId),
            new Document("$unset", new Document("leaseToken", "").append("leaseUntil", ""))
          )
        }
        gate <- Deferred[IO, Unit]
        firstReady <- Deferred[IO, Unit]
        secondReady <- Deferred[IO, Unit]
        first <- (firstReady.complete(()) *> gate.get *>
          store.queue.requeueRepair(asAccountSubjectId(requestId), 1, now.plusSeconds(3L))).start
        second <- (secondReady.complete(()) *> gate.get *>
          store.queue.requeueRepair(asAccountSubjectId(requestId), 1, now.plusSeconds(3L))).start
        _ <- firstReady.get *> secondReady.get *> gate.complete(())
        firstResult <- first.joinWithNever
        secondResult <- second.joinWithNever
        _ = assertEquals(Vector(firstResult, secondResult).count(_ == ErasureUpdate.Applied), 1)
        retryClaim <- store.queue.claim(now.plusSeconds(4L), now.plusSeconds(64L), 1).map(_.head)
        secondFailure <- store.progress.recordFailure(
          retryClaim,
          ErasureFailureCategory.InvalidState,
          2,
          None,
          now.plusSeconds(5L)
        )
        _ <- IO.raiseWhen(secondFailure != ErasureUpdate.Applied)(
          new AssertionError("second repair failure was not recorded")
        )
        claimGate <- Deferred[IO, Unit]
        claimReady <- Deferred[IO, Unit]
        requeueReady <- Deferred[IO, Unit]
        claimFiber <- (claimReady.complete(()) *> claimGate.get *>
          store.queue.claim(now.plusSeconds(6L), now.plusSeconds(66L), 1)).start
        requeueFiber <- (requeueReady.complete(()) *> claimGate.get *>
          store.queue.requeueRepair(asAccountSubjectId(requestId), 2, now.plusSeconds(6L))).start
        _ <- claimReady.get *> requeueReady.get *> claimGate.complete(())
        racingClaims <- claimFiber.joinWithNever
        racingRequeue <- requeueFiber.joinWithNever
        laterClaims <- store.queue.claim(now.plusSeconds(7L), now.plusSeconds(67L), 1)
        persisted <- IO.blocking(collection.find(new Document("_id", requestId)).first())
        tombstone <- IO.blocking(users.find(new Document("_id", requestId)).first())
        reportState <- IO.blocking(reportControl.find(new Document("_id", "analytics-report")).first())
      } yield (initial, racingClaims, racingRequeue, laterClaims, persisted, tombstone, reportState)

      val (initial, racingClaims, racingRequeue, laterClaims, persisted, tombstone, reportState) =
        result.unsafeRunSync()
      assertEquals(initial.phase, phase)
      assertEquals(initial.progress, progress)
      assertEquals(racingRequeue, ErasureUpdate.Applied)
      assertEquals(racingClaims.size + laterClaims.size, 1)
      val owner = (racingClaims ++ laterClaims).head
      assertEquals(owner.phase, phase)
      assertEquals(owner.progress, progress)
      assertEquals(owner.progressKey, progressKey)
      assertEquals(owner.attemptCount, 2)
      assertEquals(persisted.getString("state"), "Processing")
      assertEquals(persisted.getString("leaseToken"), owner.leaseToken)
      assertEquals(persisted.getString("phase"), phase.persistedName)
      assertEquals(persisted.getInteger("progress"), Int.box(progress))
      assertEquals(persisted.getLong("progressKey"), Long.box(progressKey))
      assertEquals(persisted.getInteger("attemptCount"), Int.box(2))
      assertEquals(persisted.getBoolean("repairRequired"), Boolean.box(false))
      assertEquals(persisted.getString("failureCategory"), null)
      assertEquals(tombstone.getString("accountStatus"), "Deleted")
      assertEquals(reportState.getString("state"), "Hidden")
      assertEquals(reportState.getLong("generation"), Long.box(7L))
      assertEquals(reportState.getLong("lastPublishedRevision"), Long.box(3L))
    } finally {
      client.close()
      container.stop()
    }
  }

  test("typed HMAC authorization and preparation stores round trip derived records") {
    val container = replicaSet()
    val connectionString =
      s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    val client = syncClient(connectionString)
    val databaseName = s"analytics_hmac_records_${UUID.randomUUID()}"
    val database = mongo4catsDatabase(client, databaseName)
    val root = s"s3a://analytics-test/${UUID.randomUUID()}"
    val lakehouseId = MongoAnalyticsLakehouseLock.lockId(root).toOption.get
    val facts = "operator-confirmed retention horizons and writer exclusion"
    val authorization = HmacKeyRetirementAuthorization(
      lakehouseId,
      "retiring-key",
      "v" * 43,
      facts,
      HmacKeyRetirementAuthorization.digest(facts),
      Instant.parse("2026-09-27T12:00:00Z")
    )
    val volumeName = "unit_hmac-rotation-kafka"
    val lineage = HmacKeyRetirementKafkaLineage(
      "cluster-id",
      "topic-id",
      volumeName,
      s"/var/lib/docker/volumes/$volumeName/_data",
      "2026-09-27T12:00:00Z",
      "127.0.0.1:19093"
    )
    val preparation = HmacKeyRetirementPreparation(
      lakehouseId,
      "retiring-key",
      "v" * 43,
      Instant.parse("2026-09-27T12:00:00Z"),
      KafkaRetentionBarrier.from("hiring.phase6.runtime", Vector(0 -> 10L)).toOption.get,
      lineage
    )

    try {
      val authStore = new MongoHmacKeyRetirementAuthorizationStore[IO](database, AnalyticsTestOperationalConfig.streams)
      val preparationStore =
        new MongoHmacKeyRetirementPreparationStore[IO](database, AnalyticsTestOperationalConfig.streams)
      val listed = for {
        _ <- authStore.insert(root, authorization)
        _ <- preparationStore.insert(root, preparation)
        authorizations <- authStore.list(root)
        prepared <- preparationStore.read(root, "retiring-key")
      } yield authorizations -> prepared
      val (authorizations, prepared) = listed.unsafeRunSync()

      assertEquals(authorizations, Vector(authorization))
      assertEquals(prepared, Some(preparation))
      val persisted = client
        .getDatabase(databaseName)
        .getCollection("analytics_hmac_key_retirement_preparations")
        .find(new Document("_id", s"$lakehouseId:retiring-key"))
        .first()
      assert(
        persisted.getList("partitions", classOf[Document]).get(0).get("endOffsetExclusive").isInstanceOf[java.lang.Long]
      )
    } finally {
      client.close()
      container.stop()
    }
  }
}
