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
import cats.effect.syntax.all.*
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients}
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{
  AuthenticationException,
  AuthorizationException,
  InvalidProducerEpochException,
  ProducerFencedException,
  TopicAuthorizationException
}
import org.apache.kafka.common.serialization.StringSerializer
import org.bson.Document
import munit.FunSuite
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Instant
import java.time.Duration
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Opt-in authorization proof against the authenticated local Compose Kafka broker. */
class KafkaPublisherAccessIntegrationSpec extends FunSuite {
  private def assert(condition: Boolean, clue: => Any): Unit =
    if (!condition) throw new AssertionError(clue.toString)

  override val munitTimeout = scala.concurrent.duration.FiniteDuration(3, scala.concurrent.duration.MINUTES)

  private val enabled = sys.props.get("hiring.analytics.compose.acl-evidence").contains("true") ||
    sys.env.get("HIRING_ANALYTICS_COMPOSE_ACL_EVIDENCE").contains("true")
  private val transportRecoveryEnabled =
    sys.props.get("hiring.analytics.compose.transport-recovery").contains("true") ||
      sys.env.get("HIRING_ANALYTICS_COMPOSE_TRANSPORT_RECOVERY").contains("true")
  private val adminOutageRecoveryEnabled =
    sys.props.get("hiring.analytics.compose.admin-outage-recovery").contains("true") ||
      sys.env.get("HIRING_ANALYTICS_COMPOSE_ADMIN_OUTAGE_RECOVERY").contains("true")
  private val bootstrapServers = sys.props.getOrElse(
    "hiring.analytics.compose.kafka",
    sys.env.getOrElse("HIRING_ANALYTICS_COMPOSE_KAFKA", "127.0.0.1:9092")
  )
  private val topic = sys.env.getOrElse("ANALYTICS_TOPIC", "hiring.operational-events")

  private def properties(username: String, password: String, transactionalId: Option[String] = None): Properties = {
    val result = new Properties()
    result.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    result.put(ProducerConfig.ACKS_CONFIG, "all")
    transactionalId.foreach(result.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, _))
    result.put("security.protocol", "SASL_PLAINTEXT")
    result.put("sasl.mechanism", "PLAIN")
    result.put(
      "sasl.jaas.config",
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$username\" password=\"$password\";"
    )
    result
  }

  private def hasTopicAuthorizationFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[TopicAuthorizationException])

  private def hasProducerFencingFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      case _: ProducerFencedException       => true
      case _: InvalidProducerEpochException => true
      case _                                => false
    }

  private def hasKafkaAuthorizationFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[AuthorizationException])

  private def hasAuthenticationFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists(_.isInstanceOf[AuthenticationException])

  private def hasTransportFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists { cause =>
      cause.isInstanceOf[java.net.ConnectException] ||
      cause.isInstanceOf[org.apache.kafka.common.errors.NetworkException] ||
      cause.isInstanceOf[org.apache.kafka.common.errors.TimeoutException]
    }

  private def verifyWorkerKeepsRequestPendingAfterBadFencerPassword(fencerPassword: String): Unit = {
    val mongoUri = sys.env.getOrElse(
      "HIRING_ANALYTICS_COMPOSE_MONGO_URI",
      "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
    )
    val client: MongoClient = MongoClients.create(mongoUri)
    val databaseName = "analytics_fencer_auth_" + UUID.randomUUID().toString.replace('-', '_')
    val database = client.getDatabase(databaseName)
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(mongoUri)
    val reactiveDatabase = AnalyticsMongo4catsTestSupport.database(reactiveClient, databaseName)
    val requestId = UUID.randomUUID().toString
    val transactionalId = "hiring-publisher-invalid-fencer-" + UUID.randomUUID().toString
    val requestedAt = Instant.now()
    try {
      database
        .getCollection(AnalyticsCollections.ErasureRequests)
        .insertOne(
          new Document("_id", requestId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("transactionalIds", java.util.List.of(transactionalId))
            .append("requestedAt", java.util.Date.from(requestedAt))
        )
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("state", "Retryable")
            .append("subjectIds", java.util.List.of(requestId))
            .append("subjectRefsVersion", 1)
        )
      database
        .getCollection("outbox_subject_fences")
        .insertOne(
          new Document("_id", requestId).append("deleted", true)
        )
      val store = AnalyticsErasureWorkerTestSupport.stores(reactiveClient, reactiveDatabase)
      val worker = AnalyticsErasureWorkerTestSupport.worker(
        null,
        reactiveDatabase,
        store,
        KafkaConnection(bootstrapServers, Some("analytics_reader"), Some(sys.env("KAFKA_READER_PASSWORD"))),
        KafkaConnection(bootstrapServers, Some("analytics_fencer"), Some(fencerPassword + "-invalid")),
        topic,
        IntegrationAnalyticsLakehousePaths.unsafe("file:///tmp/analytics-fencer-auth-" + UUID.randomUUID().toString),
        AnalyticsTestSubjectPseudonymizer.fromSecret("worker-auth-test-secret".padTo(32, 'x').getBytes("UTF-8")),
        new MongoAnalyticsReportPublisher[IO](
          reactiveClient,
          reactiveDatabase,
          operational = AnalyticsTestOperationalConfig.operational
        )
      )
      val result = (for {
        claim <- store.queue.claim(requestedAt, requestedAt.plusSeconds(60L), 1).map(_.head)
        reservation = AnalyticsReportReservation(
          AnalyticsErasureWorkerTestSupport.runId("worker-auth-" + requestId),
          AnalyticsErasureWorkerTestSupport.fingerprint("worker-auth-range"),
          1L,
          1L
        )
        failure <- worker.process(claim, reservation).attempt
        barrier <- store.barrier.readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId))
        request <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        outboxCount <- IO.blocking(database.getCollection("event_outbox").countDocuments())
        _ <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .updateOne(
              new Document("_id", requestId),
              new Document("$set", new Document("leaseUntil", java.util.Date.from(requestedAt.minusSeconds(1L))))
            )
        )
        reclaimAt <- IO.realTimeInstant.map(_.plusSeconds(1L))
        reclaimed <- store.queue.claim(reclaimAt, reclaimAt.plusSeconds(60L), 1).map(_.head)
        uncertainFencingCompleted = new AtomicBoolean(false)
        uncertainFencer = new TransactionalProducerFencer[IO] {
          override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
            KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
              .fence(connection, transactionalIds)
              .flatMap { _ =>
                IO.delay(uncertainFencingCompleted.set(true)) *> IO.raiseError(
                  new java.util.concurrent.TimeoutException("simulated client timeout after broker accepted fencing")
                )
              }
        }
        uncertainWorker = AnalyticsErasureWorkerTestSupport.worker(
          null,
          reactiveDatabase,
          store,
          KafkaConnection(bootstrapServers, Some("analytics_reader"), Some(sys.env("KAFKA_READER_PASSWORD"))),
          KafkaConnection(bootstrapServers, Some("analytics_fencer"), Some(fencerPassword)),
          topic,
          IntegrationAnalyticsLakehousePaths.unsafe(
            "file:///tmp/analytics-fencer-auth-uncertain-" + UUID.randomUUID().toString
          ),
          AnalyticsTestSubjectPseudonymizer.fromSecret("worker-auth-test-secret".padTo(32, 'x').getBytes("UTF-8")),
          new MongoAnalyticsReportPublisher[IO](
            reactiveClient,
            reactiveDatabase,
            operational = AnalyticsTestOperationalConfig.operational
          ),
          producerFencer = uncertainFencer
        )
        uncertainFailure <- uncertainWorker.process(reclaimed, reservation).attempt
        uncertainRequest <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        uncertainBarrier <- store.barrier.readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId))
        uncertainOutboxCount <- IO.blocking(database.getCollection("event_outbox").countDocuments())
        _ <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .updateOne(
              new Document("_id", requestId),
              new Document("$set", new Document("leaseUntil", java.util.Date.from(requestedAt.minusSeconds(1L))))
            )
        )
        retryAt <- IO.realTimeInstant.map(_.plusSeconds(1L))
        retryClaim <- store.queue.claim(retryAt, retryAt.plusSeconds(60L), 1).map(_.head)
        retryFenced = new AtomicBoolean(false)
        correctFencer = new TransactionalProducerFencer[IO] {
          override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
            KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
              .fence(connection, transactionalIds)
              .flatTap(_ => IO.delay(retryFenced.set(true)))
        }
        retryWorker = AnalyticsErasureWorkerTestSupport.worker(
          null,
          reactiveDatabase,
          store,
          KafkaConnection(bootstrapServers, Some("analytics_reader"), Some(sys.env("KAFKA_READER_PASSWORD"))),
          KafkaConnection(bootstrapServers, Some("analytics_fencer"), Some(fencerPassword)),
          topic,
          IntegrationAnalyticsLakehousePaths.unsafe(
            "file:///tmp/analytics-fencer-auth-retry-" + UUID.randomUUID().toString
          ),
          AnalyticsTestSubjectPseudonymizer.fromSecret("worker-auth-test-secret".padTo(32, 'x').getBytes("UTF-8")),
          new MongoAnalyticsReportPublisher[IO](
            reactiveClient,
            reactiveDatabase,
            operational = AnalyticsTestOperationalConfig.operational
          ),
          producerFencer = correctFencer
        )
        retryFailure <- retryWorker.process(retryClaim, reservation).attempt
        requestAfterRetry <- IO.blocking(
          database
            .getCollection(AnalyticsCollections.ErasureRequests)
            .find(new Document("_id", requestId))
            .first()
        )
        barrierAfterRetry <- store.barrier.readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId))
        outboxAfterRetry <- IO.blocking(database.getCollection("event_outbox").countDocuments())
      } yield (
        failure,
        barrier,
        request,
        outboxCount,
        reclaimed,
        uncertainFencingCompleted.get(),
        uncertainFailure,
        uncertainRequest,
        uncertainBarrier,
        uncertainOutboxCount,
        retryClaim,
        retryFenced.get(),
        retryFailure,
        requestAfterRetry,
        barrierAfterRetry,
        outboxAfterRetry
      )).unsafeRunSync()

      assert(result._1.swap.toOption.exists(hasAuthenticationFailure), clues(result._1))
      assertEquals(result._2, None)
      assertEquals(result._3.getString("state"), "Processing")
      assertEquals(result._3.getString("phase"), null)
      assertEquals(result._4, 1L)
      assertNotEquals(result._5.leaseToken, null)
      assert(result._6, "the broker did not accept fencing before the injected uncertain result")
      assert(result._7.swap.toOption.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]), clues(result._7))
      assertEquals(result._8.getString("state"), "Processing")
      assertEquals(result._8.getString("phase"), null)
      assertEquals(result._9, None)
      assertEquals(result._10, 1L)
      assertNotEquals(result._11.leaseToken, null)
      assert(result._12, "the reclaimed worker did not fence with corrected credentials")
      assert(
        result._13.swap.toOption.exists(_.getMessage.contains("outbox subject-reference migration is incomplete")),
        clues(result._13)
      )
      assertEquals(result._14.getString("phase"), "PublisherDrained")
      assertEquals(result._15, None)
      assertEquals(result._16, 1L)
    } finally {
      try database.drop()
      finally {
        AnalyticsMongo4catsTestSupport.close(reactiveClient)
        client.close()
      }
    }
  }

  private def verifyAutomaticPollRecovery(
      fencerPassword: String,
      transportFailure: Boolean,
      workerBootstrapServers: String = bootstrapServers,
      interruptedBroker: Option[KafkaContainer] = None,
      registeredTransactionalId: Option[String] = None
  ): Unit = {
    val mongoUri = sys.env.getOrElse(
      "HIRING_ANALYTICS_COMPOSE_MONGO_URI",
      "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
    )
    val client = MongoClients.create(mongoUri)
    val databaseName = "analytics_poll_recovery_" + UUID.randomUUID().toString.replace('-', '_')
    val database = client.getDatabase(databaseName)
    val reactiveClient = AnalyticsMongo4catsTestSupport.client(mongoUri)
    val reactiveDatabase = AnalyticsMongo4catsTestSupport.database(reactiveClient, databaseName)
    val requestId = UUID.randomUUID().toString
    val receiptId = UUID.randomUUID().toString
    val transactionalId =
      registeredTransactionalId.getOrElse("hiring-publisher-poll-recovery-" + UUID.randomUUID().toString)
    val requestedAt = Instant.now()
    try {
      database
        .getCollection(AnalyticsCollections.ErasureRequests)
        .insertOne(
          new Document("_id", requestId)
            .append("receiptId", receiptId)
            .append("state", "Pending")
            .append("fencingVersion", 1)
            .append("transactionalIds", java.util.List.of(transactionalId))
            .append("requestedAt", java.util.Date.from(requestedAt))
        )
      database
        .getCollection("outbox_subject_fences")
        .insertOne(new Document("_id", requestId).append("deleted", true))
      database
        .getCollection("event_outbox")
        .insertOne(new Document("_id", UUID.randomUUID().toString).append("subjectIds", java.util.List.of(requestId)))

      val store = AnalyticsErasureWorkerTestSupport.stores(reactiveClient, reactiveDatabase)
      val attempts = new AtomicInteger(0)
      val firstFailure = new AtomicReference[Throwable](null)
      val pendingWasObservedDuringOutage = new AtomicBoolean(false)
      val unavailablePort = {
        val socket = new ServerSocket(0)
        try socket.getLocalPort
        finally socket.close()
      }
      val recoveringFencer = new TransactionalProducerFencer[IO] {
        override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
          if (attempts.incrementAndGet() == 1) {
            val firstAttempt =
              interruptedBroker match {
                case Some(broker) =>
                  KafkaProducerFencer.fenceAfterSubmission[IO](
                    connection,
                    transactionalIds,
                    AnalyticsBatchTestSupport.driverExecution
                  ) { future =>
                    IO.raiseWhen(future.isDone)(
                      new AssertionError("AdminClient fencing completed before broker outage")
                    ) *>
                      IO.blocking(broker.stop()) *> IO.blocking {
                        val request = database
                          .getCollection(AnalyticsCollections.ErasureRequests)
                          .find(new Document("_id", requestId))
                          .first()
                        val phase = Option(request).flatMap(value => Option(value.getString("phase")))
                        val hasBarrier =
                          request != null && request.get("kafkaRetentionBarrier", classOf[Document]) != null
                        val stillPending =
                          request != null && request.getString("state") == "Processing" &&
                            phase.forall(_ == ErasurePhase.Requested.toString) &&
                            database.getCollection("event_outbox").countDocuments() == 1L &&
                            !hasBarrier
                        pendingWasObservedDuringOutage.set(stillPending)
                        assert(
                          stillPending,
                          "the request or outbox advanced while the AdminClient request was interrupted"
                        )
                      }
                  }
                case None if transportFailure =>
                  IO.blocking {
                    val socket = new Socket()
                    try socket.connect(new InetSocketAddress("127.0.0.1", unavailablePort), 1000)
                    finally socket.close()
                  }
                case None =>
                  KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution).fence(
                    connection.copy(saslPassword = Some(fencerPassword + "-invalid")),
                    transactionalIds
                  )
              }
            firstAttempt.handleErrorWith { error =>
              interruptedBroker.traverse_(broker => IO.blocking(broker.start())) *>
                IO(firstFailure.set(error)) *> IO.raiseError(error)
            }
          } else
            interruptedBroker.fold(
              KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution).fence(connection, transactionalIds)
            )(broker =>
              KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
                .fence(connection.copy(bootstrapServers = broker.getBootstrapServers), transactionalIds)
            )
      }
      val reservation = AnalyticsReportReservation(
        AnalyticsErasureWorkerTestSupport.runId("poll-recovery-" + requestId),
        AnalyticsErasureWorkerTestSupport.fingerprint("poll-recovery-range"),
        0L,
        1L
      )
      val publisher = new AnalyticsReportPublisher[IO] {
        override def reserve(
            runId: RunId,
            rangeFingerprint: RangeFingerprint,
            now: Instant
        ): IO[AnalyticsReportReservation] =
          IO.pure(reservation)

        override def publish(
            value: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant
        ): IO[Unit] = IO.raiseError(new AssertionError("the poll-recovery test must stop before report publication"))

        override def publishErasure(
            value: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant,
            claim: ErasureClaim,
            completedAt: Instant
        ): IO[Unit] = IO.raiseError(new AssertionError("the poll-recovery test must stop before report publication"))
      }
      val worker = AnalyticsErasureWorkerTestSupport.worker(
        null,
        reactiveDatabase,
        store,
        KafkaConnection(workerBootstrapServers, None, None),
        if (fencerPassword.isEmpty) KafkaConnection(workerBootstrapServers, None, None)
        else KafkaConnection(workerBootstrapServers, Some("analytics_fencer"), Some(fencerPassword)),
        topic,
        IntegrationAnalyticsLakehousePaths.unsafe("file:///tmp/analytics-poll-recovery-" + UUID.randomUUID().toString),
        AnalyticsTestSubjectPseudonymizer.fromSecret("poll-recovery-test-secret".padTo(32, 'x').getBytes("UTF-8")),
        publisher,
        leaseDuration = 250.millis,
        pollInterval = 10.millis,
        producerFencer = recoveringFencer
      )
      val polling = worker.pollForever.start.unsafeRunSync()
      try {
        def awaitDurableAdvance: IO[Document] = IO.defer {
          IO.blocking(
            database
              .getCollection(AnalyticsCollections.ErasureRequests)
              .find(new Document("_id", requestId))
              .first()
          ).flatMap { document =>
            if (document != null && document.getString("phase") == "PublisherDrained") IO.pure(document)
            else IO.sleep(10.millis) *> awaitDurableAdvance
          }
        }
        val request = awaitDurableAdvance.timeout(90.seconds).unsafeRunSync()
        assert(attempts.get() >= 2, s"worker poll loop did not retry the fencer; attempts=${attempts.get()}")
        val failureWasObserved = Option(firstFailure.get()).exists { error =>
          if (transportFailure) hasTransportFailure(error) else hasAuthenticationFailure(error)
        }
        val failureDetail = Option(firstFailure.get()).map(error => s"${error.getClass.getName}: ${error.getMessage}")
        assert(failureWasObserved, s"expected the configured first-attempt failure, got $failureDetail")
        if (interruptedBroker.nonEmpty)
          assert(pendingWasObservedDuringOutage.get(), "the request was not observed pending during the broker outage")
        assertEquals(request.getString("state"), "Processing")
        assertEquals(request.getString("receiptId"), receiptId)
        assertEquals(
          store.barrier.readBarrier(AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId)).unsafeRunSync(),
          None
        )
        assertEquals(database.getCollection("event_outbox").countDocuments(), 1L)
      } finally polling.cancel.unsafeRunSync()
    } finally {
      try database.drop()
      finally {
        AnalyticsMongo4catsTestSupport.close(reactiveClient)
        client.close()
      }
    }
  }

  test("Compose permits the current publisher and rejects retired publisher authentication") {
    if (!enabled) ()
    else {
      val regularBrokerCutover = sys.env.get("HIRING_ANALYTICS_REGULAR_BROKER_CUTOVER_EVIDENCE").contains("true")
      assert(
        regularBrokerCutover || !bootstrapServers.split(",").exists(_.trim.endsWith(":9092")),
        "regular broker ACL evidence requires HIRING_ANALYTICS_REGULAR_BROKER_CUTOVER_EVIDENCE=true"
      )
      val fencerPassword = sys.env.getOrElse("KAFKA_FENCER_PASSWORD", "")
      val readerPassword = sys.env.getOrElse("KAFKA_READER_PASSWORD", "")
      val publisherPassword = sys.env.getOrElse("KAFKA_PUBLISHER_V2_PASSWORD", "")
      val legacyPublisherPassword = sys.env.getOrElse("KAFKA_LEGACY_PUBLISHER_PASSWORD", "")
      assert(
        fencerPassword.nonEmpty && readerPassword.nonEmpty && publisherPassword.nonEmpty && legacyPublisherPassword.nonEmpty,
        "Compose fencer, reader, current-publisher, and former publisher credentials are required"
      )

      val transactionalId = "hiring-publisher-acl-evidence-" + UUID.randomUUID().toString
      val stalePublisher = new KafkaProducer[String, String](
        properties("hiring_publisher_v2", publisherPassword, Some(transactionalId))
      )
      try {
        stalePublisher.initTransactions()
        stalePublisher.beginTransaction()
        stalePublisher
          .send(new ProducerRecord(topic, "acl-evidence", "pending-publisher-write"))
          .get(30, TimeUnit.SECONDS)
        KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
          .fence(
            KafkaConnection(bootstrapServers, Some("analytics_fencer"), Some(fencerPassword)),
            Vector(transactionalId)
          )
          .unsafeRunSync()
        val staleCommit = try {
          stalePublisher.commitTransaction()
          None
        } catch {
          case NonFatal(error) => Some(error)
        }
        assert(staleCommit.exists(hasProducerFencingFailure), clues(staleCommit))
      } finally stalePublisher.close(Duration.ofSeconds(5))

      val unauthorizedFence = KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
        .fence(
          KafkaConnection(bootstrapServers, Some("analytics_reader"), Some(readerPassword)),
          Vector(transactionalId)
        )
        .attempt
        .unsafeRunSync()
      assert(unauthorizedFence.swap.toOption.exists(hasKafkaAuthorizationFailure), clues(unauthorizedFence))

      val badFencerCredential = KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
        .fence(
          KafkaConnection(bootstrapServers, Some("analytics_fencer"), Some(fencerPassword + "-invalid")),
          Vector(transactionalId)
        )
        .attempt
        .unsafeRunSync()
      assert(badFencerCredential.swap.toOption.exists(hasAuthenticationFailure), clues(badFencerCredential))
      verifyWorkerKeepsRequestPendingAfterBadFencerPassword(fencerPassword)
      verifyAutomaticPollRecovery(fencerPassword, transportFailure = false)

      val legacyWriterProperties = properties("publisher", legacyPublisherPassword)
      legacyWriterProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, java.lang.Boolean.FALSE)
      val legacyWriter = new KafkaProducer[String, String](legacyWriterProperties)
      val legacyWriteResult = try {
        legacyWriter
          .send(new ProducerRecord(topic, "acl-evidence", "legacy-publisher-write-must-be-denied"))
          .get(30, TimeUnit.SECONDS)
        Right(())
      } catch {
        case NonFatal(error) => Left(error)
      } finally legacyWriter.close(Duration.ofSeconds(5))
      assert(legacyWriteResult.swap.toOption.exists(hasAuthenticationFailure), clues(legacyWriteResult))

      val legacyTransactionalId = "hiring-publisher-legacy-" + UUID.randomUUID().toString
      val legacyTransactional =
        new KafkaProducer[String, String](properties("publisher", legacyPublisherPassword, Some(legacyTransactionalId)))
      val legacyTransactionalInit = try {
        legacyTransactional.initTransactions()
        Right(())
      } catch {
        case NonFatal(error) => Left(error)
      } finally legacyTransactional.close(Duration.ofSeconds(5))
      assert(legacyTransactionalInit.swap.toOption.exists(hasAuthenticationFailure), clues(legacyTransactionalInit))

      val readerProperties = properties("analytics_reader", readerPassword)
      readerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, java.lang.Boolean.FALSE)
      val producer = new KafkaProducer[String, String](readerProperties)
      val writeResult = try {
        producer
          .send(new ProducerRecord(topic, "acl-evidence", "reader-write-must-be-denied"))
          .get(30, TimeUnit.SECONDS)
        Right(())
      } catch {
        case NonFatal(error) => Left(error)
      } finally producer.close(Duration.ofSeconds(5))

      assert(writeResult.swap.toOption.exists(hasTopicAuthorizationFailure), clues(writeResult))
    }
  }

  test("the erasure worker poll loop recovers after a refused transport connection") {
    if (transportRecoveryEnabled) {
      val fencerPassword = sys.env.getOrElse("KAFKA_FENCER_PASSWORD", "")
      assert(fencerPassword.nonEmpty, "Compose fencer credentials are required")
      verifyAutomaticPollRecovery(fencerPassword, transportFailure = true)
    }
  }

  test("the erasure worker retries after Kafka interrupts an in-flight AdminClient fencing request") {
    if (adminOutageRecoveryEnabled) {
      val image = DockerImageName.parse("apache/kafka:3.9.2").asCompatibleSubstituteFor("apache/kafka")
      val broker = new KafkaContainer(image)
      broker.start()
      val transactionalId = "hiring-publisher-admin-outage-" + UUID.randomUUID().toString
      val topicName = "admin-outage-" + UUID.randomUUID().toString
      val adminProperties = new Properties()
      adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers)
      val admin = Admin.create(adminProperties)
      val producerProperties = new Properties()
      producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers)
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
      val producer = new KafkaProducer[String, String](producerProperties)
      try {
        admin.createTopics(List(new NewTopic(topicName, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
        producer.initTransactions()
        producer.beginTransaction()
        producer
          .send(new ProducerRecord(topicName, "subject", "in-flight-admin-outage-fixture"))
          .get(30, TimeUnit.SECONDS)
        broker.execInContainer("bash", "-ec", "kill -STOP $(pgrep -x java)")
        verifyAutomaticPollRecovery(
          fencerPassword = "",
          transportFailure = true,
          workerBootstrapServers = broker.getBootstrapServers,
          interruptedBroker = Some(broker),
          registeredTransactionalId = Some(transactionalId)
        )
      } finally {
        producer.close(Duration.ofSeconds(5))
        admin.close(Duration.ofSeconds(5))
        broker.stop()
      }
    }
  }
}
