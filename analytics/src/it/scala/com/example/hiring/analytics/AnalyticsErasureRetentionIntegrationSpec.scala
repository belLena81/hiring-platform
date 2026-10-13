package com.example.hiring.analytics

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.ShortRetentionFixtures.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.StreamingIngestionFixtures.Transaction
import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsErasureStores
import com.example.hiring.analytics.config.{KafkaConnection, KafkaSecurityProtocol}
import com.example.hiring.analytics.domain.{AccountSubjectId, AnalyticsTopic, RangeFingerprint, RunId}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.*
import com.example.hiring.testing.{KafkaShortRetention, KafkaTestNamespace}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.common.serialization.StringSerializer
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Date, Properties, UUID}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*

/** Account erasure against a populated lakehouse, real producer fencing and real Kafka retention. */
final class AnalyticsErasureRetentionIntegrationSpec extends SharedRecoveryRuntimeSuite {
  override val munitIOTimeout: FiniteDuration = 30.minutes

  private val Unused = KafkaConnection("unused:9092")

  private final class Request(
      val harness: Harness,
      val stores: MongoAnalyticsErasureStores[IO],
      val subject: String,
      val receiptId: String
  ) {
    val id: AccountSubjectId = AnalyticsErasureWorkerTestSupport.accountSubjectId(subject)
    val runId: RunId = AnalyticsErasureWorkerTestSupport.runId("analytics-erasure-" + subject)
    val fingerprint: RangeFingerprint = AnalyticsErasureWorkerTestSupport.fingerprint("erasure:" + subject)
    def document(collection: String): IO[Document] =
      harness.sync(_.getCollection(collection).find(new Document("_id", subject)).first())
  }

  /** The durable rows a committed account deletion leaves for the worker, with its recorded producer registrations. */
  private def seedRequest(
      harness: Harness,
      runtime: Runtime,
      subject: String,
      transactionalIds: Vector[String],
      at: Instant
  ): IO[Request] = {
    val receiptId = UUID.randomUUID().toString
    harness
      .sync { db =>
        db.getCollection("analytics_erasure_requests")
          .insertOne(
            new Document("_id", subject)
              .append("state", "Pending")
              .append("fencingVersion", 1)
              .append("producerRegistry", true)
              .append("requestedAt", Date.from(at))
              .append("receiptId", receiptId)
          )
        db.getCollection("outbox_subject_fences").insertOne(new Document("_id", subject).append("deleted", true))
        db.getCollection("users").insertOne(new Document("_id", subject).append("accountStatus", "Deleted"))
        db.getCollection("hiring_migration_ledger")
          .insertOne(new Document("_id", "003_event_outbox_subject_references").append("state", "Complete"))
        db.createCollection("producer_registrations")
        db.getCollection("hiring_migration_ledger")
          .insertOne(
            new Document("_id", "012_attributable_producer_registrations")
              .append("state", "Complete")
              .append("version", Long.box(1L))
          )
        transactionalIds.foreach { transactionalId =>
          db.getCollection("producer_registrations")
            .insertOne(
              new Document("_id", s"$subject:$transactionalId")
                .append("subjectId", subject)
                .append("transactionalId", transactionalId)
                .append("kind", "Operational")
                .append("state", "Active")
                .append("registeredAt", Date.from(at))
            )
        }
        db.getCollection("analytics_report_control")
          .updateOne(
            new Document("_id", "analytics-report"),
            new Document("$set", new Document("state", "Hidden").append("generation", 1L))
          )
      }
      .as(
        new Request(
          harness,
          AnalyticsErasureWorkerTestSupport.stores(runtime.client, harness.database),
          subject,
          receiptId
        )
      )
  }

  private def worker(
      request: Request,
      clock: Clock[IO],
      retention: KafkaRetention[IO],
      fencer: TransactionalProducerFencer[IO] = (_, _) => IO.unit,
      fencerKafka: KafkaConnection = Unused
  ): AnalyticsErasureWorker[IO] =
    AnalyticsErasureWorkerTestSupport.worker(
      request.harness.spark,
      request.harness.database,
      request.stores,
      Unused,
      fencerKafka,
      "hiring.operational-events",
      request.harness.paths,
      request.harness.keys,
      request.harness.publisher,
      clock = clock,
      leaseDuration = 60.days,
      producerFencer = fencer,
      kafkaRetention = Some(retention),
      operational = shortRetention(request.harness.operational),
      deltaLogs = shiftedLogs(32L)
    )

  private def fixedRetention(passed: AtomicBoolean): KafkaRetention[IO] = new KafkaRetention[IO] {
    private val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 0L)).toOption.get
    override def capture(): IO[KafkaRetentionBarrier] = IO.pure(barrier)
    override def retentionPassed(value: KafkaRetentionBarrier): IO[Boolean] = IO(passed.get())
  }

  private def claimAt(request: Request, at: Instant) =
    request.stores.queue.claim(at, at.plus(1L, ChronoUnit.DAYS), 1).map(_.head)

  private def processing(
      request: Request,
      at: Instant,
      worker: AnalyticsErasureWorker[IO],
      claim: ErasureClaim
  ): IO[ErasureClaimOutcome] =
    request.harness.publisher.reserve(request.runId, request.fingerprint, at).flatMap(worker.process(claim, _))

  /** Delta only vacuums a removal once it is older than the one-second safety horizon, so unverified files retry. */
  private def processUntilReclaimed(
      request: Request,
      at: Instant,
      worker: AnalyticsErasureWorker[IO],
      claim: ErasureClaim,
      attempts: Int = 60
  ): IO[ErasureClaimOutcome] =
    processing(request, at, worker, claim).attempt.flatMap {
      case Right(outcome)                                                     => IO.pure(outcome)
      case Left(AnalyticsError.PhysicalReclamationUnverified) if attempts > 0 =>
        IO.sleep(1.second) *> processUntilReclaimed(request, at, worker, claim, attempts - 1)
      case Left(error) => IO.raiseError(error)
    }

  /** The request moves from DeltaPurged to ReadyToPublish and then to a published, completed receipt. */
  private def finishAfterHorizons(
      request: Request,
      afterRetention: Instant,
      retention: KafkaRetention[IO]
  ): IO[Unit] = {
    val resumed = worker(request, AnalyticsTestClocks.fixed(afterRetention), retention)
    for {
      claim <- claimAt(request, afterRetention)
      _ = assertEquals(claim.phase, ErasurePhase.DeltaPurged)
      reclaimed <- processUntilReclaimed(request, afterRetention, resumed, claim)
      _ = assertEquals(reclaimed, ErasureClaimOutcome.Completed)
      ready <- request.document("analytics_erasure_requests")
      _ = assertEquals(ready.getString("phase"), ErasurePhase.ReadyToPublish.toString)
      _ <- request.harness.sync(
        _.getCollection("analytics_erasure_requests")
          .updateOne(
            new Document("_id", request.subject),
            new Document("$set", new Document("leaseUntil", Date.from(afterRetention.minusSeconds(1L))))
          )
      )
      finalizer <- claimAt(request, afterRetention)
      _ = assertEquals(finalizer.phase, ErasurePhase.ReadyToPublish)
      published <- processing(request, afterRetention, resumed, finalizer)
      _ = assertEquals(published, ErasureClaimOutcome.Completed)
    } yield ()
  }

  private def seedLakehouse(harness: Harness, observed: Instant): IO[Unit] =
    for {
      frame <- timedFrame(harness, 0L, observed)
      _ <- processAt(harness, frame, 0L, observed)
    } yield ()

  private def exists(file: String): Boolean =
    if (file.regionMatches(true, 0, "file:", 0, 5)) Files.exists(Path.of(new java.net.URI(file)))
    else Files.exists(Path.of(file))

  private final case class Erased(files: Vector[String], progressBefore: Long, progressAfter: Long)

  /** One complete erasure of a populated lakehouse, shared by the criteria that observe its outcome. */
  private lazy val erased: Erased = resource
    .use { runtime =>
      for {
        harness <- runtime.harness("erasure-retention-complete")
        started = Instant.now()
        afterRetention = started.plus(31L, ChronoUnit.DAYS)
        observed = harness.at.truncatedTo(ChronoUnit.MICROS)
        _ <- seedLakehouse(harness, observed)
        progressBefore <- harness.count(harness.paths.streamingProgress)
        request <- seedRequest(harness, runtime, harness.subjects.head, Vector.empty, started)
        passed = new AtomicBoolean(false)
        retention = fixedRetention(passed)
        first <- claimAt(request, started).flatMap { claim =>
          processing(request, started, worker(request, AnalyticsTestClocks.fixed(started), retention), claim)
        }
        _ = assertEquals(first, ErasureClaimOutcome.Deferred)
        checkpoint <- request.document("analytics_erasure_requests")
        _ = assertEquals(checkpoint.getString("phase"), ErasurePhase.DeltaPurged.toString)
        _ <- IO(passed.set(true))
        _ <- request.harness.sync(
          _.getCollection("analytics_erasure_requests")
            .updateOne(
              new Document("_id", request.subject),
              new Document("$set", new Document("leaseUntil", Date.from(started.minusSeconds(1L))))
            )
        )
        _ <- finishAfterHorizons(request, afterRetention, retention)
        completion <- request.document("analytics_erasure_completions")
        _ = assertEquals(completion.getString("receiptId"), request.receiptId)
        done <- request.document("analytics_erasure_requests")
        _ = assertEquals(done.getString("state"), "Complete")
        files <- request.stores.progress.readDeltaFiles(request.id)
        progressAfter <- harness.count(harness.paths.streamingProgress)
      } yield Erased(files, progressBefore, progressAfter)
    }
    .unsafeRunSync()

  test("HAL-04.1 a worker stopped at DeltaPurged is resumed by a new worker and the owner receipt completes") {
    IO.blocking(erased).map(result => assert(result.files.nonEmpty, "the purge captured Delta files"))
  }

  test("HAL-04.2 captured Delta data and log paths are absent after completion and an unrelated control row survives") {
    IO.blocking(erased).map { result =>
      val remaining = result.files.filter(exists)
      assertEquals(remaining, Vector.empty[String])
      assert(result.files.exists(_.contains("_delta_log")), "log paths are among the captured evidence")
      assert(result.files.exists(_.endsWith(".parquet")), "data paths are among the captured evidence")
      assertEquals(result.progressAfter, result.progressBefore)
      assert(result.progressAfter > 0L)
    }
  }

  private def kafkaConnection(
      manifest: com.example.hiring.testing.LocalTestServices.Manifest,
      user: String,
      pass: String
  ) =
    KafkaConnection(
      manifest.kafkaBootstrap,
      Some(user),
      Some(pass),
      KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = true
    )

  private def heldTransaction(
      namespace: KafkaTestNamespace.Namespace,
      transactionalId: String
  ): Resource[IO, KafkaProducer[String, String]] =
    Resource.make(IO.blocking {
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.manifest.kafkaBootstrap)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      properties.put(
        "sasl.jaas.config",
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="hiring_publisher_v2" password="${namespace.manifest.publisherPassword}";"""
      )
      val producer = new KafkaProducer[String, String](properties)
      producer.initTransactions()
      producer.beginTransaction()
      producer
        .send(new ProducerRecord(namespace.events, "held", "held-transaction-record"))
        .get(30L, java.util.concurrent.TimeUnit.SECONDS)
      producer
    })(producer => IO.blocking(producer.close(java.time.Duration.ofSeconds(5))))

  private def fencingError(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      case _: ProducerFencedException       => true
      case _: InvalidProducerEpochException => true
      case _                                => false
    }

  test(
    "HAL-05.3 HAL-05-FENCE-01.1 the worker fences the transactional ID recorded on the erasure request and the held commit fails"
  ) {
    KafkaTestNamespace.resource.use { optional =>
      val namespace = optional.getOrElse(fail("HIRING_TEST_MANIFEST is required"))
      val transactionalId = "hiring-publisher-" + UUID.randomUUID()
      val fencerKafka = kafkaConnection(namespace.manifest, "analytics_fencer", namespace.manifest.fencerPassword)
      resource.use { runtime =>
        for {
          harness <- runtime.harness("erasure-fence-recorded")
          started = Instant.now()
          request <- seedRequest(harness, runtime, harness.subjects(1), Vector(transactionalId), started)
          outcome <- heldTransaction(namespace, transactionalId).use { producer =>
            for {
              fenced <- claimAt(request, started).flatMap { claim =>
                processing(
                  request,
                  started,
                  worker(
                    request,
                    AnalyticsTestClocks.fixed(started),
                    fixedRetention(new AtomicBoolean(false)),
                    KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution),
                    fencerKafka
                  ),
                  claim
                )
              }
              commit <- IO.blocking(producer.commitTransaction()).attempt
            } yield (fenced, commit)
          }
        } yield {
          assertEquals(outcome._1, ErasureClaimOutcome.Deferred)
          assert(outcome._2.left.exists(fencingError), s"the held commit must fail as fenced: ${outcome._2}")
        }
      }
    }
  }

  test(
    "HAL-05-FENCE-03.2 a fence that times out after the broker accepted it keeps the phase pending and a retry drains"
  ) {
    KafkaTestNamespace.resource.use { optional =>
      val namespace = optional.getOrElse(fail("HIRING_TEST_MANIFEST is required"))
      val transactionalId = "hiring-publisher-" + UUID.randomUUID()
      val fencerKafka = kafkaConnection(namespace.manifest, "analytics_fencer", namespace.manifest.fencerPassword)
      val timeoutOnce = new AtomicBoolean(true)
      val realFencer = KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
      val acceptedThenTimedOut = new TransactionalProducerFencer[IO] {
        override def fence(connection: KafkaConnection, transactionalIds: Vector[String]): IO[Unit] =
          realFencer.fence(connection, transactionalIds) *> IO.defer(
            if (timeoutOnce.compareAndSet(true, false))
              IO.raiseError(new java.util.concurrent.TimeoutException("fence acknowledgement timed out"))
            else IO.unit
          )
      }
      resource.use { runtime =>
        for {
          harness <- runtime.harness("erasure-fence-timeout")
          started = Instant.now()
          request <- seedRequest(harness, runtime, harness.subjects(2), Vector(transactionalId), started)
          retention = fixedRetention(new AtomicBoolean(false))
          attempt = (at: Instant) =>
            claimAt(request, at).flatMap(claim =>
              worker(request, AnalyticsTestClocks.fixed(at), retention, acceptedThenTimedOut, fencerKafka)
                .runClaim(claim)
            )
          first <- heldTransaction(namespace, transactionalId).use(_ => attempt(started))
          _ = assert(first.isInstanceOf[ErasureClaimOutcome.Failed], s"the timeout fails the attempt: $first")
          pending <- request.document("analytics_erasure_requests")
          _ = assertEquals(pending.getString("phase"), null)
          _ = assertEquals(pending.getString("state"), "Processing")
          second <- attempt(started.plus(1L, ChronoUnit.HOURS))
          _ = assertEquals(second, ErasureClaimOutcome.Deferred)
          third <- attempt(started.plus(2L, ChronoUnit.HOURS))
          drained <- request.document("analytics_erasure_requests")
        } yield {
          val reached = Set(ErasurePhase.PublisherDrained, ErasurePhase.OutboxPurged, ErasurePhase.DeltaPurged)
          assert(
            reached.map(_.toString).contains(drained.getString("phase")),
            s"the retry passes the publisher drain: ${drained.getString("phase")} after $third"
          )
        }
      }
    }
  }

  test("HAL-08.1 erasure completes only after the topic's earliest offset passes the captured barrier") {
    KafkaTestNamespace.resource.use { optional =>
      val namespace = optional.getOrElse(fail("HIRING_TEST_MANIFEST is required"))
      val readerConnection = kafkaConnection(namespace.manifest, "analytics_reader", namespace.manifest.readerPassword)
      resource.use { runtime =>
        for {
          harness <- runtime.harness("erasure-kafka-retention")
          started = Instant.now()
          afterRetention = started.plus(31L, ChronoUnit.DAYS)
          _ <- StreamingIngestionFixtures.publish(
            namespace,
            started.minusSeconds(30L),
            Vector(Transaction(0 until 12))
          )
          _ <- seedLakehouse(harness, harness.at.truncatedTo(ChronoUnit.MICROS))
          request <- seedRequest(harness, runtime, harness.subjects.head, Vector.empty, started)
          topic <- IO.fromEither(AnalyticsTopic.from(namespace.events).leftMap(new IllegalArgumentException(_)))
          retention = KafkaRetentionAdapter
            .liveRetention[IO](readerConnection, topic, AnalyticsBatchTestSupport.driverExecution)
          first <- claimAt(request, started).flatMap(claim =>
            processing(request, started, worker(request, AnalyticsTestClocks.fixed(started), retention), claim)
          )
          _ = assertEquals(first, ErasureClaimOutcome.Deferred)
          barrier <- request.stores.barrier.readBarrier(request.id)
          _ = assert(barrier.nonEmpty, "the Kafka barrier is captured before the Delta purge")
          _ <- request.harness.sync(
            _.getCollection("analytics_erasure_requests")
              .updateOne(
                new Document("_id", request.subject),
                new Document("$set", new Document("leaseUntil", Date.from(started.minusSeconds(1L))))
              )
          )
          early <- claimAt(request, afterRetention).flatMap(claim =>
            processing(
              request,
              afterRetention,
              worker(request, AnalyticsTestClocks.fixed(afterRetention), retention),
              claim
            )
          )
          _ = assertEquals(early, ErasureClaimOutcome.Deferred, "Delta horizons passed but Kafka records remain")
          stillPending <- request.document("analytics_erasure_requests")
          _ = assertEquals(stillPending.getString("phase"), ErasurePhase.DeltaPurged.toString)
          _ <- KafkaShortRetention.probe(namespace.manifest).use(_.expireCurrentRecords(namespace.events))
          _ <- request.harness.sync(
            _.getCollection("analytics_erasure_requests")
              .updateOne(
                new Document("_id", request.subject),
                new Document("$set", new Document("leaseUntil", Date.from(started.minusSeconds(1L))))
              )
          )
          // The deferred attempt scheduled its next look five minutes after the calendar it ran at.
          _ <- finishAfterHorizons(request, afterRetention.plus(10L, ChronoUnit.MINUTES), retention)
          done <- request.document("analytics_erasure_requests")
        } yield assertEquals(done.getString("state"), "Complete")
      }
    }
  }
}
