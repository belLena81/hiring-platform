package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.ErasureRequestFixtures.*
import com.example.hiring.analytics.StreamingCalendarFixtures.*
import com.example.hiring.analytics.StreamingIngestionFixtures.Transaction
import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.config.{KafkaConnection, KafkaSecurityProtocol}
import com.example.hiring.analytics.domain.AnalyticsTopic
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
