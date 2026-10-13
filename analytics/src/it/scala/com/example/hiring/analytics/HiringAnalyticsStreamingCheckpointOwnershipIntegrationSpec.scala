package com.example.hiring.analytics

import cats.effect.{Deferred, FiberIO, IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.ErasureRequestFixtures.*
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.*
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.adapter.mongo.{MongoAnalyticsStreamingRegistry, MongoStreamingActivationGate}
import com.example.hiring.analytics.adapter.spark.{SparkHiringAnalyticsStream, StreamingCheckpointControl}
import com.example.hiring.analytics.app.{StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.config.{AnalyticsConfigFixtures, AnalyticsStreamingSettings, KafkaConnection}
import com.example.hiring.analytics.config.KafkaSecurityProtocol
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.ErasureClaimOutcome
import com.example.hiring.analytics.service.streaming.StreamingActivationGate
import munit.CatsEffectSuite
import org.apache.spark.sql.DataFrame

import java.nio.file.{Files, Path}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Checkpoint identity, loss and corruption fail closed, and one stream owns a lakehouse at a time. */
final class HiringAnalyticsStreamingCheckpointOwnershipIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 30.minutes

  /** A checkpoint and lakehouse a real run processed, backed up beside the original paths. */
  private final case class Processed(fixture: StreamingIngestionFixtures.Fixture, backup: Path)

  private val processed = ResourceSuiteLocalFixture(
    "processed-checkpoint",
    for {
      base <- fixture
      backup = base.root.resolve("backup")
      _ <- Resource.eval(for {
        settings <- settings(base, 13)
        _ <- publish(base.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until 12)))
        _ <- grantActivation(base, settings)
        _ <- runToCompletion(settings).timeout(10.minutes)
        _ <- IO.blocking {
          copyTree(base.root.resolve("lakehouse"), backup.resolve("lakehouse"))
          copyTree(base.checkpoint, backup.resolve("checkpoint"))
        }
      } yield ())
    } yield Processed(base, backup)
  )

  override def munitFixtures: List[munit.AnyFixture[?]] = List(processed)

  private def copyTree(from: Path, to: Path): Unit = {
    val entries = Files.walk(from)
    try
      entries.iterator().asScala.foreach { source =>
        val target = to.resolve(from.relativize(source).toString)
        if (Files.isDirectory(source)) { val _ = Files.createDirectories(target) }
        else {
          val _ = Files.createDirectories(target.getParent)
          val _ = Files.copy(source, target)
        }
      }
    finally entries.close()
  }

  /** The processed state restored to the same absolute paths on a fresh Mongo database, one case at a time. */
  private def restored: Resource[IO, StreamingIngestionFixtures.Fixture] = {
    val backup = processed()
    freshDatabase(backup.fixture).evalTap { restoredFixture =>
      for {
        _ <- deleteTree(restoredFixture.root.resolve("lakehouse")) *> deleteTree(restoredFixture.checkpoint)
        _ <- IO.blocking {
          copyTree(backup.backup.resolve("lakehouse"), restoredFixture.root.resolve("lakehouse"))
          copyTree(backup.backup.resolve("checkpoint"), restoredFixture.checkpoint)
        }
      } yield ()
    }
  }

  private def batchCounter(batches: AtomicInteger): StreamingDurableBoundaryObserver[IO] =
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
        IO.whenA(boundary == StreamingDurableBoundary.Prepared)(IO(batches.incrementAndGet()).void)
    }

  private def refusal(error: Throwable): Boolean =
    error.getMessage == StreamingCheckpointControl.invalidCheckpoint.getMessage

  private def refusedRun(settings: com.example.hiring.analytics.config.AnalyticsStreamingRuntimeSettings): IO[Int] = {
    val batches = new AtomicInteger(0)
    runToCompletion(settings, Some(batchCounter(batches))).timeout(10.minutes).attempt.flatMap {
      case Left(error) if refusal(error) => IO.pure(batches.get())
      case other => IO.raiseError(new AssertionError(s"expected a checkpoint refusal, got $other"))
    }
  }

  test("HCS-08.1 a checkpoint deleted while lineage progress exists is refused without writing") {
    restored.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- grantActivation(fixture, settings)
        before <- deltaVersions(fixture.root.resolve("lakehouse"))
        _ <- deleteTree(fixture.checkpoint)
        batches <- refusedRun(settings)
        after <- deltaVersions(fixture.root.resolve("lakehouse"))
      } yield {
        assertEquals(batches, 0)
        assertEquals(after, before, "Delta versions are unchanged")
        assert(!Files.exists(fixture.checkpoint), "the refusal does not recreate the checkpoint")
      }
    }
  }

  test("HCS-08.2 a checkpoint missing its latest offsets file is refused by log reconciliation") {
    restored.use { fixture =>
      for {
        settings <- settings(fixture, 13)
        _ <- grantActivation(fixture, settings)
        before <- deltaVersions(fixture.root.resolve("lakehouse"))
        _ <- IO.blocking {
          val offsets = fixture.checkpoint.resolve("offsets")
          val files = Files.list(offsets)
          val latest =
            try
              files
                .iterator()
                .asScala
                .filter(_.getFileName.toString.matches("\\d+"))
                .toVector
                .maxBy(_.getFileName.toString.toLong)
            finally files.close()
          Files.delete(latest)
        }
        batches <- refusedRun(settings)
        after <- deltaVersions(fixture.root.resolve("lakehouse"))
      } yield {
        assertEquals(batches, 0)
        assertEquals(after, before)
      }
    }
  }

  test("HCS-08.3 a different stream ID or lakehouse root is refused as a checkpoint identity mismatch") {
    restored.use { fixture =>
      for {
        otherStream <- mismatchIn(fixture, configurationText(fixture, 13, streamId = "hiring-ingestion-other"))
        otherRoot <- freshDatabase(fixture).use(other =>
          mismatchIn(other, configurationText(other, 13, lakehouseDirectory = "lakehouse-other"))
        )
      } yield {
        assertEquals(otherStream, 0)
        assertEquals(otherRoot, 0)
      }
    }
  }

  private def mismatchIn(fixture: StreamingIngestionFixtures.Fixture, text: String): IO[Int] =
    for {
      settings <- IO.fromEither(AnalyticsConfigFixtures.streamingRuntime(text))
      _ <- grantActivation(fixture, settings)
      batches <- refusedRun(settings)
    } yield batches

  test("HCS-08.4 the restored backup resumes and reprocesses nothing") {
    restored.use { fixture =>
      val batches = new AtomicInteger(0)
      for {
        settings <- settings(fixture, 13)
        _ <- grantActivation(fixture, settings)
        before <- stored(fixture, settings)
        _ <- runToCompletion(settings, Some(batchCounter(batches))).timeout(10.minutes)
        after <- stored(fixture, settings)
      } yield {
        assertEquals(batches.get(), 0)
        assertEquals(after, before)
        assertEquals(before, Stored(12L, 12L, 1L))
      }
    }
  }

  // ---- ownership cases run a stream directly on the shared session, per test --------------------------------------

  private val SourceIds = ("ownership-cluster", "ownership-topic")

  private def ownershipStreamSettings(checkpoint: Path, grant: String): AnalyticsStreamingSettings =
    AnalyticsConfigFixtures
      .streaming(s"""analytics.streaming {
      stream-id = "hiring-ownership"
      activation-grant-id = "$grant"
      checkpoint-location = "${checkpoint.toUri}"
      trigger-interval = 1 second
      max-offsets-per-trigger = 13
      maximum-replay-records = 1000
      maintenance-interval = 60 seconds
      progress-retention = 7 days
      initial-offsets = [{ partition = 0, offset = 0 }]
    }""")
      .fold(error => throw error, identity)

  private def openGate: StreamingActivationGate[IO] = new StreamingActivationGate[IO] {
    override def requireAuthorized(identity: StreamingActivationIdentity, grantId: String): IO[Instant] =
      IO.pure(Instant.now().plus(1L, ChronoUnit.HOURS))
  }

  private def stream(
      runtime: Runtime,
      harness: Harness,
      namespace: com.example.hiring.testing.KafkaTestNamespace.Namespace,
      settings: AnalyticsStreamingSettings,
      gate: StreamingActivationGate[IO],
      callback: (DataFrame, StreamingBatchId, StreamingLineage, Map[(String, Int), Long], IO[Unit]) => IO[Unit] =
        (_, _, _, _, authorize) => authorize
  ): SparkHiringAnalyticsStream[IO] = {
    val manifest = namespace.manifest
    new SparkHiringAnalyticsStream[IO](
      runtime.spark,
      runtime.execution,
      runtime.driver,
      KafkaConnection(
        manifest.kafkaBootstrap,
        Some("analytics_reader"),
        Some(manifest.readerPassword),
        KafkaSecurityProtocol.SaslPlaintext,
        allowPlaintext = true
      ),
      AnalyticsTopic.from(namespace.events).toOption.get,
      settings,
      gate,
      new MongoAnalyticsStreamingRegistry[IO](harness.database, harness.streams),
      harness.lock,
      harness.paths.root,
      harness.checkpoint(harness.journal),
      callback,
      () => IO.pure(SourceIds)
    )
  }

  private def freshCheckpoint(namespace: com.example.hiring.testing.KafkaTestNamespace.Namespace): Path =
    Path
      .of(namespace.manifest.workspace)
      .resolve(".local/data/analytics/checkpoints/ownership-" + UUID.randomUUID())

  private def mutexRows(harness: Harness): IO[Long] =
    harness.sync(_.getCollection("analytics_lakehouse_mutexes").countDocuments())

  private def activeQueries(runtime: Runtime): IO[Int] = runtime.driver(runtime.spark.streams.active.length)

  /** Bounded polling for an observable condition. */
  private def eventually(description: String, attempts: Int = 120)(check: IO[Boolean]): IO[Unit] =
    check.flatMap { done =>
      if (done) IO.unit
      else if (attempts == 0) IO.raiseError(new AssertionError(s"timed out waiting for $description"))
      else IO.sleep(500.millis) *> eventually(description, attempts - 1)(check)
    }

  private def runningStream(
      runtime: Runtime,
      harness: Harness,
      namespace: com.example.hiring.testing.KafkaTestNamespace.Namespace,
      gate: StreamingActivationGate[IO] = openGate,
      grant: String = "ownership-grant"
  ): Resource[IO, FiberIO[Unit]] =
    Resource.make(
      stream(runtime, harness, namespace, ownershipStreamSettings(freshCheckpoint(namespace), grant), gate).resource
        .use(_ => IO.unit)
        .start
    )(_.cancel) <* Resource.eval(eventually("the stream query to become active")(activeQueries(runtime).map(_ > 0)))

  private def withNamespace[A](use: com.example.hiring.testing.KafkaTestNamespace.Namespace => IO[A]): IO[A] =
    com.example.hiring.testing.KafkaTestNamespace.resource.use(optional =>
      use(optional.getOrElse(throw new IllegalStateException("HIRING_TEST_MANIFEST is required")))
    )

  test("HCS-08.5 a stream stops when its activation grant expires and a restart under the same grant is denied") {
    withNamespace { namespace =>
      resource.use { runtime =>
        for {
          harness <- runtime.harness("ownership-grant-expiry")
          settings = ownershipStreamSettings(freshCheckpoint(namespace), "ownership-expiring-grant")
          identity <- IO.fromEither(
            settings.activationIdentity(SourceIds._1, SourceIds._2, namespace.events, harness.paths.root)
          )
          _ <- insertGrant(
            namespace.manifest.mongoUri,
            harness.database.name,
            identity,
            90.seconds,
            "ownership-expiring-grant"
          )
          gate = new MongoStreamingActivationGate[IO](harness.database, harness.streams)
          fiber <- stream(runtime, harness, namespace, settings, gate).resource.use(_ => IO.unit).start
          observed <- IO.race(
            fiber.join,
            eventually("the query to be observed active")(activeQueries(runtime).map(_ > 0))
          )
          _ = observed.left.foreach(early => fail(s"the stream ended before it became active: $early"))
          outcome <- fiber.join.timeout(2.minutes)
          _ <- eventually("the query to stop after expiry")(activeQueries(runtime).map(_ == 0))
          restart <- stream(runtime, harness, namespace, settings, gate).resource.use(_ => IO.unit).attempt
        } yield {
          assert(outcome.isError, s"the stream must fail when its grant expires: $outcome")
          assert(
            restart.left.exists(error =>
              error.isInstanceOf[AnalyticsError.InvalidConfiguration] &&
                error.getMessage.contains("activation is absent, malformed, or does not match")
            ),
            s"the same grant must not authorize a restart: $restart"
          )
        }
      }
    }
  }

  test("HCS-12.1 a one-shot batch is rejected on a registered streaming lakehouse before any batch work") {
    withNamespace { namespace =>
      resource.use { runtime =>
        for {
          harness <- runtime.harness("ownership-batch-denied")
          topic = AnalyticsTopic.from(namespace.events).toOption.get
          manifest = AnalyticsRunManifest
            .from(
              RunId.from("ownership-batch").toOption.get,
              PartitionOffsetRange.fromTopic(topic, 0, 0L, 12L).map(Vector(_))
            )
            .toOption
            .get
          untouched = new AnalyticsBatchLakehouse[IO] {
            private def forbidden[A]: IO[A] = IO.raiseError(new AssertionError("batch work started"))
            override def validateHmacConfiguration: IO[Unit] = forbidden
            override def prepare(m: AnalyticsRunManifest, tokens: Vector[SubjectToken]) = forbidden
            override def rebuildGoldAndExtractReport(asOf: Instant) = forbidden
          }
          batch = new HiringAnalyticsBatch[IO](
            harness.paths,
            harness.markers,
            harness.publisher,
            (_, _, _) => IO.raiseError(new AssertionError("manifest persisted")),
            untouched,
            harness.lock,
            new MongoAnalyticsStreamingRegistry[IO](harness.database, harness.streams),
            harness.operational,
            cats.effect.Clock[IO]
          )
          disabled = (error: Throwable) => error.getMessage.contains("one-shot analytics batch is disabled")
          during <- runningStream(runtime, harness, namespace).use(_ => batch.run(manifest).attempt)
          after <- batch.run(manifest).attempt
        } yield {
          assert(during.left.exists(disabled), s"batch must be refused while the stream runs: $during")
          assert(after.left.exists(disabled), s"batch must stay refused after the stream stops: $after")
        }
      }
    }
  }

  test("HCS-12.2 a second stream does not acquire the stream owner and the first owner stays intact") {
    withNamespace { namespace =>
      resource.use { runtime =>
        for {
          harness <- runtime.harness("ownership-second-stream")
          _ <- runningStream(runtime, harness, namespace).use { first =>
            for {
              owned <- mutexRows(harness)
              _ = assert(owned >= 1L, "the first stream owns a durable mutex")
              second <- stream(
                runtime,
                harness,
                namespace,
                ownershipStreamSettings(freshCheckpoint(namespace), "ownership-grant"),
                openGate
              ).resource.use(_ => IO.unit).timeout(10.seconds).attempt
              _ = assert(
                second.left.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]),
                s"the second stream must wait for ownership and be cancelled: $second"
              )
              after <- mutexRows(harness)
              running <- first.join.timeout(1.second).attempt
              active <- activeQueries(runtime)
            } yield {
              assertEquals(after, owned, "the first owner's mutex is intact")
              assert(running.isLeft, "the first stream is still running")
              assertEquals(active, 1)
            }
          }
          released <- mutexRows(harness)
        } yield assertEquals(released, 0L)
      }
    }
  }

  test("HCS-12.3 an erasure worker claim waits for the active callback and then runs") {
    withNamespace { namespace =>
      resource.use { runtime =>
        for {
          harness <- runtime.harness("ownership-worker-waits")
          started <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          callback = (
              _: DataFrame,
              _: StreamingBatchId,
              _: StreamingLineage,
              _: Map[(String, Int), Long],
              authorize: IO[Unit]
          ) => authorize *> started.complete(()).void *> release.get
          now = Instant.now()
          request <- seedRequest(harness, runtime, harness.subjects.head, Vector.empty, now)
          _ <- publish(namespace, now.minusSeconds(30L), Vector(StreamingIngestionFixtures.Transaction(0 until 1)))
          outcome <- Resource
            .make(
              stream(
                runtime,
                harness,
                namespace,
                ownershipStreamSettings(freshCheckpoint(namespace), "ownership-grant"),
                openGate,
                callback
              ).resource.use(_ => IO.unit).start
            )(_.cancel)
            .use { _ =>
              for {
                _ <- started.get.timeout(2.minutes)
                claim <- claimAt(request, now)
                work <- processing(
                  request,
                  now,
                  worker(
                    request,
                    AnalyticsTestClocks.fixed(now),
                    fixedRetention(new java.util.concurrent.atomic.AtomicBoolean(false))
                  ),
                  claim
                ).start
                early <- work.join.timeout(5.seconds).attempt
                _ = assert(early.isLeft, s"the claim must wait while the callback owns the lakehouse: $early")
                _ <- release.complete(())
                done <- work.joinWithNever.timeout(3.minutes)
              } yield done
            }
        } yield assertEquals(outcome, ErasureClaimOutcome.Deferred)
      }
    }
  }
}
