package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.ErasureClaim
import com.example.hiring.analytics.service.streaming.*
import com.mongodb.client.MongoClients
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, encode, lit}
import org.apache.spark.sql.types.*
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.time.{Duration, Instant}
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Real disposable Delta/Mongo sink composition. Source frames and injected failures are test inputs. */
private[analytics] object HiringAnalyticsRecoveryTestSupport {
  val InjectedFailure: Throwable = new IllegalStateException("injected hiring analytics sink boundary failure")
  private final class ReplicaSet
      extends GenericContainer[ReplicaSet](
        DockerImageName.parse(
          "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
        )
      )

  final case class Runtime(
      root: Path,
      uri: String,
      spark: SparkSession,
      client: MongoClient[IO],
      execution: SparkExecution[IO],
      driver: SparkBlockingExecution[IO]
  ) {
    def harness(name: String): IO[Harness] = for {
      database <- client.getDatabase("hiring_recovery_" + UUID.randomUUID().toString.replace('-', '_'))
      _ <- IO.blocking {
        val sync = MongoClients.create(uri)
        try {
          val db = sync.getDatabase(database.underlying.getName)
          db.createCollection("analytics_erasure_requests")
          db.getCollection("analytics_report_control")
            .insertOne(
              new Document("_id", "analytics-report")
                .append("generation", 0L)
                .append("state", "Unpublished")
                .append("nextRevision", 0L)
                .append("lastPublishedRevision", 0L)
                .append("lastRunId", "")
            )
        } finally sync.close()
      }
    } yield new Harness(this, database, IntegrationAnalyticsLakehousePaths.unsafe(root.resolve(name).toUri.toString))
  }

  def resource: Resource[IO, Runtime] = for {
    root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-analytics-recovery-")))(path =>
      IO.blocking {
        val entries = Files.walk(path)
        try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
        finally entries.close()
      }
    )
    container <- Resource.make(IO.blocking {
      new ReplicaSet()
        .withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
    })(mongo => IO.blocking(mongo.stop()))
    _ <- Resource.eval(IO.blocking {
      container.start()
      val initiated = container.execInContainer(
        "mongosh",
        "--quiet",
        "--eval",
        "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
      )
      require(initiated.getExitCode == 0, "disposable replica set must initialize")
      var primary = false
      var tries = 60
      while (!primary && tries > 0) {
        val observed = container.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary")
        primary = observed.getExitCode == 0 && observed.getStdout.trim == "true"
        if (!primary) Thread.sleep(250L)
        tries -= 1
      }
      require(primary, "disposable replica set must elect a primary")
    })
    uri = s"mongodb://${container.getHost}:${container.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"
    managed <- AppModule.sparkMongo[IO](
      uri,
      "local[2]",
      "HiringAnalyticsDurableSinkRecovery",
      Some(false),
      root.resolve("spark-temp").toString
    )
    (spark, client, blocking) = managed
    _ <- Resource.eval(blocking {
      spark.conf.set("spark.sql.shuffle.partitions", "2")
      spark.conf.set("spark.sql.session.timeZone", "UTC")
    })
  } yield Runtime(root, uri, spark, client, new LakehouseOperation[IO](blocking), blocking)

  final class Harness(val runtime: Runtime, val database: MongoDatabase[IO], val paths: AnalyticsLakehousePaths) {
    val execution = runtime.execution
    val spark = runtime.spark
    val operational = AnalyticsTestOperationalConfig.operational
    val streams = AnalyticsTestOperationalConfig.streams
    val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(19))
    val lock = new MongoAnalyticsLakehouseLock[IO](database, streams)
    val markers = new MongoActiveDeletionMarkerSource[IO](database, keys, streams)
    val retirements = new MongoHmacKeyRetirementAuthorizationStore[IO](database, streams)
    val publisher = new MongoAnalyticsReportPublisher[IO](runtime.client, database, operational)
    val writer = new DeltaBatchWriter[IO](paths, execution)
    val reader = new DeltaBatchReader[IO](execution)
    val maintenance = new DeltaAnalyticsErasureLakehouse[IO](
      spark,
      paths,
      keys,
      lock,
      retirements,
      operational,
      execution,
      org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO]
    )
    val topic = "hiring.recovery." + UUID.randomUUID().toString
    val subjects: Vector[String] = Vector.fill(12)(UUID.randomUUID().toString)
    val at: Instant = Instant.now().minusSeconds(30L)

    def sync[A](read: com.mongodb.client.MongoDatabase => A): IO[A] = IO.blocking {
      val client = MongoClients.create(runtime.uri)
      try read(client.getDatabase(database.underlying.getName))
      finally client.close()
    }

    def count(path: String): IO[Long] = execution {
      if (io.delta.tables.DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path).count() else 0L
    }

    def journal = new DeltaStreamingBatchJournal[IO](spark, paths, execution)

    def preparation(index: Long = 0L): StreamingInputPreparation = StreamingInputPreparation(
      StreamingBatchIdentity(
        StreamingLineage.from("hiring-recovery").toOption.get,
        StreamingBatchId.from(index).toOption.get
      ),
      at,
      None,
      RangeFingerprint.from(AnalyticsDigest.sha256Hex("selected recovery source".getBytes("UTF-8"))).toOption.get,
      (0 until 3).toVector.map(p => StreamingPartitionEndOffset.from(topic, p, 4L).toOption.get),
      (0 until 3).toVector.map(p => StreamingPartitionSummary.from(topic, p, 0L, 3L, 4L).toOption.get)
    )

    def parsed(malformed: Boolean = false, eventTimes: Vector[Instant] = Vector.empty): IO[DataFrame] = execution {
      require(eventTimes.isEmpty || eventTimes.size == 12, "fixture event times must cover its twelve records")
      val rows = (0 until 12).map { n =>
        val occurredAt = eventTimes.lift(n).getOrElse(at)
        val raw =
          if (malformed && n == 0) "{invalid"
          else
            s"""{"eventId":"job-event-$n","eventType":"JOB_CREATED","occurredAt":"$occurredAt","aggregateType":"Job","aggregateId":"job-$n","actorId":"${subjects(
                n
              )}","payload":{"jobId":"job-$n","job":{"skills":["Scala"]}}}"""
        Row(topic, n % 3, (n / 3).toLong, raw)
      }
      val schema = StructType(
        Vector(
          StructField("topic", StringType, false),
          StructField("partition", IntegerType, false),
          StructField("offset", LongType, false),
          StructField("value", StringType, true)
        )
      )
      val raw = spark
        .createDataFrame(rows.asJava, schema)
        .withColumn("value", encode(col("value"), "UTF-8"))
        .withColumn("timestamp", lit(Timestamp.from(at)))
      OperationalEventTransforms.parseKafkaRecords(raw)
    }

    def streamingStages(
        frame: DataFrame,
        sink: DeltaWriter[IO] = writer,
        publication: AnalyticsReportPublisher[IO] = publisher
    ): Resource[IO, StreamingBatchStages[IO]] = {
      val manifests = new DeltaManifestStore[IO](spark, paths, execution)
      val ingestion =
        new AnalyticsBatchIngestionStage[IO](paths, keys, execution, manifests, sink, operational.retention)
      val silver = new AnalyticsBatchSilverStage[IO](
        paths,
        keys,
        execution,
        sink,
        reader,
        QuarantineIdentifier,
        operational.retention
      )
      SparkStreamingBatchStages.resource[IO](
        spark,
        frame,
        paths,
        keys,
        execution,
        ingestion,
        silver,
        new AnalyticsLateFactStage[IO](paths, execution, sink),
        sink,
        reader,
        publication,
        operational.retention,
        maintenance.configureRawTables,
        maintenance.applyActiveDeletions,
        IO.unit
      )
    }

    def checkpoint(store: StreamingBatchJournal[IO]): StreamingCheckpointAcknowledgement[IO] =
      new StreamingCheckpointAcknowledgement[IO] {
        override def callbackMayAcknowledge(identity: StreamingBatchIdentity): IO[Unit] =
          store
            .load(identity)
            .flatMap(value =>
              IO.raiseUnless(value.exists(_.terminalOutcome.nonEmpty))(
                AnalyticsError
                  .InvalidConfiguration("actual durable terminal progress is required before callback acknowledgement")
              )
            )
        override def reconcile(
            lineage: StreamingLineage,
            batches: Vector[StreamingCheckpointBatch],
            established: Boolean
        ) = IO.unit
      }

    def process(
        frame: DataFrame,
        sink: DeltaWriter[IO] = writer,
        publication: AnalyticsReportPublisher[IO] = publisher,
        store: StreamingBatchJournal[IO] = journal
    ): IO[StreamingCoordinatorResult] = lock.resource(paths.root).use { _ =>
      maintenance.validateHmacConfigurationLocked *> streamingStages(frame, sink, publication).use { stages =>
        new StreamingBatchCoordinator[IO](store, markers, stages, checkpoint(store)).process(preparation())
      }
    }

    def replayStages: AnalyticsLateFactReplayStages[IO] =
      new SparkAnalyticsLateFactReplayStages[IO](spark, paths, execution, reader, writer, maintenance)
    def replayJournal = new MongoAnalyticsLateFactReplayJournal[IO](database, streams, paths.root)
    def replay(
        request: AnalyticsLateFactReplayRequest,
        publication: AnalyticsReportPublisher[IO] = publisher
    ): IO[AnalyticsLateFactReplayOutcome] =
      replayJournal.ensureIndexes *> new AnalyticsLateFactReplayService[IO](
        paths.root,
        replayJournal,
        markers,
        replayStages,
        publication,
        lock,
        operational.retention.publishedSnapshotDays
      ).run(request)
  }

  class DelegatingPublisher(delegate: AnalyticsReportPublisher[IO]) extends AnalyticsReportPublisher[IO] {
    override def reserve(run: RunId, fingerprint: RangeFingerprint, at: Instant) =
      delegate.reserve(run, fingerprint, at)
    override def reservePinned(run: RunId, fingerprint: RangeFingerprint, at: Instant) =
      delegate.reservePinned(run, fingerprint, at)
    override def publicationReceipt(reservation: AnalyticsReportReservation)(using cats.Applicative[IO]) =
      delegate.publicationReceipt(reservation)
    override def publish(reservation: AnalyticsReportReservation, report: AnalyticsReportOutput, expiry: Instant) =
      delegate.publish(reservation, report, expiry)
    override def publishErasure(
        reservation: AnalyticsReportReservation,
        report: AnalyticsReportOutput,
        expiry: Instant,
        claim: ErasureClaim,
        completed: Instant
    ) = delegate.publishErasure(reservation, report, expiry, claim, completed)
  }
}
