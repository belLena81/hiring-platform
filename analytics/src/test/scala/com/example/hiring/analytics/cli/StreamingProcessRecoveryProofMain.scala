package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.app.{AppModule, StreamingDurableBoundary, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import pureconfig.ConfigSource
import HiringAnalyticsStreamingWorkloadMain.ProducerCredentials
import HiringAnalyticsStreamingWorkloadMain.given
import java.time.Instant
import java.util.{Properties, UUID}
import com.example.hiring.analytics.adapter.mongo.{MongoAnalyticsLakehouseLock, MongoAnalyticsReportPublisher}
import com.example.hiring.analytics.adapter.spark.{DeltaStreamingBatchJournal, SparkExecution, SparkPhysicalLocation}
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehousePaths,
  AnalyticsReportPublicationReceipt,
  AnalyticsStreamingRegistry
}
import com.example.hiring.analytics.service.streaming.*
import com.mongodb.client.MongoClients
import com.mongodb.{ConnectionString, ReadConcern, WriteConcern}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sha2}
import org.bson.Document
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, LinkOption, Path, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Actual AppModule, Kafka source and Spark checkpoint. The supervisor owns SIGKILL and explicit mutex recovery. */
object StreamingProcessRecoveryProofMain extends IOApp {
  private def safe(path: Path): Unit = {
    require(path.isAbsolute && path.normalize() == path)
    Iterator
      .iterate(Option(path))(_.flatMap(p => Option(p.getParent)))
      .takeWhile(_.nonEmpty)
      .flatten
      .foreach(p => require(!Files.isSymbolicLink(p)))
  }
  private def read(path: Path): Document = {
    safe(path)
    require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && Files.size(path) <= 65536L)
    Document.parse(Files.readString(path, UTF_8))
  }
  private def writeDocument(path: Path, value: Document): Unit = {
    safe(path)
    Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    Files.writeString(path, value.toJson, UTF_8, StandardOpenOption.WRITE)
    ()
  }
  private def write(path: Path, value: Document): IO[Unit] = IO.blocking(writeDocument(path, value))
  private def execution: SparkExecution[IO] = new SparkExecution[IO] {
    override def apply[A](value: => A) = IO.blocking(value)
    override def either[A](value: => Either[AnalyticsError, A]) = IO.blocking(value).flatMap(IO.fromEither)
  }
  private[cli] def identity(document: Document): StreamingBatchIdentity = StreamingBatchIdentity(
    StreamingLineage.from(document.getString("lineage")).toOption.get,
    StreamingBatchId.from(document.get("batchId", classOf[java.lang.Number]).longValue()).toOption.get
  )

  private[cli] final case class CapturedMutex(lockId: String, ownerToken: String)

  private[cli] def capturedMutexes(
      document: Document,
      dataLockId: String,
      streamLockId: String
  ): Either[String, Vector[CapturedMutex]] = scala.util
    .Try {
      require(dataLockId != streamLockId)
      val owners = document.getList("mutexOwners", classOf[Document]).asScala.toVector.map { value =>
        val token = value.getString("ownerToken")
        require(UUID.fromString(token).toString == token)
        CapturedMutex(value.getString("lockId"), token)
      }
      require(owners.size == 2 && owners.map(_.lockId).toSet == Set(dataLockId, streamLockId))
      owners
    }
    .toEither
    .left
    .map(_ => "exact data and stream owners required")

  private[cli] def removeCapturedMutexes(owners: Vector[CapturedMutex])(
      validate: CapturedMutex => IO[Unit],
      remove: CapturedMutex => IO[Unit],
      record: CapturedMutex => IO[Unit]
  ): IO[Unit] = owners.traverse_(validate) *> owners.traverse_(owner => remove(owner) *> record(owner))

  private[cli] final case class AdmittedIdentity(eventId: String, fingerprint: String)

  private[cli] def requireAdmittedIdentities(actual: Vector[AdmittedIdentity], expected: Set[AdmittedIdentity]): Unit =
    require(
      actual.size == expected.size && actual.distinct.size == actual.size && actual.toSet == expected,
      "admitted identities differ from committed source"
    )

  private[cli] def admittedIdentities(frame: DataFrame, expected: Set[AdmittedIdentity]): Vector[AdmittedIdentity] =
    frame
      .filter(col("eventId").isin(expected.toVector.map(_.eventId)*))
      .select("eventId", "eventFingerprint")
      .limit(13)
      .collect()
      .toVector
      .map(row => AdmittedIdentity(row.getString(0), row.getString(1)))

  private def seedCoordinates(seed: Document): Vector[(Int, Long)] = {
    val coordinates = seed
      .getList("coordinates", classOf[Document])
      .asScala
      .toVector
      .map(value =>
        value.getInteger("partition").intValue() -> value.get("offset", classOf[java.lang.Number]).longValue()
      )
    require(coordinates.size == 12 && coordinates.distinct.size == 12, "exact committed coordinates required")
    coordinates
  }

  private[cli] def seedIdentityMapping(seed: Document): Map[(Int, Long), AdmittedIdentity] = {
    val coordinates = seedCoordinates(seed)
    val seeded = seed.getList("coordinates", classOf[Document]).asScala.toVector
    require(
      seeded.forall(value => value.containsKey("eventId") && value.containsKey("eventFingerprint")),
      "complete seed identity evidence required"
    )
    coordinates
      .zip(seeded.map(value => AdmittedIdentity(value.getString("eventId"), value.getString("eventFingerprint"))))
      .toMap
  }

  private def bronzeEvidence(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      topic: String,
      seed: Document
  ): (DataFrame, Set[AdmittedIdentity]) = {
    val expected = seedCoordinates(seed).toSet
    val predicate = expected.toVector
      .map { case (partition, offset) =>
        (col("topic") === topic) && (col("partition") === partition) && (col("offset") === offset)
      }
      .reduce(_ || _)
    val selected = spark.read
      .format("delta")
      .load(paths.bronze)
      .filter(predicate)
      .withColumn("eventFingerprint", sha2(col("rawValue"), 256))
    val rows = selected.select("partition", "offset", "eventId", "eventFingerprint").limit(13).collect().toVector
    require(
      rows.size == 12 && rows.map(row => row.getInt(0) -> row.getLong(1)).toSet == expected,
      "Bronze coordinates differ from committed seed"
    )
    val identities = rows.map(row => AdmittedIdentity(row.getString(2), row.getString(3)))
    require(identities.distinct.size == 12 && identities.map(_.eventId).distinct.size == 12)
    val expectedMapping = seedIdentityMapping(seed)
    val actualMapping =
      rows.map(row => (row.getInt(0) -> row.getLong(1)) -> AdmittedIdentity(row.getString(2), row.getString(3))).toMap
    require(actualMapping == expectedMapping, "Bronze identity binding differs from committed seed")
    selected -> identities.toSet
  }

  private[cli] enum DiagnosticStage {
    case Setup, Query, Journal, Terminal, Fingerprint, Receipt, Report, Coordinates, Counts, Watermark, Snapshot
  }

  private[cli] final case class InspectionMismatch(stage: DiagnosticStage) extends IllegalStateException
  private def inspectionRequire(condition: Boolean, stage: DiagnosticStage): Unit =
    if (!condition) throw InspectionMismatch(stage)

  private[cli] def sameSnapshot(recorded: Document, observed: Document): Boolean =
    Document.parse(recorded.toJson) == Document.parse(observed.toJson)

  private[cli] def snapshotFile(mode: String): String =
    if (mode == "inspect") "inspection.json" else "recovered.json"

  private[cli] def failureDiagnostic(stage: DiagnosticStage, failure: Throwable): String = {
    val name = failure.getClass.getSimpleName
    val safeClass = if (name.matches("[A-Za-z0-9_$]+")) name else "UnexpectedError"
    val observedStage = failure match {
      case mismatch: InspectionMismatch => mismatch.stage
      case _                            => stage
    }
    s"STREAMING_PROCESS_RECOVERY_FAILED stage=$observedStage errorClass=$safeClass"
  }

  override def run(args: List[String]): IO[ExitCode] =
    Ref.of[IO, DiagnosticStage](DiagnosticStage.Setup).flatMap { diagnostic =>
      val program = for {
        _ <- IO(
          require(args.size == 3 && Set("seed", "pause", "recover", "inspect", "release-owner").contains(args.head))
        )
        boundary <- IO(StreamingDurableBoundary.valueOf(args(1)))
        output <- IO(Path.of(args(2)).toAbsolutePath.normalize())
        settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
        config <- IO.blocking(ConfigFactory.load())
        nonce <- IO(config.getString("analytics.workload-api.nonce"))
        _ <- IO.fromEither(
          StreamingProofIsolation
            .validate(
              settings.common.mongoDatabase,
              AnalyticsTopic.unwrap(settings.topic),
              settings.common.lakehouseRoot,
              settings.streaming.checkpointLocation,
              settings.common.sparkLocalDirectory
            )
            .leftMap(new IllegalArgumentException(_))
        )
        configuration <- IO(Path.of(System.getProperty("config.file")).toAbsolutePath.normalize())
        state <- IO.blocking(read(configuration.getParent.resolve("state.json")))
        _ <- IO.blocking {
          val processes = ProcessHandle.allProcesses()
          try
            processes.iterator().asScala.filter(_.pid() != ProcessHandle.current().pid()).foreach { process =>
              val arguments = process.info().arguments()
              if (arguments.isPresent) {
                val values = arguments.get().toVector
                val matchesConfig = values.exists(value =>
                  value.startsWith("-Dconfig.file=") &&
                    value.contains("hiring-streaming-proof-" + nonce)
                )
                val ownerMain = values.exists(_.startsWith("com.example.hiring.analytics.cli."))
                require(!(matchesConfig && ownerMain && process.isAlive), "another fixture writer is alive")
              }
            }
          finally processes.close()
        }
        _ <- IO.fromEither(
          StreamingActiveTerminationProofMain
            .validateFixtureNonce(settings.common.mongoDatabase, nonce, state.getString("nonce"))
            .leftMap(new IllegalArgumentException(_))
        )
        _ <- IO.blocking {
          safe(output)
          require(Files.isDirectory(output, LinkOption.NOFOLLOW_LINKS))
          require(output.getFileName.toString == boundary.toString)
          require(output.getParent.getFileName.toString == "process-recovery")
          require(output.getParent.getParent.getFileName.toString == "hiring-streaming-proof-" + nonce)
        }
        paths <- IO.fromEither(
          AnalyticsLakehousePaths
            .from(settings.common.lakehouseRoot)
            .toEither
            .leftMap(_ => new IllegalArgumentException("isolated paths required"))
        )
        checkpoint = Path
          .of(SparkPhysicalLocation.resolve(settings.streaming.checkpointLocation))
          .toAbsolutePath
          .normalize()
        lockId <- IO.fromEither(MongoAnalyticsLakehouseLock.lockId(settings.common.lakehouseRoot))
        streamOwnerRoot <- IO.fromEither(AnalyticsStreamingRegistry.ownerLockRoot(settings.common.lakehouseRoot))
        streamLockId <- IO.fromEither(MongoAnalyticsLakehouseLock.lockId(streamOwnerRoot))
        ready = output.resolve("ready.json")
        _ <- args.head match {
          case "seed" =>
            for {
              _ <- IO.blocking(require(!Files.exists(output.resolve("seed.json")), "fixture already seeded"))
              credentials <- IO.fromEither(
                ConfigSource.default
                  .at("analytics.workload-producer")
                  .load[ProducerCredentials]
                  .leftMap(_ => new IllegalArgumentException("proof producer required"))
              )
              connection = settings.common.kafka
                .copy(saslUsername = Some(credentials.username), saslPassword = Some(credentials.password))
              properties <- IO.fromEither(KafkaClientProperties.clientProperties(connection))
              producerSettings = new Properties()
              _ <- IO {
                producerSettings.put("bootstrap.servers", connection.bootstrapServers)
                properties.foreach { case (key, value) => producerSettings.put(key, value) }
                producerSettings.put("key.serializer", classOf[StringSerializer].getName)
                producerSettings.put("value.serializer", classOf[StringSerializer].getName)
                producerSettings.put("acks", "all")
                producerSettings.put("enable.idempotence", "true")
                producerSettings.put("transactional.id", "hiring-publisher-process-recovery-" + nonce)
                producerSettings.put("max.block.ms", "10000")
                producerSettings.put("request.timeout.ms", "10000")
                producerSettings.put("delivery.timeout.ms", "20000")
              }
              offsets <- Resource
                .make(IO.blocking(new KafkaProducer[String, String](producerSettings)))(producer =>
                  IO.blocking(producer.close(java.time.Duration.ofSeconds(5)))
                )
                .use { producer =>
                  IO.blocking {
                    producer.initTransactions()
                    producer.beginTransaction()
                    val at =
                      if (boundary == StreamingDurableBoundary.LateFactsCommitted)
                        Instant.now().minusSeconds(3 * 86400L)
                      else Instant.now()
                    val delivered = (0 until 12).toVector.map { index =>
                      val event = UUID.randomUUID().toString
                      val actor = UUID.randomUUID().toString
                      val body =
                        s"""{"eventId":"$event","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"$event","actorId":"$actor","payload":{"job":{"skills":["recovery$nonce"]}}}"""
                      val metadata = producer
                        .send(
                          new ProducerRecord[String, String](
                            AnalyticsTopic.unwrap(settings.topic),
                            Int.box(index % 3),
                            event,
                            body
                          )
                        )
                        .get(30, TimeUnit.SECONDS)
                      new Document("partition", metadata.partition())
                        .append("offset", metadata.offset())
                        .append("eventId", event)
                        .append("eventFingerprint", AnalyticsDigest.sha256Hex(body.getBytes(UTF_8)))
                    }
                    producer.commitTransaction()
                    delivered
                  }
                }
              _ <- write(
                output.resolve("seed.json"),
                new Document("nonce", nonce)
                  .append("coordinates", offsets.asJava)
                  .append("records", 12)
                  .append("committed", true)
              )
            } yield ()
          case "pause" =>
            val observer = new StreamingDurableBoundaryObserver[IO] {
              override def completed(actual: StreamingDurableBoundary, batch: StreamingBatchIdentity): IO[Unit] =
                if (actual != boundary) IO.unit
                else
                  for {
                    session <- IO.blocking(
                      SparkSession.getActiveSession
                        .orElse(SparkSession.getDefaultSession)
                        .getOrElse(throw new IllegalStateException("original Spark session unavailable"))
                    )
                    store = new DeltaStreamingBatchJournal[IO](session, paths, execution)
                    journal <- store
                      .load(batch)
                      .flatMap(_.liftTo[IO](new IllegalStateException("durable preparation absent")))
                    records = journal.preparation.deliveredOffsets.map(_.deliveredRecordCount).sum
                    _ <- IO(require(records > 0L, "crash barrier requires a nonempty batch"))
                    _ <- IO.blocking {
                      val required = boundary match {
                        case StreamingDurableBoundary.BronzeCommitted    => Some(paths.bronze)
                        case StreamingDurableBoundary.SilverCommitted    => Some(paths.silver)
                        case StreamingDurableBoundary.LateFactsCommitted => Some(paths.lateFacts)
                        case _                                           => None
                      }
                      required.foreach { path =>
                        require(io.delta.tables.DeltaTable.isDeltaTable(session, path), "native durable dataset absent")
                        val seed = read(output.resolve("seed.json"))
                        require(seed.getString("nonce") == nonce && seed.getBoolean("committed"))
                        val (_, identities) =
                          bronzeEvidence(session, paths, AnalyticsTopic.unwrap(settings.topic), seed)
                        if (boundary == StreamingDurableBoundary.SilverCommitted)
                          requireAdmittedIdentities(
                            admittedIdentities(session.read.format("delta").load(path), identities),
                            identities
                          )
                        else {
                          val expected = seedCoordinates(seed).toSet
                          val predicate = expected.toVector
                            .map { case (partition, offset) =>
                              (col("topic") === AnalyticsTopic.unwrap(settings.topic)) && (col(
                                "partition"
                              ) === partition) && (col("offset") === offset)
                            }
                            .reduce(_ || _)
                          val rows = session.read
                            .format("delta")
                            .load(path)
                            .filter(predicate)
                            .select("partition", "offset")
                            .limit(13)
                            .collect()
                            .toVector
                          require(
                            rows.size == 12 && rows.map(row => row.getInt(0) -> row.getLong(1)).toSet == expected,
                            "durable barrier differs from committed coordinates"
                          )
                          if (boundary == StreamingDurableBoundary.LateFactsCommitted)
                            requireAdmittedIdentities(
                              admittedIdentities(session.read.format("delta").load(path), identities),
                              identities
                            )
                        }
                      }
                      if (boundary == StreamingDurableBoundary.IngestionCommitted) require(journal.ingestionCommitted)
                      if (boundary == StreamingDurableBoundary.TerminalCommitted)
                        require(journal.terminalOutcome.contains(StreamingTerminalOutcome.Published))
                    }
                    _ <- IO.blocking {
                      safe(checkpoint.resolve("commits/" + batch.batchId.value))
                      require(
                        !Files.exists(checkpoint.resolve("commits/" + batch.batchId.value)),
                        "checkpoint already acknowledged"
                      )
                    }
                    owners <- IO.blocking {
                      val client = MongoClients.create(settings.common.mongoUri)
                      try {
                        if (
                          boundary == StreamingDurableBoundary.PublicationCommitted || boundary == StreamingDurableBoundary.TerminalCommitted
                        ) {
                          val decision = journal.latestDecision.getOrElse(
                            throw new IllegalStateException("durable publication decision absent")
                          )
                          val control = Option(
                            client
                              .getDatabase(settings.common.mongoDatabase)
                              .withReadConcern(ReadConcern.MAJORITY)
                              .getCollection("analytics_report_control")
                              .find(new Document("_id", "analytics-report"))
                              .maxTime(5, TimeUnit.SECONDS)
                              .first()
                          )
                            .getOrElse(throw new IllegalStateException("durable publication control absent"))
                          require(
                            control.getString("state") == "Published" &&
                              control.getLong("generation").longValue() == decision.publicationReservation.generation &&
                              control
                                .getLong("lastPublishedRevision")
                                .longValue() == decision.publicationReservation.revision &&
                              control.getString("lastRunId") == decision.publicationReservation.runId.value
                          )
                        }
                        Vector(lockId, streamLockId).map { id =>
                          val owner = Option(
                            client
                              .getDatabase(settings.common.mongoDatabase)
                              .withReadConcern(ReadConcern.MAJORITY)
                              .getCollection("analytics_lakehouse_mutexes")
                              .find(new Document("_id", id))
                              .maxTime(5, TimeUnit.SECONDS)
                              .first()
                          ).getOrElse(throw new IllegalStateException("original mutex absent"))
                          new Document("lockId", id).append("ownerToken", owner.getString("ownerToken"))
                        }
                      } finally client.close()
                    }
                    _ <- write(
                      ready,
                      new Document("nonce", nonce)
                        .append("boundary", boundary.toString)
                        .append("pid", ProcessHandle.current().pid())
                        .append("lineage", batch.lineage.value)
                        .append("batchId", batch.batchId.value)
                        .append("records", records)
                        .append("mutexOwners", owners.asJava)
                        .append("inputFingerprint", journal.preparation.inputFingerprint.value)
                        .append("priorWatermark", journal.preparation.priorWatermark.map(_.toString).orNull)
                        .append("checkpointAcknowledged", false)
                        .append("durableOutcome", journal.terminalOutcome.map(_.toString).orNull)
                    )
                    _ <- IO.never[Unit]
                  } yield ()
            }
            AppModule.streamingObserved[IO](settings, Some(observer)).use(_.run)
          case "release-owner" =>
            for {
              owners <- IO.blocking {
                val recorded = read(ready)
                require(recorded.getString("nonce") == nonce && recorded.getString("boundary") == boundary.toString)
                val pid = recorded.get("pid", classOf[java.lang.Number]).longValue()
                require(
                  pid != ProcessHandle.current().pid() && !ProcessHandle.of(pid).isPresent,
                  "original owner still exists"
                )
                capturedMutexes(recorded, lockId, streamLockId)
                  .fold(problem => throw new IllegalArgumentException(problem), values => values)
              }
              _ <- Resource
                .make(IO.blocking(MongoClients.create(settings.common.mongoUri)))(client => IO.blocking(client.close()))
                .use { client =>
                  val mutexes = client
                    .getDatabase(settings.common.mongoDatabase)
                    .withReadConcern(ReadConcern.MAJORITY)
                    .getCollection("analytics_lakehouse_mutexes")
                    .withWriteConcern(WriteConcern.MAJORITY.withJournal(true).withWTimeout(15, TimeUnit.SECONDS))
                  removeCapturedMutexes(owners)(
                    owner =>
                      IO.blocking {
                        require(
                          mutexes
                            .find(new Document("_id", owner.lockId).append("ownerToken", owner.ownerToken))
                            .maxTime(5, TimeUnit.SECONDS)
                            .first() != null,
                          "captured stale owner changed or absent"
                        )
                      },
                    owner =>
                      IO.blocking {
                        val result =
                          mutexes.deleteOne(new Document("_id", owner.lockId).append("ownerToken", owner.ownerToken))
                        require(result.getDeletedCount == 1L, "exact stale owner was not removed")
                      },
                    owner =>
                      write(
                        output.resolve("owner-removed-" + owner.lockId + ".json"),
                        new Document("nonce", nonce)
                          .append("lockId", owner.lockId)
                          .append("originalProcessAbsent", true)
                          .append("exactOwnerRemoved", true)
                      )
                  )
                }
              _ <- write(
                output.resolve("owner-recovered.json"),
                new Document("nonce", nonce).append("originalProcessAbsent", true).append("exactOwnersRemoved", 2)
              )
            } yield ()
          case "recover" | "inspect" =>
            for {
              recorded <- IO.blocking(read(ready))
              _ <- IO(
                require(recorded.getString("nonce") == nonce && recorded.getString("boundary") == boundary.toString)
              )
              batch <- IO(identity(recorded))
              await = {
                def loop: IO[Unit] = IO
                  .blocking(
                    SparkSession.getActiveSession
                      .orElse(SparkSession.getDefaultSession)
                      .exists(session =>
                        !session.sparkContext.isStopped && session.streams.active.exists(_.isActive)
                      ) &&
                      Files.isRegularFile(checkpoint.resolve("commits/" + batch.batchId.value))
                  )
                  .flatMap(if (_) IO.unit else IO.sleep(100.millis) *> loop)
                loop.timeout(5.minutes)
              }
              _ <- diagnostic.set(DiagnosticStage.Query)
              _ <-
                if (args.head == "inspect") IO.blocking {
                  safe(checkpoint.resolve("commits/" + batch.batchId.value))
                  require(
                    Files.isRegularFile(checkpoint.resolve("commits/" + batch.batchId.value)),
                    "inspection requires native commit"
                  )
                }
                else
                  IO.race(AppModule.streaming[IO](settings).use(_.run), await).flatMap {
                    case Right(_) => IO.unit
                    case Left(_)  =>
                      IO.raiseError(new IllegalStateException("query terminated before recovered checkpoint commit"))
                  }
              _ <- AppModule
                .sparkMongo[IO](
                  settings.common.mongoUri,
                  "local[2]",
                  "HiringAnalyticsProcessRecoveryReadback",
                  Some(false),
                  settings.common.sparkLocalDirectory
                )
                .use { case (spark, client, driver) =>
                  val store = new DeltaStreamingBatchJournal[IO](spark, paths, execution)
                  for {
                    _ <- diagnostic.set(DiagnosticStage.Journal)
                    journal <- store
                      .load(batch)
                      .flatMap(_.liftTo[IO](new IllegalStateException("recovered journal absent")))
                    _ <- diagnostic.set(DiagnosticStage.Terminal)
                    _ <- IO(require(journal.terminalOutcome.contains(StreamingTerminalOutcome.Published)))
                    _ <- diagnostic.set(DiagnosticStage.Fingerprint)
                    _ <- IO(
                      require(journal.preparation.inputFingerprint.value == recorded.getString("inputFingerprint"))
                    )
                    watermark <- store.latestWatermark(batch.lineage)
                    database <- client.getDatabase(settings.common.mongoDatabase)
                    publisher = new MongoAnalyticsReportPublisher[IO](client, database, settings.common.operational)
                    decision <- IO.fromOption(journal.latestDecision)(
                      new IllegalStateException("recovered decision absent")
                    )
                    _ <- diagnostic.set(DiagnosticStage.Receipt)
                    receipt <- publisher.publicationReceipt(decision.publicationReservation)
                    _ <- IO(require(receipt == AnalyticsReportPublicationReceipt.CurrentGeneration))
                    _ <- diagnostic.set(DiagnosticStage.Report)
                    reportFingerprint <- IO.blocking {
                      val native = MongoClients.create(settings.common.mongoUri)
                      try {
                        val current = Option(
                          native
                            .getDatabase(settings.common.mongoDatabase)
                            .withReadConcern(ReadConcern.MAJORITY)
                            .getCollection("analytics_report_snapshots")
                            .find(new Document("_id", "current"))
                            .maxTime(5, TimeUnit.SECONDS)
                            .first()
                        ).getOrElse(throw new IllegalStateException("current report absent"))
                        AnalyticsDigest.sha256Hex(current.toJson.getBytes(UTF_8))
                      } finally native.close()
                    }
                    _ <- diagnostic.set(DiagnosticStage.Coordinates)
                    snapshot <- driver {
                      def coordinates(path: String): String = {
                        if (!io.delta.tables.DeltaTable.isDeltaTable(spark, path)) "absent"
                        else {
                          val rows = spark.read
                            .format("delta")
                            .load(path)
                            .select("topic", "partition", "offset")
                            .orderBy("topic", "partition", "offset")
                            .limit(2001)
                            .collect()
                            .toVector
                          require(
                            rows.size <= 2000 && rows.distinct.size == rows.size,
                            "coordinate bound or uniqueness failed"
                          )
                          AnalyticsDigest.sha256Hex(
                            rows.map(_.toString).mkString("\n").getBytes(UTF_8)
                          ) + ":" + rows.size
                        }
                      }
                      val seeded = read(output.resolve("seed.json"))
                      require(seeded.getString("nonce") == nonce && seeded.getBoolean("committed"))
                      val (_, expectedIdentities) =
                        bronzeEvidence(spark, paths, AnalyticsTopic.unwrap(settings.topic), seeded)
                      val admitted = Vector(paths.silver, paths.lateFacts).flatMap { path =>
                        if (!io.delta.tables.DeltaTable.isDeltaTable(spark, path)) Vector.empty
                        else admittedIdentities(spark.read.format("delta").load(path), expectedIdentities)
                      }
                      requireAdmittedIdentities(admitted, expectedIdentities)
                      val expectedCoordinates = seedCoordinates(seeded)
                      require(
                        expectedCoordinates.forall { case (partition, offset) =>
                          journal.preparation.deliveredOffsets.exists(summary =>
                            AnalyticsTopic.unwrap(summary.topic) == AnalyticsTopic.unwrap(settings.topic) &&
                              AnalyticsPartition.unwrap(summary.partition) == partition &&
                              AnalyticsOffset.unwrap(summary.minimumDeliveredOffset) <= offset &&
                              AnalyticsOffset.unwrap(summary.maximumDeliveredOffset) >= offset
                          )
                        },
                        "seed coordinates outside durable delivered offsets"
                      )
                      if (boundary == StreamingDurableBoundary.LateFactsCommitted) {
                        val predicate = expectedCoordinates
                          .map { case (partition, offset) =>
                            (col("topic") === AnalyticsTopic
                              .unwrap(settings.topic)) && (col("partition") === partition) && (col("offset") === offset)
                          }
                          .reduce(_ || _)
                        val lateCoordinates = spark.read
                          .format("delta")
                          .load(paths.lateFacts)
                          .filter(predicate)
                          .select("partition", "offset")
                          .limit(13)
                          .collect()
                          .toVector
                          .map(row => row.getInt(0) -> row.getLong(1))
                        require(lateCoordinates.size == 12 && lateCoordinates.toSet == expectedCoordinates.toSet)
                      }

                      inspectionRequire(
                        journal.preparation.deliveredOffsets.map(_.deliveredRecordCount).sum == 12L,
                        DiagnosticStage.Counts
                      )
                      def identitySnapshot(path: String): String = {
                        val rows = spark.read
                          .format("delta")
                          .load(path)
                          .select("eventId", "eventFingerprint")
                          .orderBy("eventId", "eventFingerprint")
                          .limit(2001)
                          .collect()
                          .toVector
                        require(rows.size <= 2000 && rows.map(_.getString(0)).distinct.size == rows.size)
                        AnalyticsDigest.sha256Hex(rows.map(_.toString).mkString("\n").getBytes(UTF_8)) + ":" + rows.size
                      }
                      inspectionRequire(
                        watermark == journal.latestDecision
                          .flatMap(_.candidateWatermark)
                          .orElse(journal.preparation.priorWatermark),
                        DiagnosticStage.Watermark
                      )
                      new Document("nonce", nonce)
                        .append("batchId", batch.batchId.value)
                        .append("bronze", coordinates(paths.bronze))
                        .append("silver", identitySnapshot(paths.silver))
                        .append("lateIdentities", identitySnapshot(paths.lateFacts))
                        .append("lateFacts", coordinates(paths.lateFacts))
                        .append("watermark", watermark.map(_.toString).orNull)
                        .append("outcome", journal.terminalOutcome.map(_.toString).orNull)
                        .append("publicationGeneration", decision.publicationReservation.generation)
                        .append("publicationRevision", decision.publicationReservation.revision)
                        .append("publicationReceipt", receipt.toString)
                        .append("reportFingerprint", reportFingerprint)
                    }
                    _ <- diagnostic.set(DiagnosticStage.Snapshot)
                    result = output.resolve(snapshotFile(args.head))
                    _ <-
                      if (args.head == "inspect") write(result, snapshot)
                      else
                        IO.blocking(Files.exists(result)).flatMap {
                          case false => write(result, snapshot)
                          case true  =>
                            IO.blocking(
                              require(sameSnapshot(read(result), snapshot), "second restart changed native snapshot")
                            ) *>
                              write(
                                output.resolve("second-restart.json"),
                                new Document("nonce", nonce).append("identicalNativeSnapshot", true)
                              )
                        }
                  } yield ()
                }
            } yield ()
          case _ => IO.raiseError(new IllegalArgumentException("invalid recovery operation"))
        }
      } yield ExitCode.Success
      program.handleErrorWith(error =>
        diagnostic.get.flatMap(stage => IO.println(failureDiagnostic(stage, error))).as(ExitCode.Error)
      )
    }
}
