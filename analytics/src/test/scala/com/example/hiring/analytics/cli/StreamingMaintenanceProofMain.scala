package com.example.hiring.analytics.cli

import cats.effect.{IO, IOApp, ExitCode, Ref, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.{AnalyticsRuntimeConfig, AnalyticsStreamingRuntimeSettings}
import com.example.hiring.analytics.cli.StreamingProofIsolation
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
import cats.effect.syntax.all.*
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.mongodb.ReadConcern
import com.mongodb.client.{MongoClients, MongoDatabase}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, get_json_object}
import org.bson.Document
import pureconfig.{ConfigReader, ConfigSource}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.time.Instant
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Private, independently reviewed fixture runner. All processing/pruning belongs to production AppModule. */
object StreamingMaintenanceProofMain extends IOApp {
  private case class Credentials(username: String, password: String) {
    override def toString = "Credentials([REDACTED])"
  }
  private given ConfigReader[Credentials] = ConfigReader.forProduct2("username", "password")(Credentials.apply)
  private case class Api(url: String, nonce: String, password: String) { override def toString = "Api([REDACTED])" }
  private given ConfigReader[Api] = ConfigReader.forProduct3("url", "nonce", "password")(Api.apply)
  private def check(value: Boolean): Unit = require(value, "STREAMING_MAINTENANCE_INVARIANT_REJECTED")
  private def sha(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
  private def safe(path: Path): Unit = {
    check(path.isAbsolute && path.normalize == path)
    (path +: path.iterator().asScala.scanLeft(path.getRoot)((parent, part) => parent.resolve(part)).toVector)
      .foreach(value => check(!Files.isSymbolicLink(value)))
  }
  private def emit(directory: Path, name: String, value: Document): IO[Unit] = IO.blocking {
    safe(directory)
    check(Files.isDirectory(directory))
    val file = directory.resolve(name)
    Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    Files.writeString(file, value.toJson, StandardOpenOption.WRITE)
    ()
  }
  private def http(api: Api, operation: Document, token: Option[String]): Document = {
    val uri = URI.create(api.url.stripSuffix("/") + "/graphql")
    check(uri.getScheme == "http" && uri.getHost == "127.0.0.1" && uri.getUserInfo == null)
    val builder = HttpRequest
      .newBuilder(uri)
      .timeout(java.time.Duration.ofSeconds(10))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(operation.toJson))
    token.foreach(value => builder.header("Authorization", "Bearer " + value))
    val response = HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofInputStream())
    val input = response.body()
    val bytes = try input.readNBytes(1024 * 1024 + 1)
    finally input.close()
    check(response.statusCode() == 200 && bytes.length <= 1024 * 1024)
    Document.parse(new String(bytes, StandardCharsets.UTF_8))
  }
  private def login(api: Api): String = {
    val request = new Document(
      "query",
      "mutation($input: LoginInput!) { login(input: $input) { __typename ... on AuthSuccess { accessToken } } }"
    )
      .append(
        "variables",
        new Document(
          "input",
          new Document("idempotencyKey", UUID.randomUUID().toString)
            .append("name", "Proof Admin " + api.nonce)
            .append("password", api.password)
        )
      )
    val result = http(api, request, None).get("data", classOf[Document]).get("login", classOf[Document])
    check(result.getString("__typename") == "AuthSuccess"); result.getString("accessToken")
  }
  private def admin(api: Api, token: String, skill: String, expected: Long): Option[Instant] = {
    val day = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.DAYS)
    val query = s"""{ analyticsReport(from: "${day.minusSeconds(86400)}", to: "${day.plusSeconds(
        86400
      )}") { __typename asOf skillPostingActivity { skill postings } } }"""
    val response = http(api, new Document("query", query), Some(token))
    Option(response.get("data", classOf[Document]))
      .flatMap(data => Option(data.get("analyticsReport", classOf[Document])))
      .filter(_.getString("__typename") == "AnalyticsReport")
      .filter { report =>
        Option(report.getList("skillPostingActivity", classOf[Document])).toVector
          .flatMap(_.asScala)
          .filter(_.getString("skill") == skill)
          .map(_.get("postings", classOf[java.lang.Number]).longValue())
          .sum == expected
      }
      .map(row => Instant.parse(row.getString("asOf")))
  }
  private def report(database: MongoDatabase): Option[Document] = Option(
    database
      .getCollection("analytics_report_snapshots")
      .find(new Document("_id", "current").append("state", "Published"))
      .first()
  ).filter(_.getDate("expiresAt").toInstant.isAfter(Instant.now()))
  private def grant(database: MongoDatabase, id: String): Document = {
    val value = database.getCollection("analytics_streaming_activation").find(new Document("_id", id)).first()
    check(value != null); value
  }
  private def waitFor[A](test: IO[Option[A]], maximum: FiniteDuration): IO[A] = {
    def loop: IO[A] = test.flatMap {
      case Some(value) => IO.pure(value); case None => IO.sleep(2.seconds) *> IO.defer(loop)
    }
    loop.timeout(maximum)
  }
  private def publish(
      producer: KafkaProducer[String, String],
      topic: String,
      skill: String
  ): IO[Vector[(String, Int, Long)]] = IO.blocking {
    val at = Instant.now().toString
    producer.beginTransaction()
    try {
      val records = (0 until 12).toVector.map { index =>
        val id = UUID.randomUUID().toString; val subject = UUID.randomUUID().toString;
        val job = UUID.randomUUID().toString
        val event = new Document("eventId", id)
          .append("eventType", "JOB_CREATED")
          .append("occurredAt", at)
          .append("aggregateType", "Job")
          .append("aggregateId", job)
          .append("actorId", subject)
          .append("payload", new Document("jobId", job).append("job", new Document("skills", List(skill).asJava)))
        val metadata = producer
          .send(new ProducerRecord[String, String](topic, index % 3, subject, event.toJson))
          .get(20, TimeUnit.SECONDS)
        (id, metadata.partition(), metadata.offset())
      }
      producer.commitTransaction(); records
    } catch { case error: Throwable => producer.abortTransaction(); throw error }
  }
  private def metadataHash(database: MongoDatabase): String = sha(
    List("analytics_report_control", "analytics_report_snapshots")
      .map(name =>
        database
          .getCollection(name)
          .find()
          .sort(new Document("_id", 1))
          .limit(3)
          .into(new java.util.ArrayList[Document]())
          .asScala
          .map(_.toJson)
          .mkString
      )
      .mkString("\n")
      .getBytes(StandardCharsets.UTF_8)
  )

  private def observeCohort(
      spark: SparkSession,
      database: MongoDatabase,
      producer: KafkaProducer[String, String],
      api: Api,
      skill: String,
      settings: AnalyticsStreamingRuntimeSettings,
      lakehouse: Path,
      checkpoint: Path,
      execution: SparkBlockingExecution[IO],
      index: Int,
      output: Path,
      nonce: String
  ): IO[StreamingMaintenanceObservations.Snapshot] = for {
    cohort <- publish(producer, AnalyticsTopic.unwrap(settings.topic), skill)
    token <- IO.blocking(login(api))
    snapshot <- waitFor(
      execution {
        val paths = Vector(
          "bronze/operational_events",
          "silver/operational_events",
          "control/streaming_progress",
          "control/streaming_decisions"
        )
        if (!paths.forall(name => io.delta.tables.DeltaTable.isDeltaTable(spark, lakehouse.resolve(name).toString)))
          None
        else {
          val bronze = spark.read.format("delta").load(lakehouse.resolve(paths(0)).toString)
          val silver = spark.read.format("delta").load(lakehouse.resolve(paths(1)).toString)
          val coordinates = cohort
            .map { case (id, partition, offset) =>
              (col("partition") === lit(partition)) && (col("offset") === lit(offset)) && (get_json_object(
                col("rawValue"),
                "$.eventId"
              ) === lit(id))
            }
            .reduce(_ || _)
          val exactBronze =
            bronze.filter(col("topic") === lit(AnalyticsTopic.unwrap(settings.topic))).filter(coordinates)
          val exactSilver = silver.filter(col("eventId").isin(cohort.map(_._1)*))
          if (
            exactBronze.count() != 12L || exactBronze.select("partition", "offset").distinct().count() != 12L ||
            exactSilver.count() != 12L || exactSilver.select("eventId").distinct().count() != 12L
          ) None
          else {
            val snapshot = StreamingMaintenanceObservations.capture(spark, lakehouse, checkpoint)
            val bound = StreamingMaintenanceObservations.bindCohort(
              snapshot.progress,
              cohort.map { case (_, partition, offset) => (AnalyticsTopic.unwrap(settings.topic), partition, offset) }
            )
            val stored = report(database)
            val control = Option(
              database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report")).first()
            )
            val visible = admin(api, token, skill, (index + 1L) * 12L)
            val publication = bound.flatMap(_.traverse { row =>
              val decisions = spark.read
                .format("delta")
                .load(lakehouse.resolve(paths(3)).toString)
                .filter((col("lineage") === lit(row.lineage)) && (col("batchId") === lit(row.batchId)))
                .orderBy(col("revision").desc)
                .select("publicationRunId", "publicationGeneration", "publicationRevision")
                .limit(1)
                .collect()
                .headOption
              decisions.flatMap { value =>
                val cohortReceipt = Option(
                  database
                    .getCollection("analytics_report_runs")
                    .find(new Document("_id", value.getString(0)))
                    .maxTime(5, TimeUnit.SECONDS)
                    .first()
                )
                val durableCohort = cohortReceipt.exists(receipt =>
                  receipt.getString("state") == "Published" &&
                    receipt.get("generation", classOf[java.lang.Number]).longValue() == value.getLong(1) &&
                    receipt.get("revision", classOf[java.lang.Number]).longValue() == value.getLong(2)
                )
                stored.flatMap { snapshot =>
                  control.flatMap { current =>
                    val generation = snapshot.get("generation", classOf[java.lang.Number]).longValue()
                    val revision = snapshot.get("revision", classOf[java.lang.Number]).longValue()
                    val matched = durableCohort && current.getString("state") == "Published" &&
                      current.getString("lastRunId") == snapshot.getString("runId") &&
                      current.get("generation", classOf[java.lang.Number]).longValue() == generation &&
                      current.get("lastPublishedRevision", classOf[java.lang.Number]).longValue() == revision &&
                      StreamingMaintenanceObservations.publicationCovers(
                        value.getLong(1),
                        value.getLong(2),
                        generation,
                        revision
                      ) &&
                      (revision > value.getLong(2) || snapshot.getString("runId") == value.getString(0)) &&
                      visible.contains(snapshot.getDate("asOf").toInstant)
                    if (matched) Some(revision > value.getLong(2)) else None
                  }
                }
              }
            })
            if (bound.exists(_.forall(row => snapshot.nativeCommits.contains(row.batchId))))
              publication.map(superseded =>
                snapshot.copy(
                  publicationSuperseded = superseded.exists(identity),
                  cohortBatchIds = bound.toVector.flatten.map(_.batchId).sorted
                )
              )
            else None
          }
        }
      },
      3.minutes
    )
    _ <- emit(
      output,
      "cohort-" + index + ".json",
      new Document("nonce", nonce)
        .append("cohortIndex", index)
        .append("actualBatchIds", snapshot.cohortBatchIds.map(Long.box).asJava)
        .append("records", 12)
        .append("partitions", 3)
        .append("exactBronzeCoordinates", true)
        .append("exactSilverEventIds", true)
        .append("publishedSourceEndsCoverCoordinates", true)
        .append("durableCohortPublishedReceipt", true)
        .append("currentControlSnapshotAndAdminMatch", true)
        .append("publicationSuperseded", snapshot.publicationSuperseded)
        .append("nativeCommitted", true)
        .append("nativePublished", true)
        .append("adminPostings", (index + 1) * 12)
    )
  } yield snapshot

  override def run(args: List[String]): IO[ExitCode] = (for {
    _ <- IO(check(args.isEmpty))
    settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
    api <- IO.fromEither(
      ConfigSource.default
        .at("analytics.workload-api")
        .load[Api]
        .leftMap(_ => new IllegalArgumentException("API settings required"))
    )
    credentials <- IO.fromEither(
      ConfigSource.default
        .at("analytics.workload-producer")
        .load[Credentials]
        .leftMap(_ => new IllegalArgumentException("producer settings required"))
    )
    namespace <- IO.fromEither(
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
    nonce = settings.common.mongoDatabase.stripPrefix("hiring_streaming_proof_")
    configuration <- IO(Paths.get(System.getProperty("config.file")).toAbsolutePath.normalize())
    output = configuration.getParent.getParent.getParent
      .resolve("logs")
      .resolve("hiring-streaming-proof-" + nonce)
      .resolve("maintenance")
    lakehouse = Paths.get(URI.create(settings.common.lakehouseRoot))
    checkpoint = Paths.get(URI.create(settings.streaming.checkpointLocation))
    _ <- IO.blocking {
      check(api.nonce == nonce && settings.streaming.progressRetention == 60.seconds)
      check(configuration.getParent.getFileName.toString == "hiring-streaming-proof-" + nonce)
      safe(configuration); safe(output); safe(namespace)
      Vector(lakehouse, checkpoint).foreach(path => check(!Files.exists(path)))
      check(!Files.exists(output))
      Files.createDirectory(output, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")))
    }
    properties <- IO
      .fromEither(
        KafkaClientProperties.clientProperties(
          settings.common.kafka
            .copy(saslUsername = Some(credentials.username), saslPassword = Some(credentials.password))
        )
      )
      .map { values =>
        val props = new Properties(); props.setProperty("bootstrap.servers", settings.common.kafka.bootstrapServers)
        values.foreach { case (name, value) => props.setProperty(name, value) }
        props.setProperty("key.serializer", classOf[StringSerializer].getName);
        props.setProperty("value.serializer", classOf[StringSerializer].getName)
        props.setProperty("acks", "all"); props.setProperty("enable.idempotence", "true")
        props.setProperty("max.block.ms", "10000"); props.setProperty("request.timeout.ms", "10000");
        props.setProperty("delivery.timeout.ms", "20000")
        props.setProperty("transactional.id", "hiring-publisher-progress-" + nonce); props
      }
    original <- Ref.of[IO, Option[SparkSession]](None)
    grantBaseline <- Ref.of[IO, Option[String]](None)
    _ <- (
      Resource.make(IO.blocking(MongoClients.create(settings.common.mongoUri)))(client => IO.blocking(client.close())),
      Resource.make(IO.blocking(new KafkaProducer[String, String](properties)))(producer =>
        IO.blocking(producer.close(java.time.Duration.ofSeconds(5)))
      )
    ).tupled
      .use { case (client, producer) =>
        val database = client.getDatabase(settings.common.mongoDatabase).withReadConcern(ReadConcern.MAJORITY)
        val skill = "progressretention" + nonce
        val body = for {
          authorization <- IO.blocking(grant(database, settings.streaming.activationGrantId))
          _ <- grantBaseline.set(Some(sha(authorization.toJson.getBytes(StandardCharsets.UTF_8))))
          _ <- IO.blocking(producer.initTransactions())
          _ <- AppModule.streaming[IO](settings).use { program =>
            SparkBlockingExecution.resource[IO].use { execution =>
              for {
                spark <- IO.blocking(
                  SparkSession.getDefaultSession.getOrElse(throw new IllegalStateException("native session absent"))
                )
                _ <- execution.attachSparkContext(spark.sparkContext)
                _ <- execution {
                  check(spark.streams.active.isEmpty)
                  spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")
                  StreamingMaintenanceObservations.verifyNativeProfileBeforeQuery(spark, settings)
                }
                _ <- original.set(Some(spark))
                _ <- program.run.background.use { joined =>
                  val observation = for {
                    _ <- waitFor(IO.blocking(if (spark.streams.active.length == 1) Some(()) else None), 60.seconds)
                    _ <- emit(
                      output,
                      "ready.json",
                      new Document("nonce", nonce)
                        .append("pid", ProcessHandle.current().pid())
                        .append("nativeEffectiveMinBatchesToRetain", 2)
                        .append("typedProgressRetentionSeconds", 60)
                        .append("verifiedBeforeQuery", true)
                    )
                    initialInventory <- IO.blocking(
                      StreamingMaintenanceObservations.inventory(
                        lakehouse,
                        checkpoint,
                        StreamingMaintenanceObservations
                          .Snapshot(Vector.empty, Set.empty, Set.empty, Set.empty, Map.empty),
                        Instant.now()
                      )
                    )
                    snapshots <- (0 until 5).toVector.traverse(index =>
                      observeCohort(
                        spark,
                        database,
                        producer,
                        api,
                        skill,
                        settings,
                        lakehouse,
                        checkpoint,
                        execution,
                        index,
                        output,
                        nonce
                      )
                    )
                    baseline = snapshots(1)
                    before = snapshots.last
                    _ <- IO(
                      check(
                        before.cohortBatchIds.nonEmpty && before.cohortBatchIds.forall(id =>
                          before.nativeCommits.contains(id) && before.progress
                            .exists(row => row.batchId == id && row.outcome == "Published")
                        )
                      )
                    )
                    beforeInventory <- IO.blocking(
                      StreamingMaintenanceObservations.inventory(lakehouse, checkpoint, before, Instant.now())
                    )
                    after <- waitFor(
                      execution {
                        val snap = StreamingMaintenanceObservations.capture(spark, lakehouse, checkpoint)
                        val absent =
                          !snap.nativeOffsets.exists(Set(0L, 1L)) && !snap.nativeCommits.exists(Set(0L, 1L)) &&
                            !snap.progress.exists(row => Set(0L, 1L).contains(row.batchId)) && !snap.decisions
                              .exists(value => Set(0L, 1L).contains(value._2))
                        if (absent) Some(snap) else None
                      },
                      240.seconds
                    )
                    at <- IO.realTimeInstant
                    afterInventory <- IO.blocking(
                      StreamingMaintenanceObservations.inventory(lakehouse, checkpoint, after, at)
                    )
                    _ <- IO(
                      check(
                        StreamingMaintenanceObservations.boundedGrowth(
                          beforeInventory.get("files", classOf[java.lang.Number]).longValue(),
                          beforeInventory.get("bytes", classOf[java.lang.Number]).longValue(),
                          afterInventory.get("files", classOf[java.lang.Number]).longValue(),
                          afterInventory.get("bytes", classOf[java.lang.Number]).longValue()
                        )
                      )
                    )
                    pruning <- IO.blocking(StreamingMaintenanceObservations.verifyPruning(baseline, before, after, at))
                    _ <- emit(
                      output,
                      "pruning.json",
                      new Document("schema", "streaming-progress-retention-pruning-v1")
                        .append("nonce", nonce)
                        .append("beforeInventory", beforeInventory)
                        .append("afterInventory", afterInventory)
                        .append("observedAt", at.toString)
                        .append(
                          "oldCompletedAt",
                          baseline.progress
                            .filter(row => Set(0L, 1L).contains(row.batchId))
                            .sortBy(_.batchId)
                            .map(row =>
                              new Document("batchId", row.batchId).append("completedAt", row.completedAt.get.toString)
                            )
                            .asJava
                        )
                        .append(
                          "baselineDecisionIds",
                          baseline.decisions.map(_._2).toVector.sorted.map(Long.box).asJava
                        )
                        .append("remainingProgressIds", after.progress.map(_.batchId).sorted.map(Long.box).asJava)
                        .append("remainingDecisionIds", after.decisions.map(_._2).toVector.sorted.map(Long.box).asJava)
                        .append("nativeOffsetIds", after.nativeOffsets.toVector.sorted.map(Long.box).asJava)
                        .append("nativeCommitIds", after.nativeCommits.toVector.sorted.map(Long.box).asJava)
                        .append("permanentLineageUnchanged", true)
                        .append("latestTerminalAndWatermarkRetained", true)
                    )
                    continued <- observeCohort(
                      spark,
                      database,
                      producer,
                      api,
                      skill,
                      settings,
                      lakehouse,
                      checkpoint,
                      execution,
                      5,
                      output,
                      nonce
                    )
                    _ <- IO(
                      check(
                        continued.cohortBatchIds.nonEmpty && continued.cohortBatchIds
                          .forall(id => id > after.nativeCommits.max && continued.nativeCommits.contains(id))
                      )
                    )
                    _ <- IO.blocking(StreamingMaintenanceObservations.verifyContinuation(after, continued))
                    finalInventory <- IO.blocking(
                      StreamingMaintenanceObservations.inventory(lakehouse, checkpoint, continued, Instant.now())
                    )
                    _ <- IO(
                      check(
                        StreamingMaintenanceObservations.boundedGrowth(
                          initialInventory.get("files", classOf[java.lang.Number]).longValue(),
                          initialInventory.get("bytes", classOf[java.lang.Number]).longValue(),
                          finalInventory.get("files", classOf[java.lang.Number]).longValue(),
                          finalInventory.get("bytes", classOf[java.lang.Number]).longValue()
                        )
                      )
                    )
                    _ <- emit(
                      output,
                      "observed.json",
                      new Document("nonce", nonce)
                        .append("schema", "streaming-progress-retention-observation-v1")
                        .append("initialInventory", initialInventory)
                        .append("finalInventory", finalInventory)
                        .append("storageGrowthFilesMax", 10000L)
                        .append("storageGrowthBytesMax", 1073741824L)
                        .append("transactions", 6)
                        .append("records", 72)
                        .append("nativeOldCheckpointAbsent", true)
                        .append("agedProgressAndDecisionsAbsent", pruning.oldCompletedRemoved)
                        .append("anchorsRetained", pruning.latestAnchorsRetained)
                        .append("unfinishedStatus", pruning.unfinishedStatus)
                        .append("continuationCommitted", true)
                        .append("adminPostings", 72)
                        .append("observedAt", at.toString)
                        .append("physicalRetentionClaim", false)
                        .append("healthyWorkloadClaim", false)
                    )
                  } yield ()
                  IO.race(joined.flatMap(_.embedNever), observation).flatMap {
                    case Left(_) =>
                      IO.raiseError[Unit](new IllegalStateException("native stream ended before observation"))
                    case Right(_) => IO.unit
                  }
                }
              } yield ()
            }
          }
        } yield ()
        body.guarantee {
          original.get.flatMap {
            case None        => IO.unit
            case Some(spark) =>
              for {
                expected <- grantBaseline.get
                first <- IO.blocking(metadataHash(database))
                _ <- IO.sleep(500.millis)
                second <- IO.blocking(metadataHash(database))
                _ <- IO.blocking {
                  check(spark.sparkContext.isStopped && spark.streams.active.isEmpty)
                  check(database.getCollection("analytics_lakehouse_mutexes").countDocuments() == 0L)
                  check(
                    expected.contains(
                      sha(grant(database, settings.streaming.activationGrantId).toJson.getBytes(StandardCharsets.UTF_8))
                    )
                  )
                  check(first == second)
                  val scratch = Paths.get(settings.common.sparkLocalDirectory)
                  if (Files.exists(scratch)) {
                    val entries = Files.newDirectoryStream(scratch);
                    try check(!entries.iterator().hasNext)
                    finally entries.close()
                  }
                }
                _ <- emit(
                  output,
                  "closed.json",
                  new Document("nonce", nonce)
                    .append("actualAppModuleReturned", true)
                    .append("originalContextStopped", true)
                    .append("originalQueryInactive", true)
                    .append("mutexCount", 0)
                    .append("ownedScratchAbsent", true)
                    .append("immutableGrantUnchanged", true)
                    .append("postStopReportMetadataStable", true)
                )
              } yield ()
          }
        }
      }
      .timeout(15.minutes)
    _ <- IO.println(
      "STREAMING_MAINTENANCE_RETENTION_PASS actualRecords=72 cohorts=6 nativePruning=true anchorsRetained=true continuation=true originalContextStopped=true physicalRetentionClaim=false"
    )
  } yield ExitCode.Success).handleErrorWith(error =>
    IO.println("PROGRESS_RETENTION_PROOF_FAILED category=" + error.getClass.getSimpleName).as(ExitCode.Error)
  )
}

import com.example.hiring.analytics.config.AnalyticsStreamingRuntimeSettings
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.*

/** Native runtime read-only observations. Read-only assertions; never starts a query or mutates its storage. */
private[analytics] object StreamingMaintenanceObservations {
  final case class DeliveredRange(topic: String, partition: Int, minimum: Long, maximum: Long)
  final case class Progress(
      lineage: String,
      batchId: Long,
      outcome: String,
      completedAt: Option[Instant],
      watermark: Option[Instant],
      observedAt: Option[Instant] = None,
      delivered: Vector[DeliveredRange] = Vector.empty,
      sourceEnds: Vector[(String, Int, Long)] = Vector.empty
  )
  final case class Snapshot(
      progress: Vector[Progress],
      decisions: Set[(String, Long)],
      nativeOffsets: Set[Long],
      nativeCommits: Set[Long],
      permanentLineageHashes: Map[String, String],
      publicationSuperseded: Boolean = false,
      cohortBatchIds: Vector[Long] = Vector.empty
  )
  final case class Result(oldCompletedRemoved: Boolean, latestAnchorsRetained: Boolean, unfinishedStatus: String)

  def bindCohort(progress: Vector[Progress], coordinates: Vector[(String, Int, Long)]): Option[Vector[Progress]] = {
    if (coordinates.isEmpty || coordinates.distinct.size != coordinates.size) None
    else
      coordinates
        .traverse { case (topic, partition, offset) =>
          val matching = progress.filter(row =>
            row.delivered.exists(range =>
              range.topic == topic && range.partition == partition && range.minimum <= offset && offset <= range.maximum
            )
          )
          matching match {
            case Vector(row)
                if row.outcome == "Published" && row.completedAt.nonEmpty &&
                  row.sourceEnds.exists { case (t, p, end) => t == topic && p == partition && end > offset } =>
              Some(row)
            case _ => None
          }
        }
        .map(_.distinct.sortBy(_.batchId))
  }

  def publicationCovers(
      cohortGeneration: Long,
      cohortRevision: Long,
      currentGeneration: Long,
      currentRevision: Long
  ): Boolean =
    currentGeneration == cohortGeneration && currentRevision >= cohortRevision

  def boundedGrowth(beforeFiles: Long, beforeBytes: Long, afterFiles: Long, afterBytes: Long): Boolean =
    Vector(beforeFiles, beforeBytes, afterFiles, afterBytes).forall(_ >= 0L) &&
      BigInt(afterFiles) - BigInt(beforeFiles) <= 10000L &&
      BigInt(afterBytes) - BigInt(beforeBytes) <= 1073741824L

  private val rowBound = 256
  private val checkpointEntryBound = 1024
  private val terminal = Set("Published", "QualityBlocked", "ErasurePending")
  private val unfinished = Set("Prepared", "IngestionCommitted")
  private def requireProof(value: Boolean): Unit = require(value, "PROGRESS_RETENTION_OBSERVATION_REJECTED")

  /** Call in the SAME AppModule Spark session before its program.run starts the native query. */
  def verifyNativeProfileBeforeQuery(spark: SparkSession, settings: AnalyticsStreamingRuntimeSettings): Unit = {
    requireProof(spark.version == "4.0.1")
    requireProof(settings.streaming.progressRetention == 60.seconds)
    requireProof(spark.conf.get("spark.sql.streaming.minBatchesToRetain") == "2")
    requireProof(spark.conf.get("spark.sql.shuffle.partitions") == "2")
    requireProof(spark.conf.get("spark.databricks.delta.snapshotPartitions") == "2")
    requireProof(spark.streams.active.isEmpty)
  }

  private def nativeIds(directory: Path): Set[Long] = {
    requireProof(Files.isDirectory(directory) && !Files.isSymbolicLink(directory))
    val stream = Files.newDirectoryStream(directory)
    try {
      val iterator = stream.iterator()
      var inspected = 0
      var ids = Set.empty[Long]
      while (iterator.hasNext) {
        requireProof(inspected < checkpointEntryBound)
        val path = iterator.next()
        inspected += 1
        requireProof(!Files.isSymbolicLink(path) && Files.isRegularFile(path))
        val name = path.getFileName.toString
        if (name.matches("[0-9]+")) ids += name.toLong
        else requireProof(name.matches("\\.[0-9]+\\.crc"))
      }
      ids
    } finally stream.close()
  }

  private def permanentHashes(directory: Path): Map[String, String] = {
    requireProof(Files.isDirectory(directory) && !Files.isSymbolicLink(directory))
    val stream = Files.newDirectoryStream(directory)
    try {
      val iterator = stream.iterator()
      var result = Map.empty[String, String]
      while (iterator.hasNext) {
        requireProof(result.size < 16)
        val file = iterator.next()
        requireProof(Files.isRegularFile(file) && !Files.isSymbolicLink(file) && Files.size(file) <= 65536L)
        val bytes = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))
        result += file.getFileName.toString -> bytes.map(byte => f"${byte & 0xff}%02x").mkString
      }
      requireProof(result.nonEmpty)
      result
    } finally stream.close()
  }

  /** Caller owns exact nonce/root/ancestor/UID/config/provenance checks and runs this through managed Spark blocking
    * IO.
    */
  def capture(spark: SparkSession, lakehouse: Path, checkpoint: Path): Snapshot = {
    val progressRows = spark.read
      .format("delta")
      .load(lakehouse.resolve("control/streaming_progress").toString)
      .select(
        "lineage",
        "batchId",
        "outcome",
        "completedAt",
        "candidateWatermark",
        "observedAt",
        "deliveredOffsets",
        "sourceEndOffsets"
      )
      .limit(rowBound + 1)
      .collect()
      .toVector
    requireProof(progressRows.size <= rowBound)
    val progress = progressRows.map { row =>
      requireProof(!row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2))
      val value = Progress(
        row.getString(0),
        row.getLong(1),
        row.getString(2),
        Option(row.getAs[java.sql.Timestamp](3)).map(_.toInstant),
        Option(row.getAs[java.sql.Timestamp](4)).map(_.toInstant),
        Option(row.getAs[java.sql.Timestamp](5)).map(_.toInstant),
        row
          .getSeq[org.apache.spark.sql.Row](6)
          .toVector
          .map(value =>
            DeliveredRange(
              value.getAs[String]("topic"),
              value.getAs[Int]("partition"),
              value.getAs[Long]("minimumDeliveredOffset"),
              value.getAs[Long]("maximumDeliveredOffset")
            )
          ),
        row
          .getSeq[org.apache.spark.sql.Row](7)
          .toVector
          .map(value => (value.getAs[String]("topic"), value.getAs[Int]("partition"), value.getAs[Long]("endOffset")))
      )
      requireProof(value.batchId >= 0L && (terminal ++ unfinished).contains(value.outcome))
      value
    }
    val decisionRows = spark.read
      .format("delta")
      .load(lakehouse.resolve("control/streaming_decisions").toString)
      .select("lineage", "batchId")
      .distinct()
      .limit(rowBound + 1)
      .collect()
      .toVector
    requireProof(decisionRows.size <= rowBound)
    Snapshot(
      progress,
      decisionRows.map(row => row.getString(0) -> row.getLong(1)).toSet,
      nativeIds(checkpoint.resolve("offsets")),
      nativeIds(checkpoint.resolve("commits")),
      permanentHashes(lakehouse.resolve("control/streaming_lineage"))
    )
  }

  final case class PendingCleanup(rows: Int, oldestEligibleAgeMillis: Option[Long])

  /** Match journal protection per lineage. Age starts when the terminal completion crosses the 60s fixture retention.
    */
  def pendingCleanup(snapshot: Snapshot, at: Instant): PendingCleanup = {
    val nativeIds = snapshot.nativeOffsets ++ snapshot.nativeCommits
    val eligible = snapshot.progress.groupBy(_.lineage).values.toVector.flatMap { rows =>
      val latestTerminal = rows.filter(row => terminal.contains(row.outcome)).maxByOption(_.batchId).map(_.batchId)
      val latestWatermark = rows
        .filter(row => row.outcome == "Published" && row.watermark.nonEmpty)
        .maxByOption(_.batchId)
        .map(_.batchId)
      rows.filter { row =>
        terminal.contains(row.outcome) && !nativeIds.contains(row.batchId) &&
        !latestTerminal.contains(row.batchId) && !latestWatermark.contains(row.batchId) &&
        row.completedAt.exists(time => !time.plusSeconds(60L).isAfter(at))
      }
    }
    val ages =
      eligible.flatMap(_.completedAt).map(time => java.time.Duration.between(time.plusSeconds(60L), at).toMillis)
    PendingCleanup(eligible.size, ages.maxOption)
  }

  /** Filesystem metadata only, with a hard inventory bound and no payload names or content emitted. */
  def inventory(lakehouse: Path, checkpoint: Path, snapshot: Snapshot, at: Instant): Document = {
    var files = 0L
    var bytes = 0L
    var oldestAge = 0L
    Vector(lakehouse, checkpoint).foreach { root =>
      val entries = Files.walk(root)
      try {
        val iterator = entries.iterator()
        var visited = 0
        while (iterator.hasNext) {
          requireProof(visited < 10000)
          val path = iterator.next()
          visited += 1
          requireProof(!Files.isSymbolicLink(path))
          if (Files.isRegularFile(path)) {
            files += 1L
            bytes += Files.size(path)
            oldestAge = math.max(
              oldestAge,
              math.max(0L, java.time.Duration.between(Files.getLastModifiedTime(path).toInstant, at).toMillis)
            )
          }
        }
      } finally entries.close()
    }
    val pending = snapshot.progress.filter(row => unfinished.contains(row.outcome))
    val oldestPending =
      pending.flatMap(_.observedAt).map(time => math.max(0L, java.time.Duration.between(time, at).toMillis)).maxOption
    val eligible = snapshot.progress.count(row => row.completedAt.exists(_.plusSeconds(60L).isBefore(at)))
    val pendingEligible = pendingCleanup(snapshot, at)
    new Document("files", files)
      .append("bytes", bytes)
      .append("oldestFileAgeMillis", oldestAge)
      .append("unfinishedRows", pending.size)
      .append("oldestUnfinishedAgeMillis", oldestPending.map(Long.box).orNull)
      .append("eligiblePendingCleanupRows", pendingEligible.rows)
      .append("oldestEligiblePendingCleanupAgeMillis", pendingEligible.oldestEligibleAgeMillis.map(Long.box).orNull)
      .append("agedTerminalRowsIncludingProtectedAnchors", eligible)
      .append("physicalRetentionClaim", false)
  }

  /** oldBaseline is captured when BOTH0/1 exist, before natural purge. No fixture row is invented or edited. */
  def verifyPruning(oldBaseline: Snapshot, before: Snapshot, after: Snapshot, observedAt: Instant): Result = {
    val lineages = oldBaseline.progress.map(_.lineage).toSet
    requireProof(lineages.size == 1)
    val lineage = lineages.head
    val old = oldBaseline.progress.filter(row => row.batchId == 0L || row.batchId == 1L)
    requireProof(old.map(_.batchId).toSet == Set(0L, 1L))
    requireProof(Set(lineage -> 0L, lineage -> 1L).subsetOf(oldBaseline.decisions))
    requireProof(
      old.forall(row =>
        terminal.contains(row.outcome) && row.completedAt.exists(_.plusSeconds(60L).isBefore(observedAt))
      )
    )
    requireProof(Set(0L, 1L).subsetOf(oldBaseline.nativeOffsets) && Set(0L, 1L).subsetOf(oldBaseline.nativeCommits))
    requireProof(!after.nativeOffsets.exists(Set(0L, 1L)) && !after.nativeCommits.exists(Set(0L, 1L)))
    requireProof(!after.progress.exists(row => row.lineage == lineage && Set(0L, 1L).contains(row.batchId)))
    requireProof(!after.decisions.exists { case (storedLineage, id) =>
      storedLineage == lineage && Set(0L, 1L).contains(id)
    })
    val latestTerminal =
      before.progress.filter(row => row.lineage == lineage && terminal.contains(row.outcome)).maxByOption(_.batchId)
    val latestWatermark = before.progress
      .filter(row => row.lineage == lineage && row.outcome == "Published" && row.watermark.nonEmpty)
      .maxByOption(_.batchId)
    requireProof(latestTerminal.nonEmpty && latestWatermark.nonEmpty)
    val protectedRows = (latestTerminal.toVector ++ latestWatermark.toVector).distinct
    requireProof(protectedRows.forall(row => after.progress.contains(row)))
    requireProof(
      protectedRows
        .filter(row => before.decisions.contains(row.lineage -> row.batchId))
        .forall(row => after.decisions.contains(row.lineage -> row.batchId))
    )
    requireProof(oldBaseline.permanentLineageHashes == after.permanentLineageHashes)
    val nativePinned = after.nativeOffsets ++ after.nativeCommits
    requireProof(before.progress.filter(row => nativePinned.contains(row.batchId)).forall(after.progress.contains))
    val currentUnfinished = before.progress.filter(row => unfinished.contains(row.outcome))
    val unfinishedStatus =
      if (currentUnfinished.isEmpty) "NOT_EXERCISED"
      else if (currentUnfinished.forall(after.progress.contains)) "OBSERVED_UNCHANGED_RETAINED"
      else {
        requireProof(
          currentUnfinished.forall(row =>
            after.progress.exists(next =>
              next.lineage == row.lineage && next.batchId == row.batchId && terminal.contains(
                next.outcome
              ) && next.completedAt.nonEmpty
            )
          )
        )
        "OBSERVED_TERMINAL_SUCCESSOR_RETAINED"
      }
    Result(oldCompletedRemoved = true, latestAnchorsRetained = true, unfinishedStatus = unfinishedStatus)
  }

  def verifyContinuation(before: Snapshot, after: Snapshot): Unit = {
    requireProof(
      before.nativeCommits.nonEmpty && after.nativeCommits.nonEmpty && after.nativeCommits.max > before.nativeCommits.max
    )
    requireProof(before.permanentLineageHashes == after.permanentLineageHashes)
    val previous = before.progress.filter(_.watermark.nonEmpty).maxByOption(_.batchId).flatMap(_.watermark)
    val current = after.progress.filter(_.watermark.nonEmpty).maxByOption(_.batchId).flatMap(_.watermark)
    requireProof(previous.nonEmpty && current.exists(value => !value.isBefore(previous.get)))
    requireProof(after.progress.exists(row => row.batchId == after.nativeCommits.max && row.outcome == "Published"))
    // Real producer coordinates, pinned publication/Mongo/Admin72, and checkpoint acknowledgment are mandatory external gates.
  }
}
