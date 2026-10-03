package com.example.hiring.analytics.cli

import cats.effect.{IO, IOApp, ExitCode, Outcome, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.adapter.spark.SparkPhysicalLocation
import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import org.apache.kafka.clients.admin.{Admin, OffsetSpec, ListOffsetsOptions}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import pureconfig.ConfigSource
import HiringAnalyticsStreamingWorkloadMain.ProducerCredentials
import HiringAnalyticsStreamingWorkloadMain.given
import java.util.{Properties, UUID}
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.mongodb.{ConnectionString, MongoClientSettings, ReadConcern}
import com.mongodb.client.MongoClients
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.bson.Document

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, LinkOption, Path, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Actual AppModule streaming composition, observed through public Spark APIs in its original JVM. */
object StreamingActiveTerminationProofMain extends IOApp {
  private final case class Original(session: SparkSession, query: StreamingQuery, scratch: Path, bronzeRows: Long)

  private def safe(path: Path): Unit = {
    require(path.isAbsolute && path.normalize() == path)
    Iterator
      .iterate(Option(path))(_.flatMap(p => Option(p.getParent)))
      .takeWhile(_.nonEmpty)
      .flatten
      .foreach(p => require(!Files.isSymbolicLink(p)))
  }

  private def write(path: Path, value: Document): IO[Unit] = IO.blocking {
    Files.createFile(path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    Files.writeString(path, value.toJson, UTF_8, StandardOpenOption.WRITE)
    ()
  }

  /** Inspect only committed native AddFile statistics, not subject records or raw payloads. */
  private def committedBronzeRows(bronze: Path): Long = {
    val directory = bronze.resolve("_delta_log")
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) 0L
    else {
      safe(directory)
      val entries = Files.newDirectoryStream(directory, "*.json")
      try {
        val iterator = entries.iterator()
        var visited = 0
        var maximum = 0L
        while (iterator.hasNext && visited < 128) {
          val path = iterator.next(); visited += 1
          require(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(path))
          require(Files.size(path) <= 1048576L)
          var rows = 0L
          Files.readAllLines(path, UTF_8).asScala.foreach { line =>
            val action = Document.parse(line)
            Option(action.get("add", classOf[Document])).flatMap(add => Option(add.getString("stats"))).foreach {
              stats =>
                val parsed = Document.parse(stats)
                Option(parsed.get("numRecords", classOf[java.lang.Number])).foreach(n => rows += n.longValue())
            }
          }
          maximum = math.max(maximum, rows)
        }
        require(!iterator.hasNext, "active shutdown Delta metadata inventory exceeds its bound")
        maximum
      } finally entries.close()
    }
  }

  private def unacknowledgedInitialOffsets(checkpoint: Path, topic: String, end: Long): Boolean = {
    val offset = checkpoint.resolve("offsets/0")
    val commit = checkpoint.resolve("commits/0")
    safe(offset)
    safe(commit)
    if (!Files.isRegularFile(offset, LinkOption.NOFOLLOW_LINKS) || Files.exists(commit, LinkOption.NOFOLLOW_LINKS))
      false
    else {
      require(Files.size(offset) <= 65536L)
      val lines = Files.readAllLines(offset, UTF_8).asScala.toVector
      require(lines.size == 3 && lines.head == "v1")
      val topics = Document.parse(lines(2))
      val partitions = topics.get(topic, classOf[Document])
      topics.keySet().asScala.toSet == Set(topic) && partitions != null &&
      partitions.keySet().asScala.toSet == Set("0", "1", "2") &&
      partitions.get("0", classOf[java.lang.Number]).longValue() == end &&
      partitions.get("1", classOf[java.lang.Number]).longValue() == 0L &&
      partitions.get("2", classOf[java.lang.Number]).longValue() == 0L
    }
  }

  private[cli] def validateFixtureNonce(
      database: String,
      workloadNonce: String,
      stateNonce: String
  ): Either[String, Unit] =
    for {
      _ <- Either.cond(
        Option(workloadNonce).exists(_.matches("[a-f0-9]{16}")),
        (),
        "active shutdown requires a valid fixture nonce"
      )
      _ <- Either.cond(
        database == "hiring_streaming_proof_" + workloadNonce,
        (),
        "active shutdown database must match its fixture nonce"
      )
      _ <- Either.cond(
        stateNonce == workloadNonce,
        (),
        "active shutdown state must match its fixture nonce"
      )
    } yield ()

  override def run(args: List[String]): IO[ExitCode] = {
    val program = for {
      settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
      config <- IO.blocking(ConfigFactory.load())
      nonce <- IO(config.getString("analytics.workload-api.nonce"))
      signal <- IO(config.getString("analytics.active-shutdown.signal"))
      _ <- IO(require(nonce.matches("[a-f0-9]{16}") && Set("TERM", "INT").contains(signal)))
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
      paths <- IO.fromEither(
        AnalyticsLakehousePaths
          .from(settings.common.lakehouseRoot)
          .toEither
          .leftMap(_ => new IllegalArgumentException("active shutdown lakehouse paths invalid"))
      )
      configurationPath <- IO(Path.of(System.getProperty("config.file")).toAbsolutePath.normalize())
      repository = configurationPath.getParent.getParent.getParent.getParent
      _ <- IO.blocking {
        safe(configurationPath)
        require(Files.isRegularFile(configurationPath, LinkOption.NOFOLLOW_LINKS))
        require(configurationPath.getParent.getFileName.toString == "hiring-streaming-proof-" + nonce)
        val statePath = configurationPath.getParent.resolve("state.json")
        safe(statePath)
        require(Files.size(statePath) <= 65536L)
        val state = Document.parse(Files.readString(statePath, UTF_8))
        validateFixtureNonce(settings.common.mongoDatabase, nonce, state.getString("nonce"))
          .fold(message => throw new IllegalArgumentException(message), identity)
        val connection = new ConnectionString(settings.common.mongoUri)
        require(connection.getHosts.asScala.toVector == Vector("127.0.0.1:" + state.getInteger("mongoPort")))
        require(
          connection.getRequiredReplicaSetName == "rs0" && connection.isDirectConnection == java.lang.Boolean.TRUE
        )
        require(connection.getCredential.getUserName == "analytics_runtime")
        require(connection.getCredential.getSource == settings.common.mongoDatabase)
      }
      root = Path.of(SparkPhysicalLocation.resolve(settings.common.lakehouseRoot)).toAbsolutePath.normalize()
      bronze = Path.of(SparkPhysicalLocation.resolve(paths.bronze)).toAbsolutePath.normalize()
      output = repository
        .resolve(".local/logs")
        .resolve("hiring-streaming-proof-" + nonce)
        .resolve("active-shutdown-" + signal)
        .normalize()
      checkpoint = Path
        .of(SparkPhysicalLocation.resolve(settings.streaming.checkpointLocation))
        .toAbsolutePath
        .normalize()
      _ <- IO.blocking {
        Vector(root, output).foreach(safe)
        require(!Files.exists(bronze, LinkOption.NOFOLLOW_LINKS), "active shutdown requires a fresh lakehouse")
        Files.createDirectory(
          output,
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
        )
      }
      credentials <- IO.fromEither(
        ConfigSource.default
          .at("analytics.workload-producer")
          .load[ProducerCredentials]
          .leftMap(_ => new IllegalArgumentException("active shutdown producer missing"))
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
        producerSettings.put("max.block.ms", "10000")
        producerSettings.put("request.timeout.ms", "10000")
        producerSettings.put("delivery.timeout.ms", "20000")
        producerSettings.put("transactional.id", "hiring-publisher-active-shutdown-" + nonce)
      }
      topic = AnalyticsTopic.unwrap(settings.topic)
      partitions = Vector.tabulate(3)(index => new TopicPartition(topic, index))
      ends <- Resource
        .make(IO.blocking(Admin.create(producerSettings)))(admin =>
          IO.blocking(admin.close(java.time.Duration.ofSeconds(5)))
        )
        .use { admin =>
          def latest = IO.blocking(
            admin
              .listOffsets(
                partitions.map(_ -> OffsetSpec.latest()).toMap.asJava,
                new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)
              )
              .all()
              .get(10, TimeUnit.SECONDS)
              .asScala
              .toMap
              .view
              .mapValues(_.offset())
              .toMap
          )
          for {
            initial <- latest
            _ <- IO(
              require(initial.size == 3 && initial.values.forall(_ == 0L), "active shutdown broker must be fresh")
            )
            _ <- Resource
              .make(IO.blocking(new KafkaProducer[String, String](producerSettings)))(producer =>
                IO.blocking(producer.close(java.time.Duration.ofSeconds(5)))
              )
              .use { producer =>
                IO.blocking {
                  producer.initTransactions()
                  producer.beginTransaction()
                  (0 until 12).foreach { index =>
                    val event = UUID.randomUUID().toString
                    val actor = UUID.randomUUID().toString
                    val body = new Document("eventId", event)
                      .append("eventType", "JOB_CREATED")
                      .append("occurredAt", java.time.Instant.now().toString)
                      .append("aggregateType", "Job")
                      .append("aggregateId", event)
                      .append("actorId", actor)
                      .append("payload", new Document("job", new Document("skills", Vector("active-shutdown").asJava)))
                      .toJson
                    val metadata = producer
                      .send(new ProducerRecord[String, String](topic, Int.box(0), event, body))
                      .get(10, TimeUnit.SECONDS)
                    require(metadata.partition() == 0 && metadata.offset() == index.toLong)
                  }
                  producer.commitTransaction()
                }
              }
            current <- latest
            _ <- IO(
              require(current(partitions(0)) > 12L && current(partitions(1)) == 0L && current(partitions(2)) == 0L)
            )
          } yield current
        }
      _ <- write(
        output.resolve("seed.json"),
        new Document("nonce", nonce)
          .append("actualRecords", 12)
          .append("partition0ReadCommittedEnd", ends(partitions(0)))
          .append("transactionControlGapObserved", true)
      )
      original <- Ref.of[IO, Option[Original]](None)
      monitor = {
        def await: IO[Unit] = IO
          .blocking {
            SparkSession.getDefaultSession.flatMap { session =>
              val queries = session.streams.active
              val rows = committedBronzeRows(bronze)
              queries
                .find(query => query.name == settings.streaming.streamId && query.isActive)
                .filter(_ =>
                  session.sparkContext.appName == "hiring-analytics-streaming" && queries.length == 1 &&
                    rows >= 12L && session.sparkContext.statusTracker.getActiveJobIds().nonEmpty &&
                    session.sparkContext.statusTracker
                      .getActiveStageIds()
                      .exists(stage =>
                        session.sparkContext.statusTracker.getStageInfo(stage).exists(_.numActiveTasks > 0)
                      ) &&
                    unacknowledgedInitialOffsets(checkpoint, topic, ends(partitions(0)))
                )
                .map { query =>
                  val scratch = Path.of(session.sparkContext.getConf.get("spark.local.dir")).toAbsolutePath.normalize()
                  safe(scratch)
                  val base = Path.of(settings.common.sparkLocalDirectory).toAbsolutePath.normalize()
                  require(scratch.getParent == base && scratch.getFileName.toString.matches("streaming-[a-f0-9-]{36}"))
                  Original(session, query, scratch, rows)
                }
            }
          }
          .flatMap {
            case None        => IO.sleep(20.millis) *> await
            case Some(value) =>
              original.set(Some(value)) *> write(
                output.resolve("ready.json"),
                new Document("nonce", nonce)
                  .append("signal", signal)
                  .append("originalPid", ProcessHandle.current().pid())
                  .append("nonEmptyBronzeRows", value.bronzeRows)
                  .append("activeJobObserved", true)
              )
          }
        await.timeout(90.seconds)
      }
      _ <- monitor.background.use { outcome =>
        val failure = outcome.flatMap {
          case Outcome.Errored(error) => IO.raiseError[Unit](error)
          case Outcome.Canceled()     =>
            IO.raiseError[Unit](new IllegalStateException("active shutdown observer cancelled"))
          case Outcome.Succeeded(done) => done *> IO.never[Unit]
        }
        IO.race(StreamingProcessTermination.run(AppModule.streaming[IO](settings).use(_.run)), failure).void
      }
      captured <- original.get.flatMap(_.liftTo[IO](new IllegalStateException("original resources not captured")))
      _ <- IO(require(captured.session.sparkContext.isStopped && !captured.query.isActive))
      absent <- IO.blocking(!Files.exists(captured.scratch, LinkOption.NOFOLLOW_LINKS))
      _ <- IO(require(absent, "original owned scratch remains"))
      count <- IO.blocking {
        val connection = new ConnectionString(settings.common.mongoUri)
        val native = MongoClientSettings
          .builder()
          .applyConnectionString(connection)
          .applyToClusterSettings(b => b.serverSelectionTimeout(5, TimeUnit.SECONDS))
          .applyToSocketSettings(b => b.connectTimeout(5, TimeUnit.SECONDS).readTimeout(5, TimeUnit.SECONDS))
          .build()
        val client = MongoClients.create(native)
        try
          client
            .getDatabase(settings.common.mongoDatabase)
            .withReadConcern(ReadConcern.MAJORITY)
            .getCollection("analytics_lakehouse_mutexes")
            .find()
            .projection(new Document("_id", 1))
            .limit(3)
            .maxTime(5, TimeUnit.SECONDS)
            .into(new java.util.ArrayList[Document]())
            .size()
        finally client.close()
      }
      _ <- IO(require(count == 0, "original mutex owners remain"))
      _ <- write(
        output.resolve("result.json"),
        new Document("nonce", nonce)
          .append("signal", signal)
          .append("originalContextStopped", true)
          .append("originalQueryInactive", true)
          .append("ownedScratchAbsent", true)
          .append("mutexCount", count)
          .append("nonEmptyBronzeRows", captured.bronzeRows)
          .append("actualAppModuleUseReturned", true)
      )
      _ <- IO.println(
        "STREAMING_ACTIVE_TERMINATION_COMPONENT_PASSED originalContextStopped=true originalQueryInactive=true ownedScratchAbsent=true mutexCount=0"
      )
    } yield ExitCode.Success
    program.handleErrorWith(_ => IO.println("STREAMING_ACTIVE_TERMINATION_COMPONENT_FAILED").as(ExitCode.Error))
  }
}
