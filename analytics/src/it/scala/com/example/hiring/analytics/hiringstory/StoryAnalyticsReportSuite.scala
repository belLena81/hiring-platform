package com.example.hiring.analytics.hiringstory

import com.example.hiring.analytics.AnalyticsTestSpark
import cats.effect.{IO, Resource}
import com.example.hiring.analytics.{IntegrationAnalyticsLakehousePaths, StreamingIngestionFixtures}
import com.example.hiring.analytics.adapter.spark.SparkRuntime
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.{AnalyticsBatchSettings, AnalyticsConfigFixtures}
import com.example.hiring.analytics.service.batch.AnalyticsPublication
import com.example.hiring.testing.LocalTestServices
import com.mongodb.client.MongoClients
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.bson.Document
import org.bson.json.{JsonMode, JsonWriterSettings}

import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant
import java.util.{Base64, Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Shared steps: the published events of one hiring dataset read by the real analytics batch.
  * (docs/specs/hiring-end-to-end-acceptance.md)
  *
  * The events are the ones the real application published during the story suite
  * (`HiringStoryAcceptanceIntegrationSpec` HEA-11.3 writes them to
  * `.local/data/hiring-story-analytics/story-events.jsonl`). They are republished unchanged, one transaction each, to a
  * fresh isolated topic and consumed by `AppModule.batch`, the production one-shot batch.
  *
  * The suite runs only in a started chain (marker file `chain-started` in the hand-off directory); otherwise its tests
  * are skipped, which is reported as unverified, never as a pass. In a started chain a missing events file fails.
  * Expected values below are written from the spec's story table, never from the aggregation code or its output.
  *
  * Evidence is provisional until the analytics refactoring closes.
  */
abstract class StoryAnalyticsReportSuite(eventsFile: String, reportFile: String, runId: String)
    extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 25.minutes

  private val HandOff = "hiring-story-analytics"

  private def workspaceFile(name: String): Path =
    Path
      .of(sys.env.getOrElse("HIRING_TEST_WORKSPACE", System.getProperty("user.dir")))
      .resolve(".local/data")
      .resolve(HandOff)
      .resolve(name)

  private def chainStarted: Boolean = Files.isRegularFile(workspaceFile("chain-started"))

  private def eventsPath: Path = workspaceFile(eventsFile)

  private def sha256(bytes: Array[Byte]): String =
    java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString

  /** The exported events and the SHA-256 of the exact bytes read now. A missing file FAILS in a started chain. */
  private def storyEvents: (Vector[(String, String)], String) = {
    val file = eventsPath
    if (!Files.isRegularFile(file)) fail(s"hand-off events file missing ($eventsFile): run the story export step first")
    val bytes = Files.readAllBytes(file)
    val events = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).linesIterator.toVector
      .filter(_.nonEmpty)
      .map { line =>
        val json = parse(line).fold(e => fail(e.message), identity)
        val cursor = json.hcursor
        (
          cursor.get[String]("key").fold(e => fail(e.message), identity),
          cursor.get[String]("value").fold(e => fail(e.message), identity)
        )
      }
    (events, sha256(bytes))
  }

  /** The events keep their order, key and exact value; each is its own transaction like the real publisher's. */
  private def republish(fixture: StreamingIngestionFixtures.Fixture, events: Vector[(String, String)]): IO[Unit] =
    IO.blocking {
      val manifest = fixture.namespace.manifest
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, manifest.kafkaBootstrap)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "hiring-publisher-story-" + UUID.randomUUID())
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      properties.put(
        "sasl.jaas.config",
        LocalTestServices.jaasConfig("hiring_publisher_v2", manifest.publisherPassword)
      )
      val producer = new KafkaProducer[String, String](properties)
      try {
        producer.initTransactions()
        events.foreach { case (key, value) =>
          producer.beginTransaction()
          producer.send(new ProducerRecord(fixture.namespace.events, key, value)).get(30L, TimeUnit.SECONDS)
          producer.commitTransaction()
        }
      } finally producer.close(java.time.Duration.ofSeconds(5))
    }

  private def batchSettings(
      fixture: StreamingIngestionFixtures.Fixture,
      runId: String,
      endExclusive: Long
  ): IO[AnalyticsBatchSettings] = {
    val manifest = fixture.namespace.manifest
    val key = Base64.getEncoder.encodeToString(Array.fill[Byte](32)(31))
    val packaged = new String(getClass.getClassLoader.getResourceAsStream("application.conf").readAllBytes(), "UTF-8")
    val overrides =
      s"""
         |analytics {
         |  mongo { uri = "${manifest.mongoUri}", database = "${fixture.databaseName}" }
         |  spark { master = "local[2]", local-directory = "${fixture.sparkTemp}" }
         |  kafka {
         |    bootstrap-servers = "${manifest.kafkaBootstrap}"
         |    username = "analytics_reader"
         |    password = "${manifest.readerPassword}"
         |    topic = "${fixture.namespace.events}"
         |    security-protocol = "SASL_PLAINTEXT"
         |    allow-plaintext = true
         |  }
         |  lakehouse { root = "${fixture.root.resolve("lakehouse").toUri}" }
         |  hmac { secret-base64 = "$key", key-id = "hmac-v1" }
         |  batch { run-id = "$runId", partition = 0, start-offset = 0, end-offset-exclusive = $endExclusive }
         |}
         |""".stripMargin
    IO.fromEither(AnalyticsConfigFixtures.batch(packaged + overrides))
  }

  private def run(settings: AnalyticsBatchSettings): IO[AnalyticsPublication] =
    AppModule.batch[IO](settings).use(program => program).timeout(15.minutes)

  /** The published report as plain numbers: funnel totals over all days, and what was withheld. */
  protected final case class ReportView(
      funnelDays: Int,
      created: Long,
      accepted: Long,
      declined: Long,
      interview: Long,
      hired: Long,
      rejected: Long,
      timeToHirePresent: Boolean,
      stageMetrics: Set[String],
      timeToHire: Option[Hire],
      stageEligible: Map[String, Long]
  )

  protected final case class Hire(
      p50: Double,
      p75: Double,
      p90: Double,
      p95: Double,
      eligibleCount: Long,
      excludedCount: Long
  )

  protected def view(snapshot: Document): ReportView = {
    val days = snapshot.getList("funnel", classOf[Document]).asScala.toVector
    def total(field: String): Long = days.map(_.get(field).asInstanceOf[Number].longValue()).sum
    ReportView(
      days.size,
      total("created"),
      total("accepted"),
      total("declined"),
      total("interview"),
      total("hired"),
      total("rejected"),
      snapshot.get("timeToHire") != null,
      snapshot.getList("stageDurations", classOf[Document]).asScala.map(_.getString("metric")).toSet,
      Option(snapshot.get("timeToHire", classOf[Document])).map { d =>
        def n(f: String): Double = d.get(f).asInstanceOf[Number].doubleValue()
        Hire(
          n("p50Hours"),
          n("p75Hours"),
          n("p90Hours"),
          n("p95Hours"),
          n("eligibleCount").toLong,
          n("excludedCount").toLong
        )
      },
      snapshot
        .getList("stageDurations", classOf[Document])
        .asScala
        .map(d => d.getString("metric") -> d.get("eligibleCount").asInstanceOf[Number].longValue())
        .toMap
    )
  }

  private def exportReport(fixture: StreamingIngestionFixtures.Fixture, eventsDigest: String): IO[Unit] = IO.blocking {
    val sync = MongoClients.create(fixture.namespace.manifest.mongoUri)
    try {
      val database = sync.getDatabase(fixture.databaseName)
      val settings = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build()
      def one(collection: String, id: String): String =
        database.getCollection(collection).find(new Document("_id", id)).first().toJson(settings)
      val file = workspaceFile(reportFile)
      Files.deleteIfExists(file)
      Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
      val _ = Files.writeString(
        file,
        Seq(
          "events-sha256\t" + eventsDigest,
          "analytics_report_control\t" + one("analytics_report_control", "analytics-report"),
          "analytics_report_snapshots\t" + one("analytics_report_snapshots", "current")
        ).mkString("", "\n", "\n")
      )
    } finally sync.close()
  }

  /** One application's earliest time per stage, as stored in `silver/application_stage_times`. */
  protected final case class StageRow(applicationId: String, stages: Map[String, Instant])

  protected val StageColumns = Vector("createdAt", "acceptedAt", "declinedAt", "interviewAt", "hiredAt", "rejectedAt")

  protected def stageRows(fixture: StreamingIngestionFixtures.Fixture, root: String): IO[Vector[StageRow]] = {
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(root)
    SparkRuntime
      .sparkMongo[IO](
        IO.unit,
        fixture.namespace.manifest.mongoUri,
        "local[2]",
        "HiringStoryStageTimesRead",
        Some(false),
        fixture.root.resolve("story-read-temp").toString
      )
      .use { case (spark, _, blocking) =>
        blocking.blocking {
          AnalyticsTestSpark.applyRuntimeSettings(spark)
          spark.read
            .format("delta")
            .load(paths.applicationStageTimes)
            .collect()
            .toVector
            .map { row =>
              StageRow(
                row.getAs[String]("applicationId"),
                StageColumns.flatMap(name => Option(row.getAs[java.sql.Timestamp](name)).map(name -> _.toInstant)).toMap
              )
            }
            .sortBy(_.applicationId)
        }
      }
  }

  protected final case class Observed(
      eventCount: Int,
      first: AnalyticsPublication,
      second: AnalyticsPublication,
      before: ReportView,
      after: ReportView,
      bronzeAfterSecond: Long,
      silverAfterSecond: Long,
      stagesBefore: Vector[StageRow],
      stagesAfter: Vector[StageRow],
      events: Vector[(String, String)]
  )

  private val observed = ResourceSuiteLocalFixture(
    "hiring-story-analytics",
    if (!chainStarted) Resource.pure[IO, Option[Observed]](None)
    else
      Resource.eval(IO { Files.deleteIfExists(workspaceFile(reportFile)); storyEvents }).flatMap {
        case (events, eventsDigest) =>
          StreamingIngestionFixtures.fixture.evalMap { fixture =>
            val endExclusive = events.size * 2L // one record and one transaction marker per event
            val root = fixture.root.resolve("lakehouse").toUri.toString
            for {
              _ <- republish(fixture, events)
              first <- batchSettings(fixture, runId, endExclusive).flatMap(run)
              firstSnapshot <- StreamingIngestionFixtures.reportSnapshot(fixture)
              stagesBefore <- stageRows(fixture, root)
              second <- batchSettings(fixture, runId, endExclusive).flatMap(run)
              secondSnapshot <- StreamingIngestionFixtures.reportSnapshot(fixture)
              stagesAfter <- stageRows(fixture, root)
              streamSettings <- StreamingIngestionFixtures.settings(fixture, 13)
              counts <- StreamingIngestionFixtures.stored(fixture, streamSettings)
              _ <- exportReport(fixture, eventsDigest)
            } yield Option(
              Observed(
                events.size,
                first,
                second,
                view(firstSnapshot.getOrElse(sys.error("no report snapshot was published"))),
                view(secondSnapshot.getOrElse(sys.error("no report snapshot after the second read"))),
                counts.bronze,
                counts.silver,
                stagesBefore,
                stagesAfter,
                events
              )
            )
          }
      }
  )

  override def munitFixtures: List[munit.AnyFixture[?]] = List(observed)

  /** Skipped (unverified, never a pass) unless the chain was started; in a started chain a missing result fails. */
  protected def observation(): Observed = {
    assume(chainStarted, "SKIPPED: analytics chain not started (create the chain-started marker, see the spec, D7)")
    observed().getOrElse(fail("the analytics chain was started but produced no observation"))
  }

  /** The story's own events give the earliest time each application entered each stage, independent of Spark. */
  protected def stagesFromEvents(events: Vector[(String, String)]): Map[String, Map[String, Instant]] = {
    val entered = events.flatMap { case (_, value) =>
      val cursor = parse(value).fold(e => fail(e.message), identity).hcursor
      val eventType = cursor.get[String]("eventType").toOption
      val at = cursor.get[String]("occurredAt").toOption.map(Instant.parse)
      val application = cursor.downField("payload").get[String]("applicationId").toOption
      val status = eventType match {
        case Some("APPLICATION_CREATED")        => Some("createdAt")
        case Some("APPLICATION_STATUS_CHANGED") =>
          cursor.downField("payload").get[String]("newStatus").toOption.map(name => name.toLowerCase + "At")
        case _ => None
      }
      (application, status, at) match {
        case (Some(a), Some(s), Some(t)) => Vector((a, s, t))
        case _                           => Vector.empty
      }
    }
    entered.groupBy(_._1).view.mapValues(_.groupBy(_._2).view.mapValues(_.map(_._3).min).toMap).toMap
  }
}
