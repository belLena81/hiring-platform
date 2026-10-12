package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.SparkRuntime
import com.example.hiring.analytics.app.{AppModule, StreamingDurableBoundaryObserver}
import com.example.hiring.analytics.config.{AnalyticsConfigFixtures, AnalyticsStreamingRuntimeSettings}
import com.example.hiring.analytics.domain.{StreamingActivationAuthorization, StreamingActivationIdentity}
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices}
import com.mongodb.client.MongoClients
import mongo4cats.client.MongoClient
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.streaming.Trigger
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.{Base64, Date, Properties, UUID}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

/** Real Kafka topic, Mongo database, lakehouse and checkpoint for one run-to-completion streaming scenario. */
private[analytics] object StreamingIngestionFixtures {
  val StreamId = "hiring-ingestion"
  val GrantId = "hiring-ingestion-grant"

  final case class Fixture(
      namespace: KafkaTestNamespace.Namespace,
      root: Path,
      sparkTemp: Path,
      checkpoint: Path,
      databaseName: String
  )

  /** Job-created events by index, committed or aborted together within one Kafka transaction. */
  final case class Transaction(indexes: Range, commit: Boolean = true)

  def deleteTree(path: Path): IO[Unit] = IO
    .blocking(Files.exists(path))
    .flatMap(IO.whenA(_)(IO.blocking {
      val entries = Files.walk(path)
      try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally entries.close()
    }))

  /** Fails, never skips, when the isolated test services are not configured. */
  def fixture: Resource[IO, Fixture] =
    for {
      namespace <- KafkaTestNamespace.resource.evalMap(
        _.liftTo[IO](new IllegalStateException("HIRING_TEST_MANIFEST is required for streaming ingestion"))
      )
      manifest = namespace.manifest
      client <- MongoClient.fromConnectionString[IO](manifest.mongoUri)
      database <- LocalTestServices.database(client)
      root <- Resource.make(
        IO.blocking(
          Files.createTempDirectory(
            Path.of(manifest.workspace).resolve(".local/data/test-services"),
            "streaming-ingestion-"
          )
        )
      )(deleteTree)
      owned = Path.of(manifest.workspace).resolve(".local/data/analytics")
      sparkTemp <- Resource.make(IO.pure(owned.resolve("spark-temp").resolve("ingestion-" + UUID.randomUUID())))(
        deleteTree
      )
      checkpoint <- Resource.make(IO.pure(owned.resolve("checkpoints").resolve("ingestion-" + UUID.randomUUID())))(
        deleteTree
      )
      _ <- Resource.eval(IO.blocking {
        val sync = MongoClients.create(manifest.mongoUri)
        try {
          val db = sync.getDatabase(database.name)
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
      })
    } yield Fixture(namespace, root, sparkTemp, checkpoint, database.name)

  def event(index: Int, at: Instant): String = {
    val eventId = AnalyticsOperationalEventFixtures.id(s"ingestion-event-$index")
    val jobId = AnalyticsOperationalEventFixtures.id(s"ingestion-job-$index")
    val actorId = UUID.nameUUIDFromBytes(s"ingestion-recruiter-$index".getBytes("UTF-8")).toString
    s"""{"eventId":"$eventId","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"$jobId","actorId":"$actorId","payload":{"job":{"jobId":"$jobId","status":"Open","skills":["Scala"]}}}"""
  }

  /** Each transaction is begun, filled and then committed or aborted in order by one transactional publisher. */
  def publish(namespace: KafkaTestNamespace.Namespace, at: Instant, transactions: Vector[Transaction]): IO[Unit] =
    IO.blocking {
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.manifest.kafkaBootstrap)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "hiring-publisher-ingestion-" + UUID.randomUUID())
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      properties.put(
        "sasl.jaas.config",
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="hiring_publisher_v2" password="${namespace.manifest.publisherPassword}";"""
      )
      val producer = new KafkaProducer[String, String](properties)
      try {
        producer.initTransactions()
        transactions.foreach { transaction =>
          producer.beginTransaction()
          transaction.indexes.foreach { index =>
            producer
              .send(new ProducerRecord(namespace.events, UUID.randomUUID().toString, event(index, at)))
              .get(30L, TimeUnit.SECONDS)
          }
          if (transaction.commit) producer.commitTransaction() else producer.abortTransaction()
        }
      } finally producer.close(java.time.Duration.ofSeconds(5))
    }

  private def topicId(namespace: KafkaTestNamespace.Namespace): IO[String] = IO.blocking {
    val admin =
      Admin.create(LocalTestServices.adminProperties(namespace.manifest, "broker", namespace.manifest.brokerPassword))
    try
      admin
        .describeTopics(List(namespace.events).asJava)
        .allTopicNames()
        .get(20L, TimeUnit.SECONDS)
        .get(namespace.events)
        .topicId()
        .toString
    finally admin.close()
  }

  def settings(fixture: Fixture, maxOffsetsPerTrigger: Int): IO[AnalyticsStreamingRuntimeSettings] = {
    val manifest = fixture.namespace.manifest
    val key = Base64.getEncoder.encodeToString(Array.fill[Byte](32)(23))
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
         |  streaming {
         |    stream-id = "$StreamId"
         |    activation-grant-id = "$GrantId"
         |    checkpoint-location = "${fixture.checkpoint.toUri}"
         |    trigger-interval = 1 second
         |    max-offsets-per-trigger = $maxOffsetsPerTrigger
         |    maximum-replay-records = 1000
         |    initial-offsets = [{ partition = 0, offset = 0 }]
         |  }
         |}
         |""".stripMargin
    IO.fromEither(AnalyticsConfigFixtures.streamingRuntime(packaged + overrides))
  }

  /** Real activation grant document in the collection the production gate reads. */
  def grantActivation(fixture: Fixture, settings: AnalyticsStreamingRuntimeSettings): IO[StreamingActivationIdentity] =
    for {
      sourceTopicId <- topicId(fixture.namespace)
      identity <- IO.fromEither(
        settings.streaming.activationIdentity(
          fixture.namespace.manifest.kafkaClusterId,
          sourceTopicId,
          fixture.namespace.events,
          settings.common.lakehouseRoot
        )
      )
      _ <- insertGrant(fixture, identity)
    } yield identity

  private def insertGrant(fixture: Fixture, identity: StreamingActivationIdentity): IO[Unit] = IO.blocking {
    val now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    val grant = StreamingActivationAuthorization
      .fromEvidence(
        identity,
        GrantId,
        now.minusSeconds(1),
        now.plusSeconds(1800),
        Vector("ingestion-fixture"),
        Vector("ingestion-review-a", "ingestion-review-b")
      )
      .fold(problem => throw new IllegalStateException(problem), value => value)
    val sync = MongoClients.create(fixture.namespace.manifest.mongoUri)
    try
      sync
        .getDatabase(fixture.databaseName)
        .getCollection("analytics_streaming_activation")
        .insertOne(
          new Document("_id", grant.grantId)
            .append("grantId", grant.grantId)
            .append("streamId", identity.streamId)
            .append("sourceIdentity", identity.sourceIdentity)
            .append("lakehouseId", identity.lakehouseId)
            .append("contractFingerprint", identity.contractFingerprint)
            .append("settingsFingerprint", identity.settingsFingerprint)
            .append("validFrom", Date.from(grant.validFrom))
            .append("expiresAt", Date.from(grant.expiresAt))
            .append("evidenceReferences", grant.evidenceReferences.asJava)
            .append("independentReviewerReferences", grant.independentReviewerReferences.asJava)
            .append("evidenceDigest", grant.evidenceDigest)
        )
    finally sync.close()
  }.void

  /** One production stream that processes everything currently committed and then ends. */
  def runToCompletion(
      settings: AnalyticsStreamingRuntimeSettings,
      observer: Option[StreamingDurableBoundaryObserver[IO]] = None
  ): IO[Unit] =
    AppModule
      .streamingObserved[IO](settings, observer, None, Some(Trigger.AvailableNow()))
      .use(run => run)

  /** Delta row counts read through a separate short-lived session after the stream has released its own. */
  final case class Stored(bronze: Long, silver: Long, progress: Long)

  def stored(fixture: Fixture, settings: AnalyticsStreamingRuntimeSettings): IO[Stored] = {
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(settings.common.lakehouseRoot)
    SparkRuntime
      .sparkMongo[IO](
        fixture.namespace.manifest.mongoUri,
        "local[2]",
        "HiringAnalyticsIngestionRead",
        Some(false),
        fixture.root.resolve("read-temp").toString
      )
      .use { case (spark, _, blocking) =>
        def count(path: String) =
          blocking.blocking(
            if (io.delta.tables.DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path).count()
            else 0L
          )
        (count(paths.bronze), count(paths.silver), count(paths.streamingProgress)).mapN(Stored.apply)
      }
  }

  def reportSnapshot(fixture: Fixture): IO[Option[Document]] = IO.blocking {
    val sync = MongoClients.create(fixture.namespace.manifest.mongoUri)
    try
      Option(
        sync
          .getDatabase(fixture.databaseName)
          .getCollection("analytics_report_snapshots")
          .find(new Document("_id", "current"))
          .first()
      )
    finally sync.close()
  }

  def publishedRevision(fixture: Fixture): IO[Long] = IO.blocking {
    val sync = MongoClients.create(fixture.namespace.manifest.mongoUri)
    try
      sync
        .getDatabase(fixture.databaseName)
        .getCollection("analytics_report_control")
        .find(new Document("_id", "analytics-report"))
        .first()
        .getLong("lastPublishedRevision")
        .longValue()
    finally sync.close()
  }

  def scalaPostings(snapshot: Document): Long =
    snapshot
      .getList("skillPostingActivity", classOf[Document])
      .asScala
      .filter(_.getString("skill") == "scala")
      .map(_.getLong("postings").longValue())
      .sum

  /** Whether Spark committed the numbered micro-batch of this checkpoint. */
  def committedBatch(fixture: Fixture, batch: Long): Boolean =
    Files.exists(fixture.checkpoint.resolve("commits").resolve(batch.toString))
}
