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

import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.client.model.Filters
import org.bson.Document
import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, OffsetSpec}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.common.serialization.StringSerializer

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.{Date, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.Using

/** Live API-to-Compose-worker proof on a disposable task-scoped Mongo/Kafka/Delta stack. */
final class AccountDeletionComposeIntegrationSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 20.minutes
  private val enabled = sys.env.get("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ENABLED").contains("true")
  private val completionEnabled = sys.env.get("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_COMPLETE").contains("true")

  private def required(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(fail(s"$name is required for Compose deletion proof"))

  private def isLoopback(host: String): Boolean =
    host.equalsIgnoreCase("localhost") || host == "127.0.0.1" || host == "::1" || host == "[::1]"

  private def hostName(endpoint: String): String =
    if (endpoint.startsWith("[")) endpoint.takeWhile(_ != ']') + "]"
    else endpoint.takeWhile(_ != ':')

  private def validateLocalEndpoints(api: String, mongo: String, kafka: String): Unit = {
    val apiUri = URI.create(api)
    assert(
      apiUri.getScheme == "http" && Option(apiUri.getHost).exists(isLoopback),
      "Compose proof API must use loopback HTTP"
    )
    assert(
      Option(apiUri.getRawPath).forall(path => path.isEmpty || path == "/"),
      "Compose proof API URL must be a loopback origin"
    )

    val mongoUri = new com.mongodb.ConnectionString(mongo)
    assert(!mongoUri.isSrvProtocol, "Compose proof Mongo URI must use mongodb://")
    assert(
      mongoUri.getHosts != null && !mongoUri.getHosts.isEmpty && mongoUri.getHosts.asScala.forall { host =>
        isLoopback(hostName(host))
      },
      "Compose proof Mongo hosts must all be loopback"
    )

    val kafkaHosts = kafka.split(",", -1).toVector
    assert(
      kafkaHosts.nonEmpty && kafkaHosts.forall(host => isLoopback(hostName(host))),
      "Compose proof Kafka endpoints must all be loopback"
    )
  }

  private def graphql(apiBase: String, query: String, token: Option[String] = None): String = {
    val payload = new Document("query", query).toJson
    val builder = HttpRequest
      .newBuilder(URI.create(apiBase.stripSuffix("/") + "/graphql"))
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .timeout(Duration.ofSeconds(15))
    token.foreach(value => builder.header("Authorization", "Bearer " + value))
    val request = builder.POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8)).build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    assertEquals(response.statusCode(), 200)
    val errors = Option(Document.parse(response.body()).getList("errors", classOf[Document]))
      .fold(Vector.empty[String])(_.asScala.toVector.map(_.getString("message")))
    assert(errors.isEmpty, s"GraphQL returned errors: ${errors.mkString("; ")}")
    response.body()
  }

  private val authFields =
    """__typename ... on AuthSuccess { accessToken user { id } } ... on DomainError { code message } ... on ValidationError { code message }"""

  private def authResult(body: String, field: String): (String, String) = {
    val pattern =
      ("(?s)\\\"" + field + "\\\"\\s*:\\s*\\{.*?\\\"__typename\\\"\\s*:\\s*\\\"AuthSuccess\\\".*?\\\"accessToken\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"user\\\"\\s*:\\s*\\{.*?\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").r
    pattern.findFirstMatchIn(body).fold(fail(s"$field did not return AuthSuccess"))(m => (m.group(1), m.group(2)))
  }

  private def eventually[A](read: => A, timeout: FiniteDuration = 3.minutes)(ready: A => Boolean): A = {
    val deadline = timeout.fromNow
    var value = read
    while (!ready(value) && deadline.hasTimeLeft()) {
      Thread.sleep(250)
      value = read
    }
    assert(ready(value), "timed out waiting for Compose deletion lifecycle state")
    value
  }

  private def seedOutboxEvent(database: MongoDatabase, subjectId: String, state: String): String = {
    assert(Set("Published", "Retryable").contains(state))
    val now = new Date()
    val eventId = UUID.randomUUID().toString
    val eventJson =
      s"""{"eventId":"$eventId","eventType":"SEARCH_PERFORMED","occurredAt":"${now.toInstant}","aggregateType":"Search","aggregateId":"deletion-proof-$eventId","actorId":"$subjectId","payload":{"searchKind":"jobs","query":"proof","results":[]}}"""
    val event = new Document("_id", eventId)
      .append("topic", "hiring.operational-events")
      .append("eventType", "SEARCH_PERFORMED")
      .append("occurredAt", now)
      .append("aggregateType", "Search")
      .append("aggregateId", s"deletion-proof-$eventId")
      .append("actorId", subjectId)
      .append("subjectIds", java.util.List.of(subjectId))
      .append("subjectRefsVersion", 1)
      .append("payload", "{\"searchKind\":\"jobs\",\"query\":\"proof\",\"results\":[]}")
      .append("envelopeBytes", eventJson.getBytes(StandardCharsets.UTF_8))
      .append("partitionKey", s"deletion-proof-$eventId")
      .append("state", state)
      .append("attempts", if (state == "Published") 1 else 0)
      .append("availableAt", now)
      .append("createdAt", now)
      .append("updatedAt", now)
    if (state == "Published") event.append("publishedAt", now)
    database
      .getCollection("event_outbox")
      .insertOne(event)
    eventId
  }

  private def heldPublisherTransaction(kafka: String, transactionalId: String): KafkaProducer[String, String] = {
    val connection = KafkaConnection(
      kafka,
      saslUsername = Some("hiring_publisher_v2"),
      saslPassword = Some(required("KAFKA_PUBLISHER_V2_PASSWORD")),
      securityProtocol = KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = true
    )
    val properties = new Properties()
    KafkaClientProperties
      .clientProperties(connection)
      .fold(
        error => fail(s"invalid Compose proof Kafka connection: $error"),
        _.foreach { case (key, value) => properties.setProperty(key, value) }
      )
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000")
    properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000")
    val producer = new KafkaProducer[String, String](properties)
    try {
      producer.initTransactions()
      producer.beginTransaction()
      producer
    } catch {
      case error: Throwable =>
        producer.close()
        throw error
    }
  }

  private def isFenced(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      case _: ProducerFencedException       => true
      case _: InvalidProducerEpochException => true
      case _                                => false
    }

  private def passShortKafkaRetention(kafka: String, topic: String, barrier: Document, nonce: String): Unit = {
    val settings = new Properties()
    settings.put("bootstrap.servers", kafka)
    KafkaClientProperties
      .clientProperties(
        KafkaConnection(
          kafka,
          Some("broker"),
          Some(required("KAFKA_BROKER_PASSWORD")),
          KafkaSecurityProtocol.SaslPlaintext,
          true
        )
      )
      .fold(error => fail(error.getMessage), _.foreach { case (key, value) => settings.put(key, value) })
    val admin = Admin.create(settings)
    try {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val changes = Vector(
        "retention.ms" -> "60000",
        "retention.bytes" -> "-1",
        "cleanup.policy" -> "delete",
        "segment.bytes" -> "16384",
        "segment.ms" -> "1000"
      )
        .map { case (key, value) => new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET) }
      admin
        .incrementalAlterConfigs(Map(resource -> changes.asJava).asJava)
        .all()
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
      val partitions = barrier.getList("partitions", classOf[Document]).asScala.toVector
      Using.resource(heldPublisherTransaction(kafka, "hiring-publisher-deletion-tail-" + nonce)) { producer =>
        partitions.foreach { partition =>
          (0 until 4).foreach { index =>
            val value = new Document("eventId", UUID.randomUUID().toString)
              .append("eventType", "JOB_CREATED")
              .append("occurredAt", Instant.now().toString)
              .append("aggregateType", "Job")
              .append("aggregateId", UUID.randomUUID().toString)
              .append("actorId", UUID.randomUUID().toString)
              .append("payload", new Document("padding", "t" * 12000))
              .toJson
            val metadata = producer
              .send(
                new ProducerRecord[String, String](
                  topic,
                  partition.getInteger("number"),
                  "unrelated-retention-tail",
                  value
                )
              )
              .get(30, java.util.concurrent.TimeUnit.SECONDS)
            assert(metadata.offset() >= partition.getLong("endOffsetExclusive"))
            producer.commitTransaction()
            if (index < 3 || partition != partitions.last) producer.beginTransaction()
          }
        }
      }
      eventually(
        {
          val offsets = admin
            .listOffsets(
              partitions
                .map(partition => new TopicPartition(topic, partition.getInteger("number")) -> OffsetSpec.earliest())
                .toMap
                .asJava
            )
            .all()
            .get(30, java.util.concurrent.TimeUnit.SECONDS)
          partitions.forall(partition =>
            offsets.get(new TopicPartition(topic, partition.getInteger("number"))).offset() >=
              partition.getLong("endOffsetExclusive")
          )
        },
        3.minutes
      )(identity)
      println("ACCOUNT_DELETION_ACTUAL_KAFKA_RETENTION_PASSED allCapturedPartitions=true retentionMillis=60000")
    } finally admin.close()
  }

  private def coordinateCalendarRestart(subjectId: String): Unit = {
    val path = Path.of(required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_STATE_FILE")).toAbsolutePath.normalize()
    assert(
      !Files.isSymbolicLink(path.getParent) && Files.isDirectory(path.getParent),
      "coordination parent must be real"
    )
    assert(path.getParent.getFileName.toString == "config" && path.getParent.getParent.getFileName.toString == ".local")
    assert(!Files.exists(path), "restart coordination must not overwrite prior state")
    val temporary = Files.createTempFile(
      path.getParent,
      path.getFileName.toString,
      ".tmp",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
    )
    try {
      Files.writeString(temporary, subjectId + "\n", StandardCharsets.UTF_8)
      Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
    } finally Files.deleteIfExists(temporary)
  }

  private def seedAttributedDeltaRows(subjectId: String): (String, String) = {
    val nonce = UUID.randomUUID().toString
    val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromBase64(
      required("HIRING_ANALYTICS_HMAC_SECRET_BASE64"),
      sys.env.getOrElse("HIRING_ANALYTICS_HMAC_KEY_ID", "hmac-v1"),
      sys.env.get("HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID"),
      sys.env.get("HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64")
    )
    val subjectToken = AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, subjectId)
    val controlToken = AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, s"unrelated-deletion-proof-$nonce")
    val root = new File(required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ANALYTICS_DIR"))
    assert(
      root.isAbsolute && root.getCanonicalPath == root.getAbsolutePath,
      "proof Delta directory must be an absolute, non-symlink task path"
    )
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(root.toURI.toString.stripSuffix("/") + "/lakehouse")
    val schema = StructType(
      Seq(
        StructField("subjectTokens", ArrayType(StringType, containsNull = false), nullable = false),
        StructField("rawValue", StringType, nullable = false),
        StructField("expiresAt", TimestampType, nullable = false)
      )
    )
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("AccountDeletionComposeIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try {
      val expiresAt = Timestamp.from(Instant.now().plusSeconds(30L * 86400L))
      val rows = java.util.Arrays.asList(
        org.apache.spark.sql.RowFactory.create(java.util.List.of(subjectToken), s"subject-proof-$nonce", expiresAt),
        org.apache.spark.sql.RowFactory.create(java.util.List.of(controlToken), s"control-proof-$nonce", expiresAt)
      )
      spark.createDataFrame(rows, schema).write.format("delta").mode("errorifexists").save(paths.bronze)
      val bronzePath = Path.of(URI.create(paths.bronze))
      val _ = Files.setPosixFilePermissions(bronzePath.getParent, PosixFilePermissions.fromString("rwxrwxrwx"))
      Using.resource(Files.walk(bronzePath)) { entries =>
        entries.forEach { path =>
          val mode = if (Files.isDirectory(path)) "rwxrwxrwx" else "rw-rw-rw-"
          val _ = Files.setPosixFilePermissions(path, PosixFilePermissions.fromString(mode))
        }
      }
      val seeded = spark.read.format("delta").load(paths.bronze)
      assertEquals(seeded.filter(array_contains(col("subjectTokens"), subjectToken)).count(), 1L)
      assertEquals(seeded.filter(array_contains(col("subjectTokens"), controlToken)).count(), 1L)
      (subjectToken, controlToken)
    } finally spark.stop()
  }

  private def deletionStatus(apiBase: String, receiptId: String, token: String): String = {
    val body = new Document("query", "query($receiptId: ID!) { accountDeletionStatus(receiptId: $receiptId) }")
      .append("variables", new Document("receiptId", receiptId))
      .toJson
    val request = HttpRequest
      .newBuilder(URI.create(apiBase.stripSuffix("/") + "/graphql"))
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Authorization", "Bearer " + token)
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    assertEquals(response.statusCode(), 200)
    "\\\"accountDeletionStatus\\\"\\s*:\\s*\\\"(PENDING|COMPLETE|NOT_FOUND)\\\"".r
      .findFirstMatchIn(response.body())
      .map(_.group(1))
      .getOrElse(fail("missing deletion status in authenticated GraphQL response"))
  }

  test(
    new munit.TestOptions(
      "account deletion fences a registered publisher transaction and purges only the subject's data"
    ).withTags(if (enabled) Set.empty else Set(munit.Ignore))
  ) {
    if (enabled) {
      val databaseName = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_DATABASE")
      assert(databaseName.matches("account_deletion_[0-9a-f]{16}"))
      val topic = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_TOPIC")
      assert(topic.matches("hiring[.]deletion[.][0-9a-f]{16}"))
      val uri = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_MONGO_URI")
      val api = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_API_URL")
      val kafka = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_KAFKA")
      val analyticsDir = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ANALYTICS_DIR")
      assertEquals(new File(analyticsDir).getCanonicalPath, new File(analyticsDir).getAbsolutePath)
      val nonce = databaseName.stripPrefix("account_deletion_")
      assertEquals(topic, s"hiring.deletion.$nonce", "database and topic must use the same proof nonce")
      validateLocalEndpoints(api, uri, kafka)
      val client: MongoClient = MongoClients.create(uri)
      try {
        val database = client.getDatabase(databaseName)
        List("users", "event_outbox", "analytics_erasure_requests", "outbox_subject_fences").foreach { name =>
          assertEquals(
            database.getCollection(name).countDocuments(),
            0L,
            s"task collection $name must have no preexisting rows"
          )
        }
        eventually(
          Option(
            database.getCollection("analytics_worker_heartbeats").find(Filters.eq("_id", "analytics-erasure")).first()
          )
        )(_.exists(_.getString("state") == "Ready"))
        val admin = graphql(
          api,
          s"""mutation { bootstrapAdmin(input: { idempotencyKey: "${UUID
              .randomUUID()}", name: "Compose Proof Candidate", password: "password-password" }) { $authFields } }"""
        )
        val adminId = authResult(admin, "bootstrapAdmin")._2
        val changed = database
          .getCollection("users")
          .updateOne(
            Filters.eq("_id", adminId),
            new Document(
              "$set",
              new Document("role", "Candidate").append(
                "profile",
                new Document("kind", "Candidate")
                  .append("skills", java.util.List.of("Scala"))
                  .append("skillsCanonical", java.util.List.of("scala"))
                  .append("recruiterSearchOptIn", false)
              )
            )
              .append("$unset", new Document("adminSingletonKey", ""))
          )
        assertEquals(changed.getMatchedCount, 1L)
        val login = graphql(
          api,
          s"""mutation { login(input: { idempotencyKey: "${UUID
              .randomUUID()}", name: "Compose Proof Candidate", password: "password-password" }) { $authFields } }"""
        )
        val (token, subjectId) = authResult(login, "login")
        val (subjectToken, controlToken) = seedAttributedDeltaRows(subjectId)
        val eventId = seedOutboxEvent(database, subjectId, "Published")
        val claimEventId = seedOutboxEvent(database, subjectId, "Retryable")
        eventually(
          Option(database.getCollection("event_outbox").find(Filters.eq("_id", claimEventId)).first())
        )(_.exists(_.getString("state") == "Published"))
        val registeredIds = eventually(
          Option(database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first())
            .flatMap(doc => Option(doc.getList("transactionalIds", classOf[String])))
            .map(_.asScala.toVector)
        )(_.exists(_.nonEmpty)).get
        val heldTransactionalId = registeredIds.last
        val (purgedRequest, deletedFence, receiptId) = Using.resource(
          heldPublisherTransaction(kafka, heldTransactionalId)
        ) { producer =>
          producer.send(new ProducerRecord(topic, s"held-deletion-proof-$claimEventId", "{}")).get()
          val deletion = graphql(
            api,
            s"""mutation { deleteMyAccount(input: { idempotencyKey: "${UUID
                .randomUUID()}" }) { __typename ... on DeletionReceipt { receiptId status } ... on DomainError { code message } ... on ValidationError { code message } } }""",
            Some(token)
          )
          val receiptId =
            "(?s)\\\"deleteMyAccount\\\".*?\\\"receiptId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"status\\\"\\s*:\\s*\\\"PENDING\\\"".r
              .findFirstMatchIn(deletion)
              .map(_.group(1))
              .getOrElse(fail("deleteMyAccount did not return PENDING"))
          val request = eventually(
            Option(database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first())
          )(_.nonEmpty).get
          assert(request.getString("receiptId") == receiptId, "deletion receipt must match the durable request")
          assert(
            Option(request.getList("transactionalIds", classOf[String]))
              .exists(_.asScala.contains(heldTransactionalId)),
            "deletion must copy the real publisher claim's transactional ID"
          )
          val purgedRequest = eventually(
            Option(database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first())
          )(doc => doc.exists(_.getString("phase") == ErasurePhase.DeltaPurged.toString)).get
          val staleCommit = Try(producer.commitTransaction()).failed.toOption
          assert(staleCommit.exists(isFenced), "held publisher transaction must fail after worker fencing")
          assertEquals(
            deletionStatus(api, receiptId, token),
            "PENDING",
            "real Delta retention horizon must remain pending"
          )
          val deletedFence =
            database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first()
          (purgedRequest, deletedFence, receiptId)
        }
        assert(
          java.lang.Boolean.TRUE == deletedFence.getBoolean("deleted"),
          "account deletion must close the publisher fence"
        )
        val barrier = Option(purgedRequest.get("kafkaRetentionBarrier", classOf[Document]))
          .getOrElse(fail("missing persisted Kafka barrier"))
        assertEquals(barrier.getString("topic"), topic)
        assert(barrier.getList("partitions", classOf[Document]).asScala.nonEmpty)
        assertEquals(database.getCollection("event_outbox").countDocuments(Filters.eq("_id", eventId)), 0L)
        val deltaEvidence = database
          .getCollection("analytics_erasure_delta_files")
          .find(Filters.eq("requestId", subjectId))
          .into(new java.util.ArrayList[Document]())
          .asScala
          .toVector
        assert(
          deltaEvidence.exists(_.getString("filePath").contains("bronze/operational_events/")),
          "worker must persist evidence for the populated subject-attributed Bronze file"
        )
        assert(
          purgedRequest.getLong("deltaAffectedRows") > 0L,
          "worker must record that populated Delta rows were purged"
        )
        val spark = SparkSession
          .builder()
          .master("local[1]")
          .appName("AccountDeletionComposeIntegrationSpecVerify")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "1")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
        try {
          val paths =
            IntegrationAnalyticsLakehousePaths.unsafe(
              new File(analyticsDir).toURI.toString.stripSuffix("/") + "/lakehouse"
            )
          val currentBronze = spark.read.format("delta").load(paths.bronze)
          assertEquals(
            currentBronze.filter(array_contains(col("subjectTokens"), subjectToken)).count(),
            0L,
            "subject-attributed Delta row must be removed from current snapshot"
          )
          assertEquals(
            currentBronze.filter(array_contains(col("subjectTokens"), controlToken)).count(),
            1L,
            "unrelated subject row must remain in current snapshot"
          )
        } finally spark.stop()
        if (completionEnabled) {
          passShortKafkaRetention(kafka, topic, barrier, nonce)
          val purgedAt = purgedRequest.getDate("deltaPurgedAt").toInstant
          eventually(Instant.now(), 2.minutes)(now => !now.isBefore(purgedAt.plusSeconds(60)))
          coordinateCalendarRestart(subjectId)
          eventually(deletionStatus(api, receiptId, token), 10.minutes)(_ == "COMPLETE")
          val completed =
            database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first()
          assertEquals(completed.getString("state"), "Complete")
          assertEquals(completed.getString("phase"), ErasurePhase.ReportPublished.toString)
          assertEquals(
            database.getCollection("analytics_erasure_completions").countDocuments(Filters.eq("_id", subjectId)),
            1L
          )
          assertEquals(
            database
              .getCollection("analytics_report_control")
              .find(Filters.eq("_id", "analytics-report"))
              .first()
              .getString("state"),
            "Published"
          )
          deltaEvidence.foreach { entry =>
            val containerPath = URI.create(entry.getString("filePath")).getPath
            assert(
              containerPath.startsWith("/var/lib/hiring-analytics/"),
              "captured file must belong to isolated mount"
            )
            val local =
              Path.of(analyticsDir).resolve(containerPath.stripPrefix("/var/lib/hiring-analytics/")).normalize()
            assert(local.startsWith(Path.of(analyticsDir)), "captured file must stay within isolated data")
            assert(!Files.exists(local), "captured Delta data/log path must be physically absent")
          }
          assert(
            deltaEvidence.exists(_.getString("filePath").contains("/_delta_log/")),
            "log evidence must be populated"
          )
          assert(
            deltaEvidence.exists(!_.getString("filePath").contains("/_delta_log/")),
            "data evidence must be populated"
          )
          val finalSpark = SparkSession
            .builder()
            .master("local[1]")
            .appName("AccountDeletionCompletionVerification")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
          try {
            val finalPaths = IntegrationAnalyticsLakehousePaths.unsafe(
              new File(analyticsDir).toURI.toString.stripSuffix("/") + "/lakehouse"
            )
            val surviving = finalSpark.read.format("delta").load(finalPaths.bronze)
            assertEquals(surviving.filter(array_contains(col("subjectTokens"), subjectToken)).count(), 0L)
            assertEquals(
              surviving.filter(array_contains(col("subjectTokens"), controlToken)).count(),
              1L,
              "unrelated row must survive actual VACUUM and accelerated log cleanup"
            )
          } finally finalSpark.stop()
          println(
            s"ACCOUNT_DELETION_COMPLETE authenticatedOwnerReceipt=true completionCount=1 report=Published exactCapturedPathsAbsent=${deltaEvidence.size} deltaCalendar=simulated kafkaAndDataRetention=real"
          )
        }
        val phase = database
          .getCollection("analytics_erasure_requests")
          .find(Filters.eq("_id", subjectId))
          .first()
          .getString("phase")
        assertEquals(
          phase,
          if (completionEnabled) ErasurePhase.ReportPublished.toString else ErasurePhase.DeltaPurged.toString
        )
      } finally client.close()
    }
  }
}
