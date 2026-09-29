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
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.bson.Document
import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, TimestampType}

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
import scala.util.Using

/** Live API-to-Compose-worker proof on a disposable task-scoped Mongo/Kafka/Delta stack. */
final class AccountDeletionComposeIntegrationSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 10.minutes
  private val enabled = sys.env.get("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ENABLED").contains("true")

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
    assert(!response.body().contains("\"errors\":"), "GraphQL returned errors")
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

  private def transactionEpoch(bootstrap: String, transactionalId: String): Int = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "PLAIN")
    props.put(
      "sasl.jaas.config",
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"analytics_fencer\" password=\"${required("KAFKA_FENCER_PASSWORD")}\";"
    )
    val admin = Admin.create(props)
    try admin.describeTransactions(java.util.List.of(transactionalId)).all().get().get(transactionalId).producerEpoch()
    finally admin.close(Duration.ofSeconds(5))
  }

  private def seedPublishableEvent(database: MongoDatabase, subjectId: String): String = {
    val now = new Date()
    val eventId = UUID.randomUUID().toString
    val eventJson =
      s"""{"eventId":"$eventId","eventType":"SEARCH_PERFORMED","occurredAt":"${now.toInstant}","aggregateType":"Search","aggregateId":"deletion-proof-$eventId","actorId":"$subjectId","payload":{"searchKind":"jobs","query":"proof","results":[]}}"""
    database
      .getCollection("event_outbox")
      .insertOne(
        new Document("_id", eventId)
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
          .append("state", "Retryable")
          .append("attempts", 0)
          .append("availableAt", now)
          .append("createdAt", now)
          .append("updatedAt", now)
      )
    eventId
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
    val paths = AnalyticsLakehousePaths.unsafe(root.toURI.toString.stripSuffix("/") + "/lakehouse")
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

  test("production publisher claim is captured and fenced by the long-lived Compose erasure worker") {
    if (enabled) {
      val databaseName = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_DATABASE")
      assert(databaseName.matches("account_deletion_[0-9a-f]{16}"))
      val topic = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_TOPIC")
      assert(topic.matches("hiring[.]deletion[.][0-9a-f]{16}"))
      val uri = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_MONGO_URI")
      val api = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_API_URL")
      val kafka = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_KAFKA")
      val analyticsDir = required("HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ANALYTICS_DIR")
      val publisherProofDir = Path.of(required("HIRING_ACCOUNT_DELETION_PUBLISHER_PROOF_DIR"))
      assertEquals(new File(analyticsDir).getCanonicalPath, new File(analyticsDir).getAbsolutePath)
      assertEquals(publisherProofDir.toRealPath(), publisherProofDir.toAbsolutePath.normalize())
      assertEquals(publisherProofDir.getFileName.toString, "publisher")
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
        Files.createFile(publisherProofDir.resolve("armed"))
        val eventId = seedPublishableEvent(database, subjectId)
        eventually(Files.exists(publisherProofDir.resolve("held")))(identity)
        val fence = eventually {
          Option(database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first())
        }(_.exists(doc => Option(doc.getList("transactionalIds", classOf[String])).exists(!_.isEmpty))).get
        val transactionalIds = fence.getList("transactionalIds", classOf[String]).asScala.toVector
        val transactionalId = transactionalIds.last
        assert(
          transactionalId.startsWith("hiring-publisher-"),
          "ID must be registered by the production publisher claim"
        )
        val heldOutbox = database.getCollection("event_outbox").find(Filters.eq("_id", eventId)).first()
        assertEquals(heldOutbox.getString("state"), "InFlight", "the real publisher must hold the subject lease")
        val initialEpoch = transactionEpoch(kafka, transactionalId)
        try {
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
          val captured =
            Option(request.getList("transactionalIds", classOf[String])).fold(Vector.empty[String])(_.asScala.toVector)
          assert(captured.contains(transactionalId), "deletion must capture the production claim's transactional ID")
          eventually(transactionEpoch(kafka, transactionalId))(_ > initialEpoch)
          Files.createFile(publisherProofDir.resolve("release"))
          val commitResult = eventually {
            if (Files.exists(publisherProofDir.resolve("result")))
              Some(Files.readString(publisherProofDir.resolve("result")))
            else None
          }(_.nonEmpty).get
          assertEquals(commitResult, "fenced", "the actual publisher's commit must fail due to broker fencing")
          val purgedRequest = eventually(
            Option(database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first())
          )(doc => doc.exists(_.getString("phase") == ErasurePhase.DeltaPurged.toString)).get
          assertEquals(
            deletionStatus(api, receiptId, token),
            "PENDING",
            "real Delta retention horizon must remain pending"
          )
          val deletedFence = database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first()
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
              AnalyticsLakehousePaths.unsafe(new File(analyticsDir).toURI.toString.stripSuffix("/") + "/lakehouse")
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
          val phase = database
            .getCollection("analytics_erasure_requests")
            .find(Filters.eq("_id", subjectId))
            .first()
            .getString("phase")
          assertEquals(phase, ErasurePhase.DeltaPurged.toString)
        } finally
          if (!Files.exists(publisherProofDir.resolve("release")))
            Files.createFile(publisherProofDir.resolve("release"))
      } finally client.close()
    }
  }
}
