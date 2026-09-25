package com.example.hiring.analytics

import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.client.model.Filters
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.common.serialization.StringSerializer
import org.bson.Document
import munit.FunSuite

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Date, Properties, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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
    assert(apiUri.getScheme == "http" && Option(apiUri.getHost).exists(isLoopback), "Compose proof API must use loopback HTTP")
    assert(Option(apiUri.getRawPath).forall(path => path.isEmpty || path == "/"), "Compose proof API URL must be a loopback origin")

    val mongoUri = new com.mongodb.ConnectionString(mongo)
    assert(!mongoUri.isSrvProtocol, "Compose proof Mongo URI must use mongodb://")
    assert(mongoUri.getHosts != null && !mongoUri.getHosts.isEmpty && mongoUri.getHosts.asScala.forall { host =>
      isLoopback(hostName(host))
    }, "Compose proof Mongo hosts must all be loopback")

    val kafkaHosts = kafka.split(",", -1).toVector
    assert(kafkaHosts.nonEmpty && kafkaHosts.forall(host => isLoopback(hostName(host))), "Compose proof Kafka endpoints must all be loopback")
  }

  private def graphql(apiBase: String, query: String, token: Option[String] = None): String = {
    val payload = new Document("query", query).toJson
    val builder = HttpRequest.newBuilder(URI.create(apiBase.stripSuffix("/") + "/graphql"))
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
    val pattern = ("(?s)\\\"" + field + "\\\"\\s*:\\s*\\{.*?\\\"__typename\\\"\\s*:\\s*\\\"AuthSuccess\\\".*?\\\"accessToken\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"user\\\"\\s*:\\s*\\{.*?\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").r
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

  private def isProducerFencingFailure(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
      case _: ProducerFencedException       => true
      case _: InvalidProducerEpochException => true
      case _                                => false
    }

  private def publisherProperties(bootstrap: String, transactionalId: String): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "PLAIN")
    props.put("sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"hiring_publisher_v2\" password=\"${required("KAFKA_PUBLISHER_V2_PASSWORD")}\";")
    props
  }

  private def seedPublishableEvent(database: MongoDatabase, subjectId: String): String = {
    val now = new Date()
    val eventId = UUID.randomUUID().toString
    val eventJson =
      s"""{"eventId":"$eventId","eventType":"SEARCH_PERFORMED","occurredAt":"${now.toInstant}","aggregateType":"Search","aggregateId":"deletion-proof-$eventId","actorId":"$subjectId","payload":{"searchKind":"jobs","query":"proof","results":[]}}"""
    database.getCollection("event_outbox").insertOne(
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

  private def deletionStatus(apiBase: String, receiptId: String): String = {
    val body = new Document("query", "query($receiptId: ID!) { accountDeletionStatus(receiptId: $receiptId) }")
      .append("variables", new Document("receiptId", receiptId)).toJson
    val request = HttpRequest.newBuilder(URI.create(apiBase.stripSuffix("/") + "/graphql"))
      .header("Content-Type", "application/json").header("Accept", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    assertEquals(response.statusCode(), 200)
    "\\\"accountDeletionStatus\\\"\\s*:\\s*\\\"(PENDING|COMPLETE)\\\"".r.findFirstMatchIn(response.body()).map(_.group(1)).getOrElse(fail("missing deletion status"))
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
      val nonce = databaseName.stripPrefix("account_deletion_")
      assertEquals(topic, s"hiring.deletion.$nonce", "database and topic must use the same proof nonce")
      validateLocalEndpoints(api, uri, kafka)
      val client: MongoClient = MongoClients.create(uri)
      try {
        val database = client.getDatabase(databaseName)
        List("users", "event_outbox", "analytics_erasure_requests", "outbox_subject_fences").foreach { name =>
          assertEquals(database.getCollection(name).countDocuments(), 0L, s"task collection $name must have no preexisting rows")
        }
        eventually(Option(database.getCollection("analytics_worker_heartbeats").find(Filters.eq("_id", "analytics-erasure")).first()))(_.exists(_.getString("state") == "Ready"))
        val admin = graphql(api, s"""mutation { bootstrapAdmin(input: { idempotencyKey: "${UUID.randomUUID()}", name: "Compose Proof Candidate", password: "password-password" }) { $authFields } }""")
        val adminId = authResult(admin, "bootstrapAdmin")._2
        val changed = database.getCollection("users").updateOne(
          Filters.eq("_id", adminId),
          new Document("$set", new Document("role", "Candidate").append("profile", new Document("kind", "Candidate").append("skills", java.util.List.of("Scala")).append("skillsCanonical", java.util.List.of("scala")).append("recruiterSearchOptIn", false)))
            .append("$unset", new Document("adminSingletonKey", ""))
        )
        assertEquals(changed.getMatchedCount, 1L)
        val login = graphql(api, s"""mutation { login(input: { idempotencyKey: "${UUID.randomUUID()}", name: "Compose Proof Candidate", password: "password-password" }) { $authFields } }""")
        val (token, subjectId) = authResult(login, "login")
        val eventId = seedPublishableEvent(database, subjectId)
        val fence = eventually {
          Option(database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first())
        }(_.exists(doc => Option(doc.getList("transactionalIds", classOf[String])).exists(!_.isEmpty)))
          .get
        val transactionalIds = fence.getList("transactionalIds", classOf[String]).asScala.toVector
        val transactionalId = transactionalIds.last
        assert(transactionalId.startsWith("hiring-publisher-"), "ID must be registered by the production publisher claim")
        eventually(Option(database.getCollection("event_outbox").find(Filters.eq("_id", eventId)).first()))(
          _.exists(_.getString("state") == "Published")
        )

        val producer = new KafkaProducer[String, String](publisherProperties(kafka, transactionalId))
        try {
          producer.initTransactions()
          producer.beginTransaction()
          producer.send(new ProducerRecord(topic, subjectId, s"open-deletion-transaction-$subjectId")).get(30, java.util.concurrent.TimeUnit.SECONDS)
          val deletion = graphql(api, s"""mutation { deleteMyAccount(input: { idempotencyKey: "${UUID.randomUUID()}" }) { __typename ... on DeletionReceipt { receiptId status } ... on DomainError { code message } ... on ValidationError { code message } } }""", Some(token))
          val receiptId = "(?s)\\\"deleteMyAccount\\\".*?\\\"receiptId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"status\\\"\\s*:\\s*\\\"PENDING\\\"".r.findFirstMatchIn(deletion).map(_.group(1)).getOrElse(fail("deleteMyAccount did not return PENDING"))
          val request = eventually(Option(database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first()))(_.nonEmpty).get
          assert(request.getString("receiptId") == receiptId, "deletion receipt must match the durable request")
          val captured = Option(request.getList("transactionalIds", classOf[String])).fold(Vector.empty[String])(_.asScala.toVector)
          assert(captured.contains(transactionalId), "deletion must capture the production claim's transactional ID")
          val purgedRequest = eventually(
            Option(database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first())
          )(doc => doc.exists(_.getString("phase") == ErasurePhase.DeltaPurged.toString)).get
          assertEquals(deletionStatus(api, receiptId), "PENDING", "real Delta retention horizon must remain pending")
          val deletedFence = database.getCollection("outbox_subject_fences").find(Filters.eq("_id", subjectId)).first()
          assert(java.lang.Boolean.TRUE == deletedFence.getBoolean("deleted"), "account deletion must close the publisher fence")
          val barrier = Option(purgedRequest.get("kafkaRetentionBarrier", classOf[Document])).getOrElse(fail("missing persisted Kafka barrier"))
          assertEquals(barrier.getString("topic"), topic)
          assert(barrier.getList("partitions", classOf[Document]).asScala.nonEmpty)
          assertEquals(database.getCollection("event_outbox").countDocuments(Filters.eq("_id", eventId)), 0L)
          val staleCommit = try { producer.commitTransaction(); None } catch { case scala.util.control.NonFatal(error) => Some(error) }
          assert(staleCommit.exists(isProducerFencingFailure), clues(staleCommit))
          val phase = database.getCollection("analytics_erasure_requests").find(Filters.eq("_id", subjectId)).first().getString("phase")
          assertEquals(phase, ErasurePhase.DeltaPurged.toString)
        } finally producer.close(Duration.ofSeconds(5))
      } finally client.close()
    }
  }
}
