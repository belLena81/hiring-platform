package com.example.hiring.analytics

import com.example.hiring.analytics.batch.*
import com.example.hiring.analytics.erasure.*
import com.example.hiring.analytics.mongo.*

import cats.Applicative
import cats.effect.{Clock, Deferred, IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.reactivestreams.client.{MongoClient as ReactiveMongoClient, MongoClients as ReactiveMongoClients}
import com.mongodb.client.model.ReplaceOptions
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Date, UUID}
import java.util.Properties
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Opt-in local proof that a failed producer fence recovers in the running worker and reaches the public receipt. */
final class AccountDeletionRecoveryIntegrationSpec extends munit.FunSuite {
  override val munitTimeout: FiniteDuration = 10.minutes

  private val enabled = sys.env.get("HIRING_ACCOUNT_DELETION_RECOVERY_EVIDENCE").contains("true")
  private val topic = "hiring.operational-events"
  private val brokers = "127.0.0.1:9092"
  private val mongoUri = "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"

  private def required(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(fail(s"$name is required for recovery evidence"))

  private def resources: Resource[IO, (MongoClient, ReactiveMongoClient, SparkSession, Path)] =
    for {
      client <- Resource.fromAutoCloseable(IO.blocking(MongoClients.create(mongoUri)))
      reactiveClient <- Resource.fromAutoCloseable(IO.delay(ReactiveMongoClients.create(mongoUri)))
      root <- Resource.make(IO.blocking(Files.createTempDirectory("account-deletion-recovery-")))(deleteTree)
      spark <- Resource.make(IO.blocking {
        SparkSession
          .builder()
          .master("local[1]")
          .appName("AccountDeletionRecoveryIntegrationSpec")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      })(session => IO.blocking(session.stop()))
    } yield (client, reactiveClient, spark, root)

  private def deleteTree(root: Path): IO[Unit] = IO.blocking {
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()
  }.void

  private def seedOutboxWork(database: MongoDatabase, subjectId: String, transactionalId: String): IO[Unit] =
    IO.blocking {
      val collections = database.listCollectionNames().into(new java.util.ArrayList[String]()).asScala.toSet
      if (!collections.contains("analytics_erasure_delta_files"))
        database.createCollection("analytics_erasure_delta_files")
      database
        .getCollection("outbox_subject_fences")
        .replaceOne(
          new Document("_id", subjectId),
          new Document("_id", subjectId)
            .append("deleted", false)
            .append("transactionalIds", java.util.List.of(transactionalId)),
          new ReplaceOptions().upsert(true)
        )
      database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("state", "Retryable")
            .append("subjectIds", java.util.List.of(subjectId))
            .append("subjectRefsVersion", 1)
        )
    }

  private def openPublisherTransaction(transactionalId: String, subjectId: String): IO[KafkaProducer[String, String]] =
    IO.blocking {
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      properties.put(
        "sasl.jaas.config",
        s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"hiring_publisher_v2\" password=\"${required("KAFKA_PUBLISHER_V2_PASSWORD")}\";"
      )
      val producer = new KafkaProducer[String, String](properties)
      try {
        producer.initTransactions()
        producer.beginTransaction()
        producer
          .send(new ProducerRecord(topic, subjectId, s"in-flight-deletion-fixture-$subjectId"))
          .get(30, java.util.concurrent.TimeUnit.SECONDS)
        producer
      } catch {
        case scala.util.control.NonFatal(error) =>
          producer.close(Duration.ofSeconds(5))
          throw error
      }
    }

  private def assertPublisherTransactionFenced(producer: KafkaProducer[String, String]): IO[Unit] = IO.blocking {
    val commitFailure = try {
      producer.commitTransaction()
      None
    } catch {
      case scala.util.control.NonFatal(error) => Some(error)
    }
    val wasFenced = commitFailure.exists { error =>
      Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists {
        case _: ProducerFencedException       => true
        case _: InvalidProducerEpochException => true
        case _                                => false
      }
    }
    assert(wasFenced, "the held publisher transaction was not fenced by account deletion")
  }

  private def graphql(apiBase: String, query: String, bearer: Option[String] = None): IO[String] = IO.blocking {
    val endpoint = URI.create(apiBase.stripSuffix("/") + "/graphql")
    val payload = new Document("query", query).toJson
    val builder = HttpRequest
      .newBuilder(endpoint)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .timeout(java.time.Duration.ofSeconds(15))
    bearer.foreach(token => builder.header("Authorization", "Bearer " + token))
    val request = builder.POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8)).build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    assertEquals(response.statusCode(), 200, "GraphQL endpoint returned a non-200 response")
    assert(!response.body().contains("\"errors\":"), "GraphQL returned an error response")
    response.body()
  }

  private val authFragment =
    """__typename ... on AuthSuccess { accessToken user { id } } ... on DomainError { code message } ... on ValidationError { code message }"""

  private def authSuccess(body: String, field: String): (String, String) = {
    val pattern =
      ("(?s)\\\"" + field + "\\\"\\s*:\\s*\\{.*?\\\"__typename\\\"\\s*:\\s*\\\"AuthSuccess\\\".*?\\\"accessToken\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"user\\\"\\s*:\\s*\\{.*?\\\"id\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").r
    pattern
      .findFirstMatchIn(body)
      .fold(fail(s"$field did not return AuthSuccess; response omitted the expected fields"))(matchValue =>
        (matchValue.group(1), matchValue.group(2))
      )
  }

  private def bootstrapCandidateFixtureAndLogin(apiBase: String, database: MongoDatabase): IO[(String, String)] =
    for {
      adminJson <- graphql(
        apiBase,
        s"""mutation { bootstrapAdmin(input: { idempotencyKey: "${UUID
            .randomUUID()}", name: "Recovery Admin", password: "password-password" }) { $authFragment } }"""
      )
      adminId = authSuccess(adminJson, "bootstrapAdmin")._2
      registryState <- IO.blocking(
        Option(database.getCollection("account_registry").find(new Document("_id", "user-account-registry")).first())
          .map(_.getString("state"))
      )
      adminPersisted <- IO.blocking(
        database.getCollection("users").find(new Document("_id", adminId)).first() != null
      )
      _ <- IO.raiseUnless(registryState.contains("Initialized") && adminPersisted)(
        new AssertionError(
          s"bootstrapAdmin response was not committed to the recovery database (registry=$registryState, user=$adminPersisted)"
        )
      )
      _ <- IO.blocking {
        val result = database
          .getCollection("users")
          .updateOne(
            new Document("_id", adminId),
            new Document(
              "$set",
              new Document("role", "Candidate")
                .append(
                  "profile",
                  new Document("kind", "Candidate")
                    .append("skills", java.util.List.of("Scala"))
                    .append("skillsCanonical", java.util.List.of("scala"))
                    .append("recruiterSearchOptIn", false)
                )
            ).append("$unset", new Document("adminSingletonKey", ""))
          )
        assertEquals(result.getMatchedCount, 1L)
      }
      candidateJson <- graphql(
        apiBase,
        s"""mutation { login(input: { idempotencyKey: "${UUID
            .randomUUID()}", name: "Recovery Admin", password: "password-password" }) { $authFragment } }"""
      )
      candidate = authSuccess(candidateJson, "login")
    } yield candidate

  private def deleteAccount(apiBase: String, token: String): IO[String] =
    graphql(
      apiBase,
      s"""mutation { deleteMyAccount(input: { idempotencyKey: "${UUID
          .randomUUID()}" }) { __typename ... on DeletionReceipt { receiptId status } ... on DomainError { code message } ... on ValidationError { code message } } }""",
      Some(token)
    ).map { body =>
      val pattern =
        "(?s)\\\"deleteMyAccount\\\"\\s*:\\s*\\{.*?\\\"__typename\\\"\\s*:\\s*\\\"DeletionReceipt\\\".*?\\\"receiptId\\\"\\s*:\\s*\\\"([^\\\"]+)\\\".*?\\\"status\\\"\\s*:\\s*\\\"PENDING\\\"".r
      pattern
        .findFirstMatchIn(body)
        .fold(fail("deleteMyAccount did not return a PENDING receipt; response omitted the expected fields"))(
          _.group(1)
        )
    }

  private def status(apiBase: String, receiptId: String): IO[String] = IO.blocking {
    val endpoint = URI.create(apiBase.stripSuffix("/") + "/graphql")
    val body = new Document(
      "query",
      "query($receiptId: ID!) { accountDeletionStatus(receiptId: $receiptId) }"
    ).append("variables", new Document("receiptId", receiptId)).toJson
    val request = HttpRequest
      .newBuilder(endpoint)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .timeout(java.time.Duration.ofSeconds(10))
      .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
      .build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    assertEquals(response.statusCode(), 200, "GraphQL status query returned a non-200 response")
    val pattern = "\"accountDeletionStatus\"\\s*:\\s*\"(PENDING|COMPLETE)\"".r
    pattern
      .findFirstMatchIn(response.body())
      .map(_.group(1))
      .getOrElse(
        fail("GraphQL receipt response did not contain an expected deletion status")
      )
  }

  private def eventually[A](read: IO[A])(matches: A => Boolean): IO[A] =
    read.flatMap(value => if (matches(value)) IO.pure(value) else IO.sleep(200.millis) *> eventually(read)(matches))

  test("running worker retries a failed fence and completes the same public deletion receipt") {
    if (enabled) {

      val databaseName = required("HIRING_ACCOUNT_DELETION_RECOVERY_DATABASE")
      assert(
        databaseName.matches("account_deletion_recovery_[0-9a-f]{32}"),
        "recovery evidence requires a unique disposable database name"
      )
      val apiBase = required("HIRING_ACCOUNT_DELETION_RECOVERY_API_URL")
      val ownershipNonce = required("HIRING_ACCOUNT_DELETION_RECOVERY_NONCE")
      assert(ownershipNonce.matches("[0-9a-f]{32}"), "recovery evidence requires a generated ownership nonce")
      assert(
        apiBase.matches("http://127[.]0[.]0[.]1:[0-9]{1,5}"),
        "recovery evidence requires a local API endpoint"
      )
      val reader = KafkaConnection(brokers, Some("analytics_reader"), Some(required("KAFKA_READER_PASSWORD")))
      val fencer = KafkaConnection(brokers, Some("analytics_fencer"), Some(required("KAFKA_FENCER_PASSWORD")))
      val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromBase64(required("HIRING_ANALYTICS_HMAC_SECRET_BASE64"))
      val transactionalId = "hiring-publisher-recovery-" + UUID.randomUUID().toString

      val test = resources.use { case (client, reactiveClient, spark, root) =>
        val database = client.getDatabase(databaseName)
        val reactiveDatabase = reactiveClient.getDatabase(databaseName)
        val store = new MongoAnalyticsErasureWorkerStore(reactiveClient, reactiveDatabase)
        val publisher = new MongoAnalyticsReportPublisher(reactiveClient, reactiveDatabase)
        for {
          claimedFixture <- IO.blocking(
            database
              .getCollection("account_deletion_recovery_fixture")
              .findOneAndUpdate(
                new Document("_id", databaseName).append("nonce", ownershipNonce).append("state", "Prepared"),
                new Document("$set", new Document("state", "Running"))
              ) != null
          )
          _ <- IO.raiseWhen(!claimedFixture)(new AssertionError("recovery database is not owned by this harness"))
          (candidateToken, requestId) <- bootstrapCandidateFixtureAndLogin(apiBase, database)
          _ <- seedOutboxWork(database, requestId, transactionalId)
          result <- Resource
            .make(openPublisherTransaction(transactionalId, requestId))(producer =>
              IO.blocking(producer.close(Duration.ofSeconds(5)))
            )
            .use { inFlightProducer =>
              for {
                clockOffset <- Ref.of[IO, FiniteDuration](scala.concurrent.duration.Duration.Zero)
                retentionReady <- Ref.of[IO, Boolean](false)
                attempt <- Ref.of[IO, Int](0)
                firstFailure <- Deferred[IO, Unit]
                secondStarted <- Deferred[IO, Unit]
                continueSecond <- Deferred[IO, Unit]
                clock = new Clock[IO] {
                  override val applicative: Applicative[IO] = Applicative[IO]
                  override def monotonic: IO[FiniteDuration] = IO.monotonic
                  override def realTime: IO[FiniteDuration] =
                    (IO.realTime, clockOffset.get).mapN(_ + _)
                }
                retention = new KafkaRetention {
                  override def capture(connection: KafkaConnection, name: String): IO[KafkaRetentionBarrier] =
                    KafkaRetentionBarrier.capture(connection, name)
                  override def retentionPassed(
                      connection: KafkaConnection,
                      barrier: KafkaRetentionBarrier
                  ): IO[Boolean] =
                    retentionReady.get
                }
                producerFencer = new TransactionalProducerFencer {
                  override def fence(connection: KafkaConnection, ids: Vector[String]): IO[Unit] =
                    attempt.updateAndGet(_ + 1).flatMap {
                      case 1 =>
                        val invalid = connection.copy(saslPassword = connection.saslPassword.map(_ + "-invalid"))
                        KafkaProducerFencer.fence(invalid, ids).attempt.flatMap {
                          case Left(error)
                              if Iterator
                                .iterate(error)(_.getCause)
                                .takeWhile(_ != null)
                                .exists(_.isInstanceOf[AuthenticationException]) =>
                            firstFailure.complete(()) *> IO.raiseError(error)
                          case _ => IO.raiseError(new AssertionError("bad fencer credentials were not rejected"))
                        }
                      case 2 =>
                        secondStarted.complete(()) *> continueSecond.get *> KafkaProducerFencer.fence(connection, ids)
                      case _ => IO.raiseError(new AssertionError("unexpected additional fence attempt"))
                    }
                }
                worker = new AnalyticsErasureWorker(
                  spark,
                  reactiveDatabase,
                  store,
                  reader,
                  fencer,
                  topic,
                  AnalyticsLakehousePaths(root.toUri.toString.stripSuffix("/")),
                  pseudonymizer,
                  publisher,
                  clock = clock,
                  leaseDuration = 4.seconds,
                  pollInterval = 200.millis,
                  producerFencer = producerFencer,
                  kafkaRetention = retention
                )
                _ <- Resource.make(worker.run.start)(_.cancel).use { _ =>
                  for {
                    _ <- eventually(
                      IO.blocking(
                        database
                          .getCollection("analytics_worker_heartbeats")
                          .find(new Document("_id", "analytics-erasure").append("state", "Ready"))
                          .first()
                      )
                    )(_ != null).timeout(60.seconds)
                    receiptId <- deleteAccount(apiBase, candidateToken)
                    request <- IO.blocking(
                      database
                        .getCollection("analytics_erasure_requests")
                        .find(new Document("_id", requestId))
                        .first()
                    )
                    _ = assert(request != null, "deleteMyAccount did not create the worker request")
                    _ = assert(request.getString("receiptId") == receiptId, "deleteMyAccount receipt binding mismatch")
                    capturedIds = Option(request.getList("transactionalIds", classOf[String]))
                      .fold(Vector.empty[String])(_.asScala.toVector)
                    _ = assert(
                      capturedIds.contains(transactionalId),
                      "deleteMyAccount did not capture the subject fence"
                    )
                    subjectFence <- IO.blocking(
                      database
                        .getCollection("outbox_subject_fences")
                        .find(new Document("_id", requestId))
                        .first()
                    )
                    _ = assert(subjectFence != null && subjectFence.getBoolean("deleted", false))
                    tombstoned <- IO.blocking(
                      database
                        .getCollection("users")
                        .find(new Document("_id", requestId).append("accountStatus", "Deleted"))
                        .first() != null
                    )
                    _ = assert(tombstoned, "deleteMyAccount did not tombstone the account")
                    _ <- firstFailure.get.timeout(60.seconds)
                    _ <- secondStarted.get.timeout(60.seconds)
                    pending <- status(apiBase, receiptId)
                    _ = assertEquals(pending, "PENDING")
                    barrier <- store.readBarrier(requestId)
                    _ = assertEquals(barrier, None)
                    outboxBefore <- IO.blocking(database.getCollection("event_outbox").countDocuments())
                    _ = assertEquals(outboxBefore, 1L)
                    _ <- continueSecond.complete(())
                    _ <- eventually(
                      IO.blocking(
                        database
                          .getCollection("analytics_erasure_requests")
                          .find(new Document("_id", requestId))
                          .first()
                      )
                    )(row =>
                      row != null && row.getString("phase") == ErasurePhase.DeltaPurged.toString && row.getDate(
                        "resumeAfter"
                      ) != null
                    ).timeout(4.minutes)
                    savedBarrier <- store.readBarrier(requestId)
                    _ = assert(savedBarrier.exists(value => value.topic == topic && value.partitions.nonEmpty))
                    _ <- assertPublisherTransactionFenced(inFlightProducer)
                    _ <- retentionReady.set(true)
                    _ <- clockOffset.set(31.days)
                    complete <- eventually(status(apiBase, receiptId))(_ == "COMPLETE").timeout(4.minutes)
                    _ = assertEquals(complete, "COMPLETE")
                    completionCount <- IO.blocking(
                      database
                        .getCollection("analytics_erasure_completions")
                        .countDocuments(new Document("receiptId", receiptId))
                    )
                    _ = assertEquals(completionCount, 1L)
                    outboxAfter <- IO.blocking(database.getCollection("event_outbox").countDocuments())
                    _ = assertEquals(outboxAfter, 0L)
                    control <- IO.blocking(
                      database
                        .getCollection("analytics_report_control")
                        .find(new Document("_id", "analytics-report"))
                        .first()
                    )
                    _ = assertEquals(control.getString("state"), "Published")
                    fenceAttempts <- attempt.get
                    _ = assertEquals(fenceAttempts, 2)
                  } yield ()
                }
              } yield ()
            }
        } yield result
      }
      test.unsafeRunSync()
    }
  }
}
