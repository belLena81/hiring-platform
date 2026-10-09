package com.example.hiring.analytics
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.mongo.*

import cats.Applicative
import cats.effect.{Clock, Deferred, IO, Ref, Resource}
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import mongo4cats.client.MongoClient as CatsMongoClient
import mongo4cats.database.MongoDatabase as CatsMongoDatabase
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
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices, RecoveryApiProcess}
import java.time.Duration
import java.util.UUID
import java.util.Properties
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Opt-in local proof that a failed producer fence recovers in the running worker and reaches the public receipt. */
final class AccountDeletionRecoveryIntegrationSpec extends munit.FunSuite {
  override val munitTimeout: FiniteDuration = 10.minutes

  private val enabled = sys.env.get("HIRING_ACCOUNT_DELETION_RECOVERY_EVIDENCE").contains("true")
  private def required(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(fail(s"$name is required for recovery evidence"))

  private def apiProcess(manifest: LocalTestServices.Manifest, database: String, root: Path): Resource[IO, String] = {
    val prepare = IO.blocking {
      val workspace = Path.of(manifest.workspace)
      val classpathFile = Path.of(required("HIRING_RECOVERY_CLASSPATH_FILE")).toRealPath()
      require(
        classpathFile.startsWith(workspace.resolve(".local/logs/test-services")),
        "API classpath must belong to the isolated test wrapper"
      )
      val classpath = Files
        .readAllLines(classpathFile)
        .asScala
        .find(line => !line.startsWith("[") && line.contains("/target/") && line.contains(".jar"))
        .getOrElse(throw new IllegalArgumentException("Missing compiled API classpath"))
      val socket = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))
      val port = try socket.getLocalPort
      finally socket.close()
      val config = root.resolve("api.conf")
      val secret = UUID.randomUUID().toString + UUID.randomUUID().toString
      val contents = s"""include classpath("application.conf")
                           |mongo.uri = "${manifest.mongoUri}"
                           |mongo.database = "$database"
                           |mongo.reset-on-start = false
                           |http.host = "127.0.0.1"
                           |http.port = $port
                           |auth.jwt.hs256-secret = "$secret"
                           |auth.admin-seed.enabled = true
                           |auth.admin-seed.name = "Recovery Admin"
                           |auth.admin-seed.password = "password-password"
                           |kafka.enabled = false
                           |vector-search.enabled = false
                           |kafka.interview.enabled = false
                           |""".stripMargin
      Files.writeString(config, contents)
      Files.setPosixFilePermissions(config, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"))
      val builder = new ProcessBuilder(
        RecoveryApiProcess.command(
          List(
            Path.of(System.getProperty("java.home"), "bin", "java").toString,
            "-Dotel.sdk.disabled=true",
            s"-Dconfig.file=$config",
            "-cp",
            classpath,
            "com.example.graphQL.cats.Main"
          )
        )*
      )
      builder.directory(workspace.toFile)
      // Inherited application/provider configuration cannot override synthetic fixture configuration.
      builder.environment().clear()
      builder
        .redirectErrorStream(true)
        .redirectOutput(
          workspace.resolve(".local/logs/test-services").resolve(root.getFileName.toString + "-api.log").toFile
        )
      (builder, s"http://127.0.0.1:$port")
    }
    Resource.eval(prepare).flatMap { case (builder, url) =>
      Resource
        .make(IO.blocking(builder.start())) { process =>
          IO.blocking {
            process.destroy()
            if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
              process.destroyForcibly()
              val _ = process.waitFor()
            }
          }.void
        }
        .evalMap { process =>
          val ready = IO
            .blocking {
              require(process.isAlive, "Isolated recovery API exited before readiness")
              val request =
                HttpRequest.newBuilder(URI.create(url + "/ready")).timeout(Duration.ofSeconds(2)).GET().build()
              HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).statusCode() == 200
            }
            .handleErrorWith(error => if (process.isAlive) IO.pure(false) else IO.raiseError(error))
          eventually(ready)(identity).timeout(90.seconds).as(url)
        }
    }
  }

  private def resources: Resource[
    IO,
    (
        MongoClient,
        CatsMongoClient[IO],
        SparkSession,
        Path,
        CatsMongoDatabase[IO],
        KafkaTestNamespace.Namespace,
        String,
        String
    )
  ] =
    for {
      manifest <- Resource.eval(
        LocalTestServices.manifest.flatMap(
          _.liftTo[IO](new IllegalArgumentException("Recovery requires the isolated test manifest"))
        )
      )
      _ <- Resource.eval(LocalTestServices.verifiedMongo(manifest))
      namespace <- KafkaTestNamespace.resource.evalMap(
        _.liftTo[IO](new IllegalArgumentException("Recovery requires an isolated Kafka namespace"))
      )
      client <- Resource.fromAutoCloseable(IO.blocking(MongoClients.create(manifest.mongoUri)))
      reactiveClient <- CatsMongoClient.fromConnectionString[IO](manifest.mongoUri)
      database <- LocalTestServices.database(reactiveClient)
      root <- Resource.make(
        IO.blocking(
          Files.createTempDirectory(
            Path.of(manifest.workspace).resolve(".local/data/test-services"),
            "account-deletion-recovery-"
          )
        )
      )(deleteTree)
      nonce <- Resource.eval(IO.randomUUID.map(_.toString.replace("-", "")))
      _ <- Resource.eval(
        IO.blocking(
          client
            .getDatabase(database.name)
            .getCollection("account_deletion_recovery_fixture")
            .insertOne(new Document("_id", database.name).append("nonce", nonce).append("state", "Prepared"))
        )
      )
      api <- apiProcess(manifest, database.name, root)
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
    } yield (client, reactiveClient, spark, root, database, namespace, api, nonce)

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
          new Document("_id", subjectId).append("deleted", false),
          new ReplaceOptions().upsert(true)
        )
      database
        .getCollection("producer_registrations")
        .insertOne(
          new Document("_id", s"$subjectId:$transactionalId")
            .append("subjectId", subjectId)
            .append("transactionalId", transactionalId)
            .append("kind", "Operational")
            .append("state", "Active")
            .append("registeredAt", java.util.Date.from(java.time.Instant.now()))
        )
      val _ = database
        .getCollection("event_outbox")
        .insertOne(
          new Document("_id", UUID.randomUUID().toString)
            .append("state", "Retryable")
            .append("subjectIds", java.util.List.of(subjectId))
            .append("subjectRefsVersion", 1)
        )
    }

  private def openPublisherTransaction(
      transactionalId: String,
      subjectId: String,
      namespace: KafkaTestNamespace.Namespace
  ): IO[KafkaProducer[String, String]] =
    IO.blocking {
      val properties = new Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, namespace.manifest.kafkaBootstrap)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
      properties.put(ProducerConfig.ACKS_CONFIG, "all")
      properties.put("security.protocol", "SASL_PLAINTEXT")
      properties.put("sasl.mechanism", "PLAIN")
      properties.put(
        "sasl.jaas.config",
        s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"hiring_publisher_v2\" password=\"${namespace.manifest.publisherPassword}\";"
      )
      val producer = new KafkaProducer[String, String](properties)
      try {
        producer.initTransactions()
        producer.beginTransaction()
        producer
          .send(new ProducerRecord(namespace.events, subjectId, s"in-flight-deletion-fixture-$subjectId"))
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

  private def registerCandidateFixture(apiBase: String): IO[(String, String)] =
    graphql(
      apiBase,
      s"""mutation { signUp(input: { idempotencyKey: "${UUID
          .randomUUID()}", name: "Recovery Candidate", role: CANDIDATE, skills: ["Scala"], password: "password-password" }) { $authFragment } }"""
    ).map(authSuccess(_, "signUp"))

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

  private def status(apiBase: String, receiptId: String, token: String): IO[String] = IO.blocking {
    val endpoint = URI.create(apiBase.stripSuffix("/") + "/graphql")
    val body = new Document(
      "query",
      "query($receiptId: ID!) { accountDeletionStatus(receiptId: $receiptId) }"
    ).append("variables", new Document("receiptId", receiptId)).toJson
    val request = HttpRequest
      .newBuilder(endpoint)
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .header("Authorization", s"Bearer $token")
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

  test(
    new munit.TestOptions("running worker retries a failed fence and completes the same public deletion receipt")
      .withTags(if (enabled) Set.empty else Set(munit.Ignore))
  ) {
    if (enabled) {

      val transactionalId = "hiring-publisher-recovery-" + UUID.randomUUID().toString
      val test = resources.use {
        case (client, reactiveClient, spark, root, reactiveDatabase, namespace, apiBase, ownershipNonce) =>
          val databaseName = reactiveDatabase.name
          assert(databaseName.matches("hiring_test_[0-9a-f]{32}"), "recovery requires a generated test database")
          val database = client.getDatabase(databaseName)
          val topic = namespace.events
          val reader = KafkaConnection(
            namespace.manifest.kafkaBootstrap,
            Some("analytics_reader"),
            Some(namespace.manifest.readerPassword),
            securityProtocol = com.example.hiring.analytics.config.KafkaSecurityProtocol.SaslPlaintext,
            allowPlaintext = true
          )
          val fencer = KafkaConnection(
            namespace.manifest.kafkaBootstrap,
            Some("analytics_fencer"),
            Some(namespace.manifest.fencerPassword),
            securityProtocol = com.example.hiring.analytics.config.KafkaSecurityProtocol.SaslPlaintext,
            allowPlaintext = true
          )
          val syntheticHmac = java.util.Base64.getEncoder
            .encodeToString((UUID.randomUUID().toString + UUID.randomUUID().toString).getBytes(StandardCharsets.UTF_8))
          val pseudonymizer = AnalyticsTestSubjectPseudonymizer.fromBase64(syntheticHmac)
          val store = AnalyticsErasureWorkerTestSupport.stores(reactiveClient, reactiveDatabase)
          val publisher = new MongoAnalyticsReportPublisher[IO](
            reactiveClient,
            reactiveDatabase,
            operational = AnalyticsTestOperationalConfig.operational
          )
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
            (candidateToken, requestId) <- registerCandidateFixture(apiBase)
            _ <- seedOutboxWork(database, requestId, transactionalId)
            result <- Resource
              .make(openPublisherTransaction(transactionalId, requestId, namespace))(producer =>
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
                  retention = new KafkaRetention[IO] {
                    override def capture(): IO[KafkaRetentionBarrier] =
                      KafkaRetentionAdapter.capture(
                        reader,
                        AnalyticsTopic.from(topic).toOption.get,
                        AnalyticsBatchTestSupport.driverExecution
                      )
                    override def retentionPassed(barrier: KafkaRetentionBarrier): IO[Boolean] =
                      retentionReady.get
                  }
                  producerFencer = new TransactionalProducerFencer[IO] {
                    override def fence(connection: KafkaConnection, ids: Vector[String]): IO[Unit] =
                      attempt.updateAndGet(_ + 1).flatMap {
                        case 1 =>
                          val invalid = connection.copy(saslPassword = connection.saslPassword.map(_ + "-invalid"))
                          KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution)
                            .fence(invalid, ids)
                            .attempt
                            .flatMap {
                              case Left(error)
                                  if Iterator
                                    .iterate(error)(_.getCause)
                                    .takeWhile(_ != null)
                                    .exists(_.isInstanceOf[AuthenticationException]) =>
                                firstFailure.complete(()) *> IO.raiseError(error)
                              case _ => IO.raiseError(new AssertionError("bad fencer credentials were not rejected"))
                            }
                        case 2 =>
                          secondStarted
                            .complete(()) *> continueSecond.get *> KafkaProducerFencer[IO](
                            AnalyticsBatchTestSupport.driverExecution
                          ).fence(connection, ids)
                        case _ => IO.raiseError(new AssertionError("unexpected additional fence attempt"))
                      }
                  }
                  worker = AnalyticsErasureWorkerTestSupport.worker(
                    spark,
                    reactiveDatabase,
                    store,
                    reader,
                    fencer,
                    topic,
                    IntegrationAnalyticsLakehousePaths.unsafe(root.toUri.toString.stripSuffix("/")),
                    pseudonymizer,
                    publisher,
                    clock = clock,
                    leaseDuration = 4.seconds,
                    pollInterval = 200.millis,
                    producerFencer = producerFencer,
                    kafkaRetention = Some(retention)
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
                      _ = assert(
                        request.getString("receiptId") == receiptId,
                        "deleteMyAccount receipt binding mismatch"
                      )
                      registration <- IO.blocking(
                        database
                          .getCollection("producer_registrations")
                          .find(
                            new Document("subjectId", requestId)
                              .append("transactionalId", transactionalId)
                              .append("kind", "Operational")
                              .append("state", "Active")
                          )
                          .first()
                      )
                      _ = assert(
                        registration != null && !request.containsKey("transactionalIds"),
                        "deleteMyAccount did not preserve active producer attribution"
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
                      pending <- status(apiBase, receiptId, candidateToken)
                      _ = assertEquals(pending, "PENDING")
                      barrier <- store.barrier.readBarrier(
                        AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId)
                      )
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
                      savedBarrier <- store.barrier.readBarrier(
                        AnalyticsErasureWorkerTestSupport.accountSubjectId(requestId)
                      )
                      _ = assert(
                        savedBarrier
                          .exists(value => AnalyticsTopic.unwrap(value.topic) == topic && value.partitions.nonEmpty)
                      )
                      _ <- assertPublisherTransactionFenced(inFlightProducer)
                      _ <- retentionReady.set(true)
                      _ <- clockOffset.set(31.days)
                      complete <- eventually(status(apiBase, receiptId, candidateToken))(_ == "COMPLETE")
                        .timeout(4.minutes)
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
