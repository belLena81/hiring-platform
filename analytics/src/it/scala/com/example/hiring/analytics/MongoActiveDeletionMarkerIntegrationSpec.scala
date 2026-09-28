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

import cats.effect.{Clock, IO}
import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients}
import com.mongodb.reactivestreams.client.{MongoClient as ReactiveMongoClient, MongoClients as ReactiveMongoClients}
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import munit.FunSuite

import java.nio.file.Files
import java.time.{Duration, Instant}
import java.util.Date
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class MongoActiveDeletionMarkerIntegrationSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private final class MongoContainer extends GenericContainer[MongoContainer](DockerImageName.parse(image))

  private val pseudonymizer =
    AnalyticsTestSubjectPseudonymizer.fromSecret("analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8"))
  private val markerClock = fixedClock(Instant.parse("2026-09-24T12:00:00Z"))
  private given cats.effect.Clock[IO] = markerClock
  private var mongoContainer: MongoContainer = scala.compiletime.uninitialized
  private var mongoClient: MongoClient = scala.compiletime.uninitialized
  private var reactiveMongoClient: ReactiveMongoClient = scala.compiletime.uninitialized
  private var spark: SparkSession = scala.compiletime.uninitialized

  private def manifest(runId: String): AnalyticsRunManifest =
    AnalyticsRunManifest
      .validated(runId, Vector(PartitionOffsetRange.unsafe("topic", 0, 0L, 1L)))
      .toEither
      .fold(errors => fail(errors.toString), identity)

  private def fixedClock(at: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: cats.Applicative[IO] = summon[cats.Applicative[IO]]
    override def realTime: IO[FiniteDuration] = IO.pure(at.toEpochMilli.millis)
    override def monotonic: IO[FiniteDuration] = IO.pure(0.nanos)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    mongoContainer = new MongoContainer
    mongoContainer
      .withExposedPorts(27017)
      .withCommand("mongod", "--bind_ip_all")
      .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
    mongoContainer.start()
    mongoClient = MongoClients.create(
      s"mongodb://${mongoContainer.getHost}:${mongoContainer.getMappedPort(27017)}"
    )
    reactiveMongoClient = ReactiveMongoClients.create(
      s"mongodb://${mongoContainer.getHost}:${mongoContainer.getMappedPort(27017)}"
    )
    spark = org.apache.spark.sql.classic.SparkSession
      .builder()
      .master("local[2]")
      .appName("MongoActiveDeletionMarkerIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null && !spark.sparkContext.isStopped) spark.stop()
    if (mongoClient != null) mongoClient.close()
    if (reactiveMongoClient != null) reactiveMongoClient.close()
    if (mongoContainer != null) mongoContainer.stop()
    super.afterAll()
  }

  test("Mongo marker source keeps unexpired completed markers active and ignores expired ones") {
    val database = mongoClient.getDatabase(s"markers_${UUID.randomUUID()}")
    val requests = database.getCollection("analytics_erasure_requests")
    val source = new MongoActiveDeletionMarkerSource[IO](
      reactiveMongoClient.getDatabase(database.getName),
      pseudonymizer
    )
    val retainedSubject = UUID.randomUUID().toString
    requests.insertOne(
      new Document("_id", retainedSubject)
        .append("state", "Complete")
        .append("expiresAt", Date.from(Instant.parse("2026-10-25T12:00:00Z")))
    )
    requests.insertOne(
      new Document("_id", UUID.randomUUID().toString)
        .append("state", "Complete")
        .append("expiresAt", Date.from(Instant.parse("2026-09-23T12:00:00Z")))
    )
    val retainedOnly = source
      .activeSubjectTokens(spark)
      .flatMap(frame => IO.blocking(frame.select("subjectToken").collect().map(_.getString(0)).toSet))
      .unsafeRunSync()
    assertEquals(retainedOnly, Set(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, retainedSubject)))

    val pendingSubject = UUID.randomUUID().toString
    requests.insertOne(new Document("_id", pendingSubject).append("state", "Pending"))
    val processingSubject = UUID.randomUUID().toString
    requests.insertOne(new Document("_id", processingSubject).append("state", "Processing"))

    val tokens = source
      .activeSubjectTokens(spark)
      .flatMap { frame =>
        IO.blocking(frame.select("subjectToken").collect().map(_.getString(0)).toSet)
      }
      .unsafeRunSync()

    assertEquals(
      tokens,
      Set(
        AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, retainedSubject),
        AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, pendingSubject),
        AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, processingSubject)
      )
    )
  }

  test("Mongo marker source fails closed for an array-valued completed expiry") {
    val database = mongoClient.getDatabase(s"array_expiry_${UUID.randomUUID()}")
    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", UUID.randomUUID().toString)
          .append("state", "Complete")
          .append("expiresAt", List(Date.from(Instant.parse("2026-09-23T12:00:00Z"))).asJava)
      )
    val source = new MongoActiveDeletionMarkerSource[IO](
      reactiveMongoClient.getDatabase(database.getName),
      pseudonymizer
    )

    val error = intercept[AnalyticsError](source.activeSubjectTokens(spark).unsafeRunSync())
    assertEquals(error, AnalyticsError.MalformedMarker)
  }

  test("missing or malformed Mongo marker data fails before any Delta mutation") {
    val missingCollectionDatabase = mongoClient.getDatabase(s"missing_${UUID.randomUUID()}")
    val missingPaths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-missing-markers").toUri.toString)
    val missingBatch = AnalyticsBatchTestSupport.newBatch(
      missingPaths,
      pseudonymizer,
      new MongoActiveDeletionMarkerSource[IO](
        reactiveMongoClient.getDatabase(missingCollectionDatabase.getName),
        pseudonymizer
      )
    )
    val missingManifest = manifest("missing-markers")
    val noReadSource = new BoundedOperationalEventSource[IO] {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(new AssertionError("Kafka must not be read before marker verification"))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.raiseError(new AssertionError("offsets must not be verified before marker verification"))
    }

    val missingError = intercept[AnalyticsError](missingBatch.run(spark, noReadSource, missingManifest).unsafeRunSync())
    assertEquals(missingError, AnalyticsError.MissingMarkerCollection)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, missingPaths.manifests), "assertion failed")
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, missingPaths.bronze), "assertion failed")

    val malformedDatabase = mongoClient.getDatabase(s"malformed_${UUID.randomUUID()}")
    malformedDatabase
      .getCollection("analytics_erasure_requests")
      .insertOne(new Document("_id", "not-a-uuid").append("state", "Pending"))
    val malformedPaths =
      AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-malformed-markers").toUri.toString)
    val malformedBatch = AnalyticsBatchTestSupport.newBatch(
      malformedPaths,
      pseudonymizer,
      new MongoActiveDeletionMarkerSource[IO](reactiveMongoClient.getDatabase(malformedDatabase.getName), pseudonymizer)
    )

    val malformedError = intercept[AnalyticsError](
      malformedBatch.run(spark, noReadSource, manifest("malformed-markers")).unsafeRunSync()
    )
    assertEquals(malformedError, AnalyticsError.MalformedMarker)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, malformedPaths.manifests), "assertion failed")
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, malformedPaths.bronze), "assertion failed")
  }

  test("pending marker overflow fails before any Delta mutation") {
    val database = mongoClient.getDatabase(s"overflow_${UUID.randomUUID()}")
    database
      .getCollection("analytics_erasure_requests")
      .insertMany(
        List(
          new Document("_id", UUID.randomUUID().toString).append("state", "Pending"),
          new Document("_id", UUID.randomUUID().toString).append("state", "Pending")
        ).asJava
      )
    val paths = AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-marker-overflow").toUri.toString)
    val markers = new MongoActiveDeletionMarkerSource[IO](
      reactiveMongoClient.getDatabase(database.getName),
      pseudonymizer,
      maximumPendingMarkers = 1
    )
    val batch = AnalyticsBatchTestSupport.newBatch(paths, pseudonymizer, markers)
    val overflowManifest = manifest("overflow-markers")
    val noReadSource = new BoundedOperationalEventSource[IO] {
      override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
        IO.raiseError(new AssertionError("Kafka must not be read after marker overflow"))
      override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
        IO.raiseError(new AssertionError("offsets must not be verified after marker overflow"))
    }

    val overflowError = intercept[AnalyticsError](batch.run(spark, noReadSource, overflowManifest).unsafeRunSync())
    assertEquals(overflowError, AnalyticsError.MarkerLimitExceeded(1))
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests), "assertion failed")
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze), "assertion failed")
  }

  test("unavailable Mongo marker storage fails before any Delta mutation") {
    val unavailableClient = MongoClients.create(
      "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1000&connectTimeoutMS=500"
    )
    val unavailableReactiveClient = ReactiveMongoClients.create(
      "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1000&connectTimeoutMS=500"
    )
    try {
      val paths =
        AnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-unavailable-markers").toUri.toString)
      val batch = AnalyticsBatchTestSupport.newBatch(
        paths,
        pseudonymizer,
        new MongoActiveDeletionMarkerSource[IO](
          unavailableReactiveClient.getDatabase(s"unavailable_${UUID.randomUUID()}"),
          pseudonymizer
        )
      )
      val unavailableManifest = manifest("unavailable-markers")
      val noReadSource = new BoundedOperationalEventSource[IO] {
        override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
          IO.raiseError(new AssertionError("Kafka must not be read before marker storage is available"))
        override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
          IO.raiseError(new AssertionError("offsets must not be verified before marker storage is available"))
      }

      val storageError = intercept[AnalyticsError](
        batch.run(spark, noReadSource, unavailableManifest).unsafeRunSync()
      )
      assert(storageError.isInstanceOf[AnalyticsError.MarkerStorageFailure], "assertion failed")
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.manifests), "assertion failed")
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.bronze), "assertion failed")
      assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, paths.silver), "assertion failed")
    } finally {
      unavailableClient.close()
      unavailableReactiveClient.close()
    }
  }

  test("closed Mongo marker client returns a typed storage error") {
    val closedClient = ReactiveMongoClients.create("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
    val database = closedClient.getDatabase("closed_markers")
    closedClient.close()

    val source = new MongoActiveDeletionMarkerSource[IO](database, pseudonymizer)
    val failure = intercept[AnalyticsError.MarkerStorageFailure](source.activeSubjectTokens(spark).unsafeRunSync())
    assert(failure.getCause.isInstanceOf[IllegalStateException], "assertion failed")
  }
}
