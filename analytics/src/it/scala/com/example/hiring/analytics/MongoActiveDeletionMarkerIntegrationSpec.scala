package com.example.hiring.analytics
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.mongodb.client.{MongoClient, MongoClients}
import mongo4cats.client.{MongoClient as CatsMongoClient}
import org.apache.spark.sql.SparkSession
import org.bson.Document

import java.nio.file.Files
import java.time.Instant
import java.util.Date
import java.util.UUID
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class MongoActiveDeletionMarkerIntegrationSpec extends AnalyticsMongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val pseudonymizer =
    AnalyticsTestSubjectPseudonymizer.fromSecret("analytics-integration-secret".padTo(32, 'x').getBytes("UTF-8"))
  private val clients = ResourceSuiteLocalFixture(
    "analytics-marker-clients",
    Resource.eval(IO.delay(endpointUri)).flatMap { uri =>
      (
        Resource.make(IO.blocking(MongoClients.create(uri)))(client => IO.blocking(client.close())),
        CatsMongoClient.fromConnectionString[IO](uri)
      ).tupled
    }
  )
  private val sparkResource = ResourceSuiteLocalFixture(
    "analytics-marker-spark",
    Resource.make(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .master("local[2]")
          .appName("HiringDeletionMarkers")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      )
    )(spark => IO.blocking(spark.stop()))
  )
  override def munitFixtures: List[munit.AnyFixture[?]] = super.munitFixtures ++ List(clients, sparkResource)
  private def mongoClient: MongoClient = clients()._1
  private def mongo4catsClient: CatsMongoClient[IO] = clients()._2
  private def spark: SparkSession = sparkResource()

  private def mongo4catsDatabase(name: String): mongo4cats.database.MongoDatabase[IO] =
    mongo4catsClient.getDatabase(name).unsafeRunSync()

  private def manifest(runId: String): AnalyticsRunManifest =
    TestPartitionOffsetRange
      .manifestOf(runId, Vector(TestPartitionOffsetRange.unsafe("topic", 0, 0L, 1L)))
      .toEither
      .fold(errors => fail(errors.toString), identity)

  test("Mongo marker source keeps unexpired completed markers active and ignores expired ones") {
    val database = mongoClient.getDatabase(testDatabaseName)
    val requests = database.getCollection("analytics_erasure_requests")
    val source = new MongoActiveDeletionMarkerSource[IO](
      mongo4catsDatabase(database.getName),
      pseudonymizer,
      streams = AnalyticsTestOperationalConfig.streams
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
    val retainedOnly = source.activeSubjectTokens.map(_.map(_.value).toSet).unsafeRunSync()
    assertEquals(retainedOnly, Set(AnalyticsTestSubjectPseudonymizer.tokenValue(pseudonymizer, retainedSubject)))

    val pendingSubject = UUID.randomUUID().toString
    requests.insertOne(new Document("_id", pendingSubject).append("state", "Pending"))
    val processingSubject = UUID.randomUUID().toString
    requests.insertOne(new Document("_id", processingSubject).append("state", "Processing"))

    val tokens = source.activeSubjectTokens.map(_.map(_.value).toSet).unsafeRunSync()

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
    val database = mongoClient.getDatabase(testDatabaseName)
    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", UUID.randomUUID().toString)
          .append("state", "Complete")
          .append("expiresAt", List(Date.from(Instant.parse("2026-09-23T12:00:00Z"))).asJava)
      )
    val source = new MongoActiveDeletionMarkerSource[IO](
      mongo4catsDatabase(database.getName),
      pseudonymizer,
      streams = AnalyticsTestOperationalConfig.streams
    )

    val error = intercept[AnalyticsError](source.activeSubjectTokens.unsafeRunSync())
    assertEquals(error, AnalyticsError.MalformedMarker)
  }

  test("missing or malformed Mongo marker data fails before any Delta mutation") {
    val missingCollectionDatabase = mongoClient.getDatabase(testDatabaseName)
    val missingPaths =
      IntegrationAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-missing-markers").toUri.toString)
    val missingBatch = AnalyticsBatchTestSupport.newBatch(
      missingPaths,
      pseudonymizer,
      new MongoActiveDeletionMarkerSource[IO](
        mongo4catsDatabase(missingCollectionDatabase.getName),
        pseudonymizer,
        streams = AnalyticsTestOperationalConfig.streams
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

    val malformedDatabase = mongoClient.getDatabase(testDatabaseName)
    malformedDatabase
      .getCollection("analytics_erasure_requests")
      .insertOne(new Document("_id", "not-a-uuid").append("state", "Pending"))
    val malformedPaths =
      IntegrationAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-malformed-markers").toUri.toString)
    val malformedBatch = AnalyticsBatchTestSupport.newBatch(
      malformedPaths,
      pseudonymizer,
      new MongoActiveDeletionMarkerSource[IO](
        mongo4catsDatabase(malformedDatabase.getName),
        pseudonymizer,
        streams = AnalyticsTestOperationalConfig.streams
      )
    )

    val malformedError = intercept[AnalyticsError](
      malformedBatch.run(spark, noReadSource, manifest("malformed-markers")).unsafeRunSync()
    )
    assertEquals(malformedError, AnalyticsError.MalformedMarker)
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, malformedPaths.manifests), "assertion failed")
    assert(!io.delta.tables.DeltaTable.isDeltaTable(spark, malformedPaths.bronze), "assertion failed")
  }

  Vector[(String, Document => Document)](
    "unknown" -> (_.append("state", "Unexpected")),
    "missing" -> identity[Document],
    "null" -> (_.append("state", null)),
    "numeric" -> (_.append("state", Int.box(1))),
    "array containing Complete" -> (_.append("state", List("Complete").asJava))
  ).foreach { case (label, malformedState) =>
    test(s"$label marker state fails before registry, report reservation, or Delta writes") {
      val database = mongoClient.getDatabase(testDatabaseName)
      database
        .getCollection("analytics_erasure_requests")
        .insertOne(
          malformedState(new Document("_id", UUID.randomUUID().toString))
            .append("expiresAt", Date.from(Instant.EPOCH))
        )
      val root = Files.createTempDirectory("analytics-invalid-marker-state")
      val paths = IntegrationAnalyticsLakehousePaths.unsafe(root.toUri.toString)
      val rejectingPublisher = new AnalyticsReportPublisher[IO] {
        override def publicationReceipt(
            reservation: AnalyticsReportReservation
        ): IO[AnalyticsReportPublicationReceipt] = IO.pure(AnalyticsReportPublicationReceipt.Absent)

        override def reservePinned(
            runId: RunId,
            rangeFingerprint: RangeFingerprint,
            now: Instant
        ): IO[AnalyticsReportReservation] =
          reserve(runId, rangeFingerprint, now)

        override def reserve(
            runId: RunId,
            fingerprint: RangeFingerprint,
            now: Instant
        ): IO[AnalyticsReportReservation] =
          IO.raiseError(new AssertionError("invalid markers must fail before report reservation"))
        override def publish(
            reservation: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant
        ): IO[Unit] =
          IO.raiseError(new AssertionError("invalid markers must fail before publication"))
        override def publishErasure(
            reservation: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant,
            claim: ErasureClaim,
            completedAt: Instant
        ): IO[Unit] =
          IO.raiseError(new AssertionError("invalid markers must fail before erasure publication"))
      }
      val batch = AnalyticsBatchTestSupport.newBatch(
        paths,
        pseudonymizer,
        new MongoActiveDeletionMarkerSource[IO](
          mongo4catsDatabase(database.getName),
          pseudonymizer,
          AnalyticsTestOperationalConfig.streams
        ),
        reportPublisher = rejectingPublisher
      )
      val noReadSource = new BoundedOperationalEventSource[IO] {
        override def read(spark: SparkSession, manifest: AnalyticsRunManifest): IO[org.apache.spark.sql.DataFrame] =
          IO.raiseError(new AssertionError("invalid markers must fail before reading Kafka"))
        override def verifyOffsets(frame: org.apache.spark.sql.DataFrame, manifest: AnalyticsRunManifest): IO[Unit] =
          IO.raiseError(new AssertionError("invalid markers must fail before checking offsets"))
      }
      try {
        val error = intercept[AnalyticsError](batch.run(spark, noReadSource, manifest("invalid-state")).unsafeRunSync())
        assertEquals(error, AnalyticsError.MalformedMarker)
        val files = Files.list(root)
        try assert(!files.findAny().isPresent, "marker rejection must precede every lakehouse mutation")
        finally files.close()
      } finally {
        database.drop()
        val _ = Files.deleteIfExists(root)
      }
    }
  }

  test("pending marker overflow fails before any Delta mutation") {
    val database = mongoClient.getDatabase(testDatabaseName)
    database
      .getCollection("analytics_erasure_requests")
      .insertMany(
        List(
          new Document("_id", UUID.randomUUID().toString).append("state", "Pending"),
          new Document("_id", UUID.randomUUID().toString).append("state", "Pending")
        ).asJava
      )
    val paths =
      IntegrationAnalyticsLakehousePaths.unsafe(Files.createTempDirectory("analytics-marker-overflow").toUri.toString)
    val markers = new MongoActiveDeletionMarkerSource[IO](
      mongo4catsDatabase(database.getName),
      pseudonymizer,
      maximumPendingMarkers = 1,
      streams = AnalyticsTestOperationalConfig.streams
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
    val (unavailableCatsClient, releaseUnavailableCatsClient) = CatsMongoClient
      .fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=1000&connectTimeoutMS=500")
      .allocated
      .unsafeRunSync()
    try {
      val paths =
        IntegrationAnalyticsLakehousePaths.unsafe(
          Files.createTempDirectory("analytics-unavailable-markers").toUri.toString
        )
      val batch = AnalyticsBatchTestSupport.newBatch(
        paths,
        pseudonymizer,
        new MongoActiveDeletionMarkerSource[IO](
          unavailableCatsClient.getDatabase(s"unavailable_${UUID.randomUUID()}").unsafeRunSync(),
          pseudonymizer,
          streams = AnalyticsTestOperationalConfig.streams
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
      releaseUnavailableCatsClient.unsafeRunSync()
    }
  }

  test("closed Mongo marker client returns a typed storage error") {
    val (closedClient, releaseClosedClient) = CatsMongoClient
      .fromConnectionString[IO]("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
      .allocated
      .unsafeRunSync()
    val database = closedClient.getDatabase("closed_markers").unsafeRunSync()
    releaseClosedClient.unsafeRunSync()

    val source =
      new MongoActiveDeletionMarkerSource[IO](
        database,
        pseudonymizer,
        streams = AnalyticsTestOperationalConfig.streams
      )
    val failure = intercept[AnalyticsError.MarkerStorageFailure](source.activeSubjectTokens.unsafeRunSync())
    assert(failure.getCause.isInstanceOf[IllegalStateException], "assertion failed")
  }
}
