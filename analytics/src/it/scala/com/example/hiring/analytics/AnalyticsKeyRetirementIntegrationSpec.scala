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

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import cats.effect.{Deferred, IO, Resource}
import cats.effect.unsafe.implicits.global
import com.mongodb.client.{MongoClient, MongoClients, MongoDatabase}
import com.mongodb.reactivestreams.client.{
  MongoClient as ReactiveMongoClient,
  MongoClients as ReactiveMongoClients,
  MongoDatabase as ReactiveMongoDatabase
}
import mongo4cats.client.MongoClient as CatsMongoClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.bson.Document

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.compiletime.uninitialized
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Opt-in read-only audit proof using local Delta files and a disposable replica-set database. */
final class AnalyticsKeyRetirementIntegrationSpec extends munit.CatsEffectSuite {
  // Structural retention/restart proof, not a latency SLO; physical Spark cleanup can contend with other local builds.
  override val munitIOTimeout: FiniteDuration = 15.minutes
  private val enabled = sys.env.get("ANALYTICS_KEY_RETIREMENT_MONGO_URI").exists(_.nonEmpty)
  private var mongo: MongoClient = uninitialized
  private var peerMongo: MongoClient = uninitialized
  private var reactiveMongo: ReactiveMongoClient = uninitialized
  private var catsMongo: CatsMongoClient[IO] = uninitialized
  private var catsPeerMongo: CatsMongoClient[IO] = uninitialized
  private var releaseCatsMongo: IO[Unit] = IO.unit
  private var releaseCatsPeerMongo: IO[Unit] = IO.unit
  private var database: MongoDatabase = uninitialized
  private var reactiveDatabase: ReactiveMongoDatabase = uninitialized
  private var spark: SparkSession = uninitialized
  private var lakehouseRoot: Path = uninitialized
  private val now = Instant.parse("2026-10-26T00:00:00Z")
  private val oldKeyId = "retiring-key"

  override def beforeAll(): Unit = if (enabled) {
    mongo = MongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    peerMongo = MongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    reactiveMongo = ReactiveMongoClients.create(sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
    val (catsClient, releaseCatsClient) = CatsMongoClient
      .fromConnectionString[IO](sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
      .allocated
      .unsafeRunSync()
    catsMongo = catsClient
    releaseCatsMongo = releaseCatsClient
    val (catsPeerClient, releaseCatsPeerClient) = CatsMongoClient
      .fromConnectionString[IO](sys.env("ANALYTICS_KEY_RETIREMENT_MONGO_URI"))
      .allocated
      .unsafeRunSync()
    catsPeerMongo = catsPeerClient
    releaseCatsPeerMongo = releaseCatsPeerClient
    database = mongo.getDatabase(s"analytics_key_retirement_${java.util.UUID.randomUUID().toString.replace('-', '_')}")
    reactiveDatabase = reactiveMongo.getDatabase(database.getName)
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("AnalyticsKeyRetirementIntegrationSpec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    lakehouseRoot = Files.createTempDirectory("analytics-key-retirement-")
    val required = Vector(
      "analytics_report_snapshots",
      "analytics_report_runs",
      "analytics_report_control",
      "analytics_erasure_requests",
      "analytics_erasure_completions",
      "analytics_erasure_delta_files",
      "event_outbox",
      "hiring_migration_ledger",
      "outbox_subject_fences"
    )
    required.foreach(database.createCollection)
    database
      .getCollection("hiring_migration_ledger")
      .insertOne(
        new Document("_id", "003_event_outbox_subject_references").append("state", "Complete")
      )
    seedRegistry(IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.toUri.toString))
  }

  override def afterAll(): Unit = if (enabled) {
    Try(database.drop())
    Try(mongo.close())
    Try(peerMongo.close())
    Try(reactiveMongo.close())
    Try(releaseCatsMongo.unsafeRunSync())
    Try(releaseCatsPeerMongo.unsafeRunSync())
    Try(spark.stop())
    if (lakehouseRoot != null) deleteTree(lakehouseRoot)
  }

  if (enabled) test("audit passes empty verified surfaces and blocks a live marker or old-key Delta row") {
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.toUri.toString)
    val passed = audit(paths)
    assert(passed.isRight, passed.swap.toOption.toString)

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-1").append("state", "Pending")
      )
    val activeMarker = audit(paths)
    assert(activeMarker.swap.toOption.get.exists(_.contains("active or unexpired erasure marker")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    database
      .getCollection("analytics_report_snapshots")
      .insertOne(
        new Document("_id", "snapshot-1")
          .append("report", new Document("subjectToken", s"${oldKeyId}_report-reference"))
      )
    val oldReportReference = audit(paths)
    assert(oldReportReference.swap.toOption.get.exists(_.contains("analytics_report_snapshots")), "assertion failed")
    database.getCollection("analytics_report_snapshots").deleteMany(new Document())

    val tokenSchema = StructType(Seq(StructField("subjectToken", StringType, nullable = false)))
    spark
      .createDataFrame(List(Row(s"${oldKeyId}_unsafe-reference")).asJava, tokenSchema)
      .write
      .format("delta")
      .mode("overwrite")
      .save(paths.silver)
    val oldToken = audit(paths)
    assert(
      oldToken.swap.toOption.get.contains("a current or retained Delta data file references the retiring key"),
      "assertion failed"
    )
  }

  if (enabled) test("Mongo lakehouse mutex excludes a second independent client until owner release") {
    val root = s"s3a://analytics-test/${java.util.UUID.randomUUID()}"
    val firstLock =
      new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
        catsMongo.getDatabase(database.getName).unsafeRunSync(),
        AnalyticsTestOperationalConfig.streams
      )
    val secondLock =
      new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
        catsPeerMongo.getDatabase(database.getName).unsafeRunSync(),
        AnalyticsTestOperationalConfig.streams
      )
    val result = (for {
      firstEntered <- Deferred[IO, Unit]
      releaseFirst <- Deferred[IO, Unit]
      secondEntered <- Deferred[IO, Unit]
      first <- firstLock.resource(root).use(_ => firstEntered.complete(()) *> releaseFirst.get).start
      _ <- firstEntered.get
      second <- secondLock.resource(root).use(_ => secondEntered.complete(())).start
      beforeRelease <- IO.sleep(1.second) *> secondEntered.tryGet
      _ <- releaseFirst.complete(())
      _ <- secondEntered.get.timeout(10.seconds)
      _ <- first.joinWithNever
      _ <- second.joinWithNever
    } yield beforeRelease).unsafeRunSync()
    assertEquals(result, None)
  }

  if (enabled) test("audit fails closed on malformed erasure state and permits unrelated outbox replay") {
    database
      .getCollection("event_outbox")
      .insertOne(
        new Document("_id", "event-1")
          .append("subjectIds", java.util.List.of("11111111-1111-4111-8111-111111111111"))
          .append("subjectRefsVersion", 1)
          .append("state", "Retryable")
      )
    val paths = IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("second").toUri.toString)
    seedRegistry(paths)
    val result = audit(paths)
    assert(result.isRight, result.swap.toOption.toString)
    assertEquals(
      result.toOption.get.operatorEvidence,
      "DIAGNOSTIC ONLY: operator-attested; not an authorization to retire a key"
    )

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-unknown-state").append("state", "Unexpected")
      )
    val unknownState = audit(paths).swap.toOption.get
    assert(unknownState.exists(_.contains("erasure request has an unknown state")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    database
      .getCollection("analytics_erasure_requests")
      .insertOne(
        new Document("_id", "subject-complete-without-expiry").append("state", "Complete")
      )
    val missingExpiry = audit(paths).swap.toOption.get
    assert(missingExpiry.exists(_.contains("missing or invalid retention expiry")), "assertion failed")
    database.getCollection("analytics_erasure_requests").deleteMany(new Document())

    val missingRegistry = IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("third").toUri.toString)
    val registryReasons = audit(missingRegistry).swap.toOption.get
    assert(registryReasons.exists(_.contains("continuity registry is missing")), "assertion failed")
    database.getCollection("event_outbox").deleteMany(new Document())
  }

  private def audit(paths: AnalyticsLakehousePaths) =
    AnalyticsKeyRetirement
      .audit(
        spark,
        paths,
        reactiveDatabase,
        oldKeyId,
        AnalyticsKeyRetirement.RetentionEvidence(
          kafka = AnalyticsKeyRetirement.KafkaRetentionEvidence(Some(10L), Some(10L), "test-kafka-barrier"),
          deltaData = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-delta-data"),
          deltaLogs = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-delta-logs"),
          reports = AnalyticsKeyRetirement.RetentionHorizon(Some(now.minusSeconds(1)), "test-reports")
        ),
        AnalyticsKeyRetirement.WriterInventory(
          observedAt = Some(now.minusSeconds(1)),
          coverageReference = "test-operator-inventory",
          managed = Vector(
            AnalyticsKeyRetirement
              .WriterRecord("analytics-batch", AnalyticsKeyRetirement.WriterDisposition.Stopped, "test-stop-record")
          ),
          unmanaged = Vector(
            AnalyticsKeyRetirement.WriterRecord(
              "unmanaged-writer",
              AnalyticsKeyRetirement.WriterDisposition.AccessRevoked,
              "test-access-revocation"
            )
          )
        ),
        now,
        new com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock[IO](
          catsMongo.getDatabase(database.getName).unsafeRunSync(),
          AnalyticsTestOperationalConfig.streams
        ),
        AnalyticsTestOperationalConfig.streams,
        com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
          .forTests[IO](scala.concurrent.ExecutionContext.parasitic)
      )
      .unsafeRunSync()

  if (enabled) test("HMAC retirement authorization preserves every field through native BSON Date") {
    val paths =
      IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("authorization-date-proof").toUri.toString)
    val measured = Instant.parse("2026-10-01T09:52:06.123456789Z")
    val facts = "synthetic native BSON Date retirement authorization roundtrip"
    val verifiedAt = HmacKeyRetirementCoordinator.persistedAuthorizationTime(measured)
    val lakehouseId = MongoAnalyticsLakehouseLock.lockId(paths.root).toOption.get
    val record = HmacKeyRetirementAuthorization(
      lakehouseId,
      "native-time-proof",
      "A" * 43,
      facts,
      HmacKeyRetirementAuthorization.digest(facts),
      verifiedAt
    )
    val db = catsMongo.getDatabase(database.getName).unsafeRunSync()
    val store = new MongoHmacKeyRetirementAuthorizationStore[IO](db, AnalyticsTestOperationalConfig.streams)
    val lock = new MongoAnalyticsLakehouseLock[IO](db, AnalyticsTestOperationalConfig.streams)
    val loaded = lock
      .resource(paths.root)
      .use { _ =>
        store.insert(paths.root, record) *> store.list(paths.root)
      }
      .unsafeRunSync()
    assertEquals(verifiedAt, Instant.parse("2026-10-01T09:52:06.123Z"))
    assertEquals(loaded, Vector(record))
    val stored = database
      .getCollection("analytics_hmac_key_retirements")
      .find(new Document("_id", s"$lakehouseId:native-time-proof"))
      .first()
    assertEquals(stored.getDate("authorizedAt").toInstant, record.authorizedAt)
    assertEquals(stored.get("authorizedAt").getClass, classOf[java.util.Date])
  }

  if (enabled) test("isolated calendar cleanup persists retirement and rejects old-primary restart") {
    // There are two synthetic facts and no Kafka records. Calendar and writer inventory are simulated;
    // captured-file deletion, checkpoint/spill rejection, Mongo authorization and both Spark restarts are actual.
    for {
      db <- catsMongo.getDatabase(database.getName)
      at <- IO.realTimeInstant
      paths = IntegrationAnalyticsLakehousePaths.unsafe(lakehouseRoot.resolve("restart-proof").toUri.toString)
      old = AnalyticsTestSubjectPseudonymizer.fromKeyRing(
        "restart-old",
        Array.fill[Byte](32)(41),
        Vector("restart-new" -> Array.fill[Byte](32)(42))
      )
      fresh = AnalyticsTestSubjectPseudonymizer.fromKeyRing("restart-new", Array.fill[Byte](32)(42), Vector.empty)
      shifted = at.plusSeconds(32L * 86400L)
      oldToken = AnalyticsTestSubjectPseudonymizer.tokenValue(old, java.util.UUID.randomUUID().toString)
      newToken = AnalyticsTestSubjectPseudonymizer.tokenValue(fresh, java.util.UUID.randomUUID().toString)
      store = new MongoHmacKeyRetirementAuthorizationStore[IO](db, AnalyticsTestOperationalConfig.streams)
      lock = new MongoAnalyticsLakehouseLock[IO](db, AnalyticsTestOperationalConfig.streams)
      schema = AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry)
      _ <- SparkBlockingExecution.resource[IO].use { execution =>
        Resource.make(IO.pure(spark))(session => execution.blocking(session.stop())).use { session =>
          def continuity(keys: SubjectPseudonymizer) = lock.resource(paths.root).use { _ =>
            new AnalyticsKeyContinuityStage[IO](
              paths,
              keys,
              execution,
              root => store.list(root),
              Some(IO.pure(shifted))
            )
              .validateHmacConfiguration(session)
          }
          val short = AnalyticsTestOperationalConfig.operational.copy(retention =
            AnalyticsTestOperationalConfig.operational.retention
              .copy(deltaVacuumSafety = 1.second, deltaLogRetention = 1.second)
          )
          val maintenance = new DeltaAnalyticsErasureLakehouse[IO](
            session,
            paths,
            old,
            lock,
            store,
            short,
            execution,
            org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO],
            Some(IO.pure(shifted)),
            new DeltaLogFactory {
              override def apply(current: SparkSession, path: String) =
                org.apache.spark.sql.delta.HiringAnalyticsRetentionClock.forTable(current, path, 32L * 86400L * 1000L)
            }
          )
          val capturedLog = Path.of(java.net.URI.create(paths.silver)).resolve("_delta_log/00000000000000000000.json")
          val checkpoints = lakehouseRoot.resolve("restart-checkpoint")
          val spill = lakehouseRoot.resolve("restart-spill")
          val inventory =
            AnalyticsStorageInventory(paths, Vector(checkpoints.toUri.toString), Vector(spill.toUri.toString))
          val retention = AnalyticsKeyRetirement.RetentionEvidence(
            AnalyticsKeyRetirement.KafkaRetentionEvidence(Some(0L), Some(0L), "isolated fixture has no Kafka records"),
            AnalyticsKeyRetirement.RetentionHorizon(Some(shifted.minusSeconds(1)), "actual one-second data cleanup"),
            AnalyticsKeyRetirement
              .RetentionHorizon(Some(shifted.minusSeconds(1)), "simulated calendar; actual Delta cleanup"),
            AnalyticsKeyRetirement.RetentionHorizon(Some(shifted.minusSeconds(1)), "no fixture report snapshots")
          )
          val writers = AnalyticsKeyRetirement.WriterInventory(
            Some(shifted),
            "isolated test exclusively owns this root",
            Vector(
              AnalyticsKeyRetirement.WriterRecord(
                "owned-test-writer",
                AnalyticsKeyRetirement.WriterDisposition.Stopped,
                "sequential lock; simulated external writer inventory"
              )
            ),
            Vector.empty
          )
          def audited = AnalyticsKeyRetirement.audit[IO](
            session,
            paths,
            reactiveDatabase,
            "restart-old",
            retention,
            writers,
            shifted,
            lock,
            AnalyticsTestOperationalConfig.streams,
            execution,
            Some(inventory)
          )
          for {
            _ <- execution.attachSparkContext(session.sparkContext)
            _ <- execution(session.conf.set("spark.sql.shuffle.partitions", "2"))
            _ <- continuity(old)
            _ <- execution {
              session
                .createDataFrame(List(retirementFact(oldToken, at, at.plusSeconds(30L * 86400L))).asJava, schema)
                .write
                .format("delta")
                .option("delta.dataSkippingNumIndexedCols", "0")
                .mode("errorifexists")
                .save(paths.silver)
            }
            capturedData <- execution(
              session.read
                .format("delta")
                .load(paths.silver)
                .inputFiles
                .toVector
                .map(value => Path.of(java.net.URI.create(value)))
            )
            _ <- IO.blocking(
              assert(capturedData.nonEmpty && capturedData.forall(Files.exists(_)) && Files.exists(capturedLog))
            )
            missing <- continuity(fresh).attempt
            _ <- IO(assert(missing.isLeft, "missing durable authorization must reject omission"))
            _ <- lock
              .resource(paths.root)
              .use(_ =>
                maintenance.configureRawTables *> maintenance.expireStored(shifted) *>
                  IO.sleep(1500.millis) *> maintenance.reclaimExpiredFiles
              )
            _ <- IO.blocking {
              assert(
                capturedData.forall(path => !Files.exists(path)),
                "actual VACUUM must remove captured subject data"
              )
              assert(!Files.exists(capturedLog), "actual Delta log cleanup must remove captured initial transaction")
              Files.createDirectories(checkpoints)
              Files.createDirectories(spill)
              Files.writeString(
                checkpoints.resolve("metadata"),
                "{\"id\":\"" + java.util.UUID.randomUUID().toString + "\"}"
              )
            }
            unsafeCheckpoint = checkpoints.resolve("synthetic-subject-fixture")
            _ <- IO.blocking(
              Files
                .writeString(unsafeCheckpoint, "{\"subjectTokens\":[\"" + oldToken + "\"],\"rawValue\":\"synthetic\"}")
            )
            checkpointRejected <- audited
            _ <- IO(assert(checkpointRejected.isLeft, "retained checkpoint subject fields must block retirement"))
            _ <- IO.blocking(Files.delete(unsafeCheckpoint))
            unsafeSpill = spill.resolve("synthetic-spill-fixture")
            _ <- IO.blocking(Files.writeString(unsafeSpill, "synthetic"))
            spillRejected <- audited
            _ <- IO(assert(spillRejected.isLeft, "retained Spark spill must block retirement"))
            _ <- IO.blocking(Files.delete(unsafeSpill))
            _ <- lock.resource(paths.root).use { _ =>
              for {
                result <- AnalyticsKeyRetirement.auditUnderLock[IO](
                  session,
                  paths,
                  reactiveDatabase,
                  "restart-old",
                  retention,
                  writers,
                  shifted,
                  AnalyticsTestOperationalConfig.streams,
                  execution,
                  Some(inventory)
                )
                _ <- IO(assert(result.isRight, result.swap.toOption.toString))
                lakehouseId <- IO.fromEither(MongoAnalyticsLakehouseLock.lockId(paths.root))
                facts = "isolated integration proof; physical Delta cleanup; simulated calendar and writer inventory; no Kafka source"
                record = HmacKeyRetirementAuthorization(
                  lakehouseId,
                  "restart-old",
                  old.keyVerifiers.toMap.apply("restart-old"),
                  facts,
                  HmacKeyRetirementAuthorization.digest(facts),
                  HmacKeyRetirementCoordinator.persistedAuthorizationTime(shifted)
                )
                _ <- store.insert(paths.root, record)
                loaded <- store.list(paths.root)
                _ <- IO(assertEquals(loaded, Vector(record)))
              } yield ()
            }
          } yield ()
        }
      }
      // Acquire a new context and a new attached driver to prove durable restart, with resource-owned shutdown.
      _ <- SparkBlockingExecution.resource[IO].use { execution =>
        Resource
          .make(execution {
            org.apache.spark.sql.delta.DeltaLog.clearCache()
            SparkSession
              .builder()
              .master("local[1]")
              .appName("HiringAnalyticsRetiredKeyRestart")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate()
          })(session => execution.blocking(session.stop()))
          .use { session =>
            def continuity(keys: SubjectPseudonymizer) = lock.resource(paths.root).use { _ =>
              new AnalyticsKeyContinuityStage[IO](
                paths,
                keys,
                execution,
                root => store.list(root),
                Some(IO.pure(shifted))
              )
                .validateHmacConfiguration(session)
            }
            for {
              _ <- execution.attachSparkContext(session.sparkContext)
              _ <- IO { spark = session }
              retired <- continuity(old).attempt
              _ <- IO(assert(retired.isLeft, "retired primary cannot restart"))
              _ <- continuity(fresh)
              _ <- execution(
                session
                  .createDataFrame(List(retirementFact(newToken, shifted, shifted.plusSeconds(86400))).asJava, schema)
                  .write
                  .format("delta")
                  .mode("append")
                  .save(paths.silver)
              )
              _ <- continuity(fresh)
              _ <- execution {
                assertEquals(
                  session.read.format("delta").load(paths.silver).select("subjectToken").head().getString(0),
                  newToken
                )
                val verifierRows = session.read
                  .format("delta")
                  .load(paths.hmacKeyRegistry)
                  .select("keyId")
                  .collect()
                  .map(_.getString(0))
                  .toSet
                assertEquals(verifierRows, Set("restart-old", "restart-new"))
              }
            } yield ()
          }
      }
    } yield ()
  }

  private def retirementFact(token: String, observed: Instant, until: Instant): Row = Row(
    java.util.UUID.randomUUID().toString,
    "JOB_CREATED",
    java.sql.Timestamp.from(observed),
    "Job",
    java.util.UUID.randomUUID().toString,
    null,
    java.util.UUID.randomUUID().toString,
    null,
    Vector("Scala"),
    token,
    Vector(token),
    "a" * 64,
    java.sql.Timestamp.from(observed),
    java.sql.Timestamp.from(until)
  )

  private def seedRegistry(paths: AnalyticsLakehousePaths): Unit = {
    val schema = StructType(
      Seq(
        StructField("keyId", StringType, nullable = false),
        StructField("verifier", StringType, nullable = false)
      )
    )
    spark
      .createDataFrame(List(Row(oldKeyId, "A" * 43)).asJava, schema)
      .write
      .format("delta")
      .mode("errorifexists")
      .save(paths.hmacKeyRegistry)
  }

  private def deleteTree(root: Path): Unit = {
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()
  }
}
