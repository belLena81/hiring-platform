package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import com.example.hiring.analytics.{AnalyticsTestSubjectPseudonymizer, TestAnalyticsLakehousePaths}
import com.example.hiring.analytics.adapter.mongo.MongoAnalyticsLakehouseLock
import com.example.hiring.analytics.domain.{
  AnalyticsLakehouseIdentity,
  AnalyticsRunManifest,
  PartitionOffsetRange,
  RangeFingerprint,
  StreamingBatchId,
  StreamingBatchIdentity,
  StreamingLineage,
  StreamingPartitionEndOffset,
  StreamingPartitionSummary
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsManifestStatus
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import com.example.hiring.analytics.service.streaming.StreamingInputPreparation
import munit.CatsEffectSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.net.URI
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class HiringAnalyticsStorageLocationsSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private def checked[A](value: Either[?, A]): A = value.fold(error => fail(error.toString), identity)

  test("normalized local roots share their physical directory and unchanged mutex identity") {
    val canonical = "file:///var/lib/hiring+analytics/lakehouse%20with%25percent"
    val dotted = "file:///var/lib/hiring+analytics/other/../lakehouse%20with%25percent"
    assertEquals(SparkPhysicalLocation.resolve(dotted), SparkPhysicalLocation.resolve(canonical))
    assertEquals(MongoAnalyticsLakehouseLock.lockId(dotted), MongoAnalyticsLakehouseLock.lockId(canonical))
    assertEquals(SparkPhysicalLocation.resolve(canonical), "/var/lib/hiring+analytics/lakehouse with%percent")
    assertEquals(SparkPhysicalLocation.resolve("s3a://bucket/analytics"), "s3a://bucket/analytics")
    IO.unit
  }

  test("escaped ownership aliases fail before a mutex or physical storage operation") {
    val alias = "file:///var/lib/hiring%2Banalytics/lakehouse"
    assert(MongoAnalyticsLakehouseLock.lockId(alias).isLeft)
    intercept[AnalyticsError.InvalidConfiguration](SparkPhysicalLocation.resolve(alias))
    assert(AnalyticsLakehouseIdentity.from("file:///var/lib/hiring/%2e%2e/lakehouse").isLeft)
    IO.unit
  }

  private val runtime = for {
    root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring storage+100% boundary-")))(path =>
      IO.blocking {
        val entries = Files.walk(path)
        try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
        finally entries.close()
      }
    )
    driver <- SparkBlockingExecution.resource[IO]
    spark <- Resource.make(driver.blocking {
      SparkSession
        .builder()
        .master("local[2]")
        .appName("HiringAnalyticsStorageLocationsSpec")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    })(session => driver.blocking(session.stop()))
    _ <- Resource.eval(driver.attachSparkContext(spark.sparkContext))
  } yield (root, spark, new LakehouseOperation[IO](driver))

  test("canonical escaped root creates merges reads journals and captures actual Delta files") {
    runtime.use { case (root, spark, execution) =>
      val uri = new URI("file", null, root.resolve("lakehouse").toString, null, null).toASCIIString
      val paths = TestAnalyticsLakehousePaths.unsafe(uri)
      val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(31))
      val changed = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(32))
      val lookup = new KeyRetirementLookup[IO] {
        override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] = IO.pure(Vector.empty)
      }
      val continuity = new AnalyticsKeyContinuityStage[IO](paths, keys, execution, lookup, cats.effect.Clock[IO])
      val token = checked(keys.typedToken("084c58fe-787b-410b-b1bb-7193931f06e3")).value
      val at = Instant.parse("2026-10-01T12:00:00Z")
      val manifest = checked(
        AnalyticsRunManifest
          .validated(
            "hiring-storage-boundary",
            Vector(checked(PartitionOffsetRange.from("hiring.events", 0, 0L, 1L).toEither))
          )
          .toEither
      )
      val manifestStore = new DeltaManifestStore[IO](spark, paths, execution)
      val writer = new DeltaBatchWriter[IO](paths, execution)
      val reader = new DeltaBatchReader[IO](execution)
      val identity = StreamingBatchIdentity(
        checked(StreamingLineage.from("hiring-storage-boundary")),
        checked(StreamingBatchId.from(0L))
      )
      val preparation = StreamingInputPreparation(
        identity,
        at,
        None,
        checked(RangeFingerprint.from("a" * 64)),
        Vector(checked(StreamingPartitionEndOffset.from("hiring.events", 0, 1L).toEither)),
        Vector(checked(StreamingPartitionSummary.from("hiring.events", 0, 0L, 0L, 1L).toEither))
      )
      val journal = new DeltaStreamingBatchJournal[IO](spark, paths, execution)
      val erasure = new AnalyticsBatchErasureStage[IO](paths, execution, _ => IO.unit, 100)
      for {
        _ <- continuity.validateKeyMaterialContinuity(spark)
        frame <- execution {
          val values: Map[String, Any] = Map(
            "topic" -> "hiring.events",
            "partition" -> 0,
            "offset" -> 0L,
            "eventId" -> "hiring-storage-event",
            "eventType" -> "JOB_CREATED",
            "subjectToken" -> token,
            "subjectTokens" -> Vector(token),
            "ingestedAt" -> Timestamp.from(at),
            "expiresAt" -> Timestamp.from(at.plusSeconds(86400))
          )
          val row = Row.fromSeq(AnalyticsTableSchemas.bronze.map { case (name, _) => values.getOrElse(name, null) })
          spark.createDataFrame(Vector(row).asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.bronze))
        }
        _ <- writer.merge(
          frame,
          paths.bronze,
          "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
        )
        _ <- writer.merge(
          frame,
          paths.bronze,
          "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
        )
        stored <- reader.readOrEmpty(spark, paths.bronze, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.bronze))
        count <- execution(stored.count())
        _ <- IO(assertEquals(count, 1L))
        _ <- manifestStore.persist(manifest, AnalyticsManifestStatus.Published, at)
        _ <- manifestStore.persist(manifest, AnalyticsManifestStatus.Published, at)
        manifestCount <- execution(
          spark.read.format("delta").load(SparkPhysicalLocation.resolve(paths.manifests)).count()
        )
        _ <- IO(assertEquals(manifestCount, 1L))
        _ <- journal.prepare(preparation)
        _ <- journal.prepare(preparation)
        prepared <- journal.load(identity)
        _ <- IO(assert(prepared.nonEmpty))
        wrongMaterial <- new AnalyticsKeyContinuityStage[IO](paths, changed, execution, lookup, cats.effect.Clock[IO])
          .validateKeyMaterialContinuity(spark)
          .attempt
        _ <- IO(assert(wrongMaterial.left.exists(_.isInstanceOf[AnalyticsError.InvalidConfiguration])))
        marker <- execution(
          spark.createDataFrame(
            Vector(Row(token)).asJava,
            StructType(Vector(StructField("subjectToken", StringType, nullable = false)))
          )
        )
        captured <- erasure.captureMarkedFiles(spark, marker)
        _ <- IO(assert(captured.exists(_.endsWith(".parquet")) && captured.exists(_.endsWith(".json"))))
        before <- erasure.verifyFilesAbsent(spark, captured).attempt
        _ <- IO(assertEquals(before, Left(AnalyticsError.PhysicalReclamationUnverified)))
        _ <- IO.blocking(captured.foreach { reference =>
          val file = Path.of(new URI(reference)).toAbsolutePath.normalize()
          assert(file.startsWith(root.toAbsolutePath.normalize()))
          Files.delete(file)
        })
        _ <- erasure.verifyFilesAbsent(spark, captured)
      } yield ()
    }
  }
}
