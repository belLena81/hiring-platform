package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.{DeltaStreamingProgressStore, SparkBlockingExecution}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite
import org.apache.spark.sql.SparkSession

import java.nio.file.Files
import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

final class DeltaStreamingProgressStoreSpec extends FunSuite {
  override val munitTimeout: FiniteDuration = 5.minutes

  private val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("DeltaStreamingProgressStoreSpec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  private val root = Files.createTempDirectory("analytics-stream-progress-").toUri.toString
  private val paths = AnalyticsLakehousePaths.from(root).toEither.toOption.get
  private val execution = SparkBlockingExecution.forTests[IO](ExecutionContext.parasitic)
  private val store = new DeltaStreamingProgressStore[IO](spark, paths, execution)
  private val lineage = StreamingLineage.from("test-lineage").toOption.get
  private val batchId = StreamingBatchId.from(0L).toOption.get
  private val identity = StreamingBatchIdentity(lineage, batchId)
  private val fingerprint = RangeFingerprint.from("a" * 64).toOption.get
  private val offset = StreamingPartitionSummary.from("hiring.events", 0, 10L, 10L, 1L).toEither.toOption.get
  private val preparation = StreamingBatchPreparation(
    identity,
    Instant.parse("2026-09-30T10:00:00Z"),
    None,
    None,
    fingerprint,
    Vector(offset)
  )

  test("preparation writes once, identical retries succeed, and conflicting retries fail") {
    val conflicting = preparation.copy(inputFingerprint = RangeFingerprint.from("b" * 64).toOption.get)
    for {
      _ <- store.prepare(preparation)
      prepared <- store.load(identity)
      _ <- store.prepare(preparation)
      conflict <- store.prepare(conflicting).attempt
    } yield {
      assertEquals(prepared.map(_.outcome), Some(StreamingBatchOutcome.Prepared))
      assert(conflict.left.exists(_.isInstanceOf[com.example.hiring.analytics.errors.AnalyticsError.InvalidInput]))
    }
  }

  override def afterAll(): Unit = {
    spark.stop()
    val rootPath = java.nio.file.Paths.get(new java.net.URI(root))
    val paths = Files.walk(rootPath)
    try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
    finally paths.close()
  }
}
