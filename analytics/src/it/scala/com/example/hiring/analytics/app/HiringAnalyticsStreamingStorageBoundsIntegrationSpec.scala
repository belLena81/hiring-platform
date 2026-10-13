package com.example.hiring.analytics.app

import cats.effect.IO
import cats.syntax.all.*
import com.example.hiring.analytics.AnalyticsBatchTestSupport
import com.example.hiring.analytics.IntegrationAnalyticsLakehousePaths
import com.example.hiring.analytics.StreamingIngestionFixtures.*
import com.example.hiring.analytics.adapter.kafka.KafkaRetentionAdapter
import com.example.hiring.analytics.adapter.spark.{LakehouseOperation, SparkRuntime}
import com.example.hiring.analytics.ShortRetentionFixtures.*
import com.example.hiring.analytics.app.StreamingMaintenanceFixtures.*
import com.example.hiring.analytics.config.{KafkaConnection, KafkaSecurityProtocol}
import com.example.hiring.analytics.domain.{AnalyticsDigest, AnalyticsTopic, StreamingBatchIdentity, StreamingLineage}
import com.example.hiring.analytics.service.batch.AnalyticsStorageInventory
import com.example.hiring.testing.KafkaShortRetention
import munit.CatsEffectSuite

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Real run-to-completion streams: owned scratch space, storage inventory, Kafka retention and structural bounds. */
final class HiringAnalyticsStreamingStorageBoundsIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 60.minutes

  private val oneBatch = 13

  private def observing(onBoundary: StreamingDurableBoundary => IO[Unit]): StreamingDurableBoundaryObserver[IO] =
    new StreamingDurableBoundaryObserver[IO] {
      override def completed(boundary: StreamingDurableBoundary, identity: StreamingBatchIdentity): IO[Unit] =
        onBoundary(boundary)
    }

  private def children(directory: Path): IO[Vector[String]] = IO.blocking {
    if (!Files.isDirectory(directory)) Vector.empty
    else {
      val entries = Files.list(directory)
      try entries.iterator().asScala.map(_.getFileName.toString).toVector
      finally entries.close()
    }
  }

  /** Every directory below the lakehouse root that holds a Delta log, as a normalized path. */
  private def deltaTables(root: Path): IO[Set[Path]] = IO.blocking {
    val entries = Files.walk(root)
    try
      entries
        .iterator()
        .asScala
        .filter(path => Files.isDirectory(path) && path.getFileName.toString == "_delta_log")
        .map(_.getParent.normalize)
        .toSet
    finally entries.close()
  }

  /** Parquet data files and their bytes across all Delta tables of the lakehouse. */
  private def dataFiles(root: Path): IO[(Long, Long)] = IO.blocking {
    val entries = Files.walk(root)
    try {
      val files = entries
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".parquet"))
        .filter(path => !path.toString.contains("/_delta_log/"))
        .toVector
      (files.size.toLong, files.map(Files.size).sum)
    } finally entries.close()
  }

  test(
    "HCS-11.4 HCS-11.6 the stream removes its owned scratch directory and every Delta table it created is inventoried"
  ) {
    fixture.use { fixture =>
      val observedDuringRun = new AtomicBoolean(false)
      for {
        settings <- settings(fixture, oneBatch)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until 12)))
        _ <- grantActivation(fixture, settings)
        _ <- runToCompletion(
          settings,
          Some(observing(boundary => {
            if (boundary == StreamingDurableBoundary.Prepared)
              children(fixture.sparkTemp).map(names => observedDuringRun.set(names.exists(_.startsWith("streaming-"))))
            else IO.unit
          }))
        ).timeout(10.minutes)
        remaining <- children(fixture.sparkTemp)
        tables <- deltaTables(Path.of(java.net.URI.create(settings.common.lakehouseRoot)))
        known = AnalyticsStorageInventory(
          IntegrationAnalyticsLakehousePaths.unsafe(settings.common.lakehouseRoot),
          Vector(fixture.checkpoint.toUri.toString),
          Vector(fixture.sparkTemp.toUri.toString)
        ).delta.map(surface => Path.of(java.net.URI.create(surface.location)).normalize).toSet
      } yield {
        assert(observedDuringRun.get(), "an owned streaming-* directory exists while the stream runs")
        assertEquals(remaining.filter(_.startsWith("streaming-")), Vector.empty[String])
        // HCS-11.6 shares this run: every Delta table a full run created is a listed storage surface.
        assert(tables.nonEmpty, "the run created Delta tables")
        assertEquals(tables.diff(known), Set.empty[Path], "Delta tables missing from the storage inventory")
      }
    }
  }

  test("HCS-11.5 the Kafka retention barrier passes only after the shortened topic retention removes the records") {
    fixture.use { fixture =>
      val manifest = fixture.namespace.manifest
      val connection = KafkaConnection(
        manifest.kafkaBootstrap,
        Some("analytics_reader"),
        Some(manifest.readerPassword),
        KafkaSecurityProtocol.SaslPlaintext,
        allowPlaintext = true
      )
      for {
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until 12)))
        topic <- IO.fromEither(AnalyticsTopic.from(fixture.namespace.events).leftMap(new IllegalArgumentException(_)))
        retention = KafkaRetentionAdapter
          .liveRetention[IO](connection, topic, AnalyticsBatchTestSupport.driverExecution)
        barrier <- retention.capture()
        before <- retention.retentionPassed(barrier)
        _ <- KafkaShortRetention.probe(manifest).use(_.expireCurrentRecords(fixture.namespace.events))
        after <- retention.retentionPassed(barrier)
      } yield {
        assert(!before, "the captured records are still retained")
        assert(after, "the barrier passes once the earliest offset moves beyond it")
      }
    }
  }

  test(
    "HCS-13.1 five batches of committed events keep Delta data files within structural bounds and maintenance reduces them"
  ) {
    fixture.use { fixture =>
      val batches = new AtomicInteger(0)
      val records = 65
      for {
        settings <- settings(fixture, oneBatch)
        _ <- publish(fixture.namespace, Instant.now().minusSeconds(30L), Vector(Transaction(0 until records)))
        identity <- grantActivation(fixture, settings)
        _ <- runToCompletion(
          settings,
          Some(
            observing(boundary =>
              IO.whenA(boundary == StreamingDurableBoundary.Prepared)(IO(batches.incrementAndGet()).void)
            )
          )
        ).timeout(50.minutes)
        root = Path.of(java.net.URI.create(settings.common.lakehouseRoot))
        counts <- stored(fixture, settings)
        measured <- dataFiles(root)
        lineage <- IO.fromEither(
          StreamingLineage.from(AnalyticsDigest.sha256Hex(identity.canonical)).leftMap(new IllegalArgumentException(_))
        )
        paths = IntegrationAnalyticsLakehousePaths.unsafe(settings.common.lakehouseRoot)
        reduced <- SparkRuntime
          .sparkMongo[IO](
            fixture.namespace.manifest.mongoUri,
            "local[2]",
            "HiringAnalyticsIngestionMaintenance",
            Some(false),
            fixture.root.resolve("maintenance-temp").toString
          )
          .use { case (spark, client, blocking) =>
            for {
              database <- client.getDatabase(fixture.databaseName)
              maintenance = StreamingMaintenanceFixtures.maintenance(
                spark,
                client,
                database,
                new LakehouseOperation[IO](blocking),
                paths,
                settings.common.pseudonymizer,
                shortRetention(settings.common.operational),
                7.days,
                shiftedLogs(32L)
              )
              _ <- maintenance.run(lineage, Instant.now())
              _ <- dataFilesAfterReclaim(root, maintenance, measured._1)
              after <- dataFiles(root)
            } yield after
          }
      } yield {
        val batchCount = batches.get()
        assertEquals(counts.silver, records.toLong)
        assert(batchCount >= 5 && batchCount <= 7, s"about five batches ran: $batchCount")
        assert(
          measured._1 <= batchCount * FilesPerBatch + FileCap,
          s"data files ${measured._1} for $batchCount batches"
        )
        assert(measured._2 <= ByteCap, s"data bytes ${measured._2}")
        assert(reduced._1 < measured._1, s"maintenance reduced data files from ${measured._1} to ${reduced._1}")
      }
    }
  }

  /** Structural bounds, set with margin over the first measured 19 batches: 748 data files and 5,039,945 bytes (about
    * 40 files per batch). Bounds: 50 files per batch plus an allowance of 100, and 25 MB.
    */
  private val FilesPerBatch = 50L
  private val FileCap = 100L
  private val ByteCap = 25L * 1000L * 1000L

  /** Bounded polling until reclamation removed at least one data file, since Delta vacuums only after one second. */
  private def dataFilesAfterReclaim(
      root: Path,
      maintenance: Maintenance,
      before: Long,
      attempts: Int = 60
  ): IO[Unit] =
    dataFiles(root).flatMap { case (files, _) =>
      if (files < before) IO.unit
      else if (attempts == 0) IO.raiseError(new AssertionError("maintenance did not reduce data files in time"))
      else maintenance.reclaim *> IO.sleep(1.second) *> dataFilesAfterReclaim(root, maintenance, before, attempts - 1)
    }
}
