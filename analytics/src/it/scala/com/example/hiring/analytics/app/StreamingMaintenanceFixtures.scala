package com.example.hiring.analytics.app

import cats.effect.{Clock, IO}
import com.example.hiring.analytics.HiringAnalyticsRecoveryTestSupport.Harness
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.{
  DeltaAnalyticsErasureLakehouse,
  DeltaLogFactory,
  DeltaStreamingBatchJournal,
  SparkExecution
}
import com.example.hiring.analytics.service.batch.{AnalyticsLakehouseLock, AnalyticsLakehousePaths}
import com.example.hiring.analytics.config.AnalyticsOperationalSettings
import com.example.hiring.analytics.domain.{StreamingActivationIdentity, StreamingLineage, SubjectPseudonymizer}
import com.example.hiring.analytics.service.streaming.StreamingCheckpointBatch
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** The idle-time maintenance tick composed over a disposable lakehouse, with controllable retention horizons. */
private[app] object StreamingMaintenanceFixtures {
  private val identity =
    StreamingActivationIdentity("maintenance-stream", "source", "lakehouse", "contract", "settings")

  final class Maintenance(
      val lakehouse: DeltaAnalyticsErasureLakehouse[IO],
      val tick: StreamingMaintenanceTick[IO],
      lock: AnalyticsLakehouseLock[IO],
      root: String
  ) {
    def run(
        lineage: StreamingLineage,
        at: Instant,
        retained: Vector[StreamingCheckpointBatch] = Vector.empty
    ): IO[Unit] =
      lock.resource(root).use(_ => tick.run(identity, lineage, () => IO.pure(retained), IO.unit, at))

    /** Reclaims the way the last step of every tick does, under the lakehouse mutex. */
    def reclaim: IO[Long] = lock.resource(root).use(_ => lakehouse.reclaimExpiredFiles)
  }

  def maintenance(
      harness: Harness,
      operational: AnalyticsOperationalSettings,
      progressRetention: FiniteDuration,
      logs: DeltaLogFactory = DeltaLogFactory.system
  ): Maintenance =
    maintenance(
      harness.spark,
      harness.runtime.client,
      harness.database,
      harness.execution,
      harness.paths,
      harness.keys,
      operational,
      progressRetention,
      logs
    )

  def maintenance(
      spark: SparkSession,
      client: MongoClient[IO],
      database: MongoDatabase[IO],
      execution: SparkExecution[IO],
      paths: AnalyticsLakehousePaths,
      keys: SubjectPseudonymizer,
      operational: AnalyticsOperationalSettings,
      progressRetention: FiniteDuration,
      logs: DeltaLogFactory
  ): Maintenance = {
    val streams = new MongoPublisherStream(operational)
    val lock = new MongoAnalyticsLakehouseLock[IO](database, streams, Clock[IO])
    val lakehouse = new DeltaAnalyticsErasureLakehouse[IO](
      spark,
      paths,
      keys,
      lock,
      new MongoHmacKeyRetirementAuthorizationStore[IO](database, streams),
      operational,
      execution,
      Slf4jLogger.getLogger[IO],
      Clock[IO],
      logs
    )
    new Maintenance(
      lakehouse,
      new StreamingMaintenanceTick[IO](
        lakehouse,
        new MongoActiveDeletionMarkerSource[IO](database, keys, streams),
        new MongoAnalyticsReportPublisher[IO](client, database, operational),
        new DeltaStreamingBatchJournal[IO](spark, paths, execution),
        new MongoAnalyticsLateFactReplayJournal[IO](database, streams, paths.root),
        operational.retention,
        progressRetention
      ),
      lock,
      paths.root
    )
  }

  /** Data files and Delta log files below a table directory, as relative names. */
  def tableFiles(table: String): IO[(Set[String], Set[String])] = IO.blocking {
    val root = Path.of(java.net.URI.create(table))
    if (!Files.isDirectory(root)) (Set.empty[String], Set.empty[String])
    else {
      val entries = Files.walk(root)
      try {
        val files = entries.iterator().asScala.filter(Files.isRegularFile(_)).map(root.relativize(_).toString).toSet
        val log = files.filter(_.startsWith("_delta_log/"))
        (files.filter(name => name.endsWith(".parquet") && !log.contains(name)), log.filter(_.endsWith(".json")))
      } finally entries.close()
    }
  }
}
