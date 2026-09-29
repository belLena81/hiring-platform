package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.kafka.KafkaProducerFencer
import com.example.hiring.analytics.adapter.mongo.{
  MongoActiveDeletionMarkerSource,
  MongoAnalyticsErasureBarrier,
  MongoAnalyticsErasureProgress,
  MongoAnalyticsErasureQueue,
  MongoAnalyticsErasureStores,
  MongoAnalyticsLakehouseLock,
  MongoAnalyticsReportPublisher,
  MongoHmacKeyRetirementAuthorizationStore
}
import com.example.hiring.analytics.adapter.spark.{
  DeltaAnalyticsErasureLakehouse,
  LakehouseOperation,
  SparkBlockingExecution
}
import com.example.hiring.analytics.config.{
  AnalyticsErasureWorkerPolicy,
  AnalyticsErasureWorkerTimings,
  KafkaConnection
}
import com.example.hiring.analytics.domain.{AnalyticsTopic, SubjectPseudonymizer}
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsReportPublisher}
import com.example.hiring.analytics.service.erasure.{
  AnalyticsErasureWorker,
  AnalyticsErasureKafkaRuntime,
  KafkaRetention,
  KafkaRetentionBarrier,
  TransactionalProducerFencer
}
import com.example.hiring.analytics.domain.{AccountSubjectId, RangeFingerprint, RunId}
import com.example.hiring.analytics.domain.AnalyticsDigest
import cats.effect.{Clock, IO}
import cats.effect.unsafe.implicits.global
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*

/** Builds the production-shaped required collaborator graph for erasure integration cases. */
private[analytics] object AnalyticsErasureWorkerTestSupport {
  def stores(client: MongoClient[IO], database: MongoDatabase[IO]): MongoAnalyticsErasureStores[IO] =
    MongoAnalyticsErasureStores
      .resource(client, database, AnalyticsTestOperationalConfig.streams)
      .allocated
      .map(_._1)
      .unsafeRunSync()

  def barrier(database: MongoDatabase[IO]): MongoAnalyticsErasureBarrier[IO] =
    MongoAnalyticsErasureBarrier
      .resource(database, AnalyticsTestOperationalConfig.streams)
      .allocated
      .map(_._1)
      .unsafeRunSync()

  def runId(raw: String): RunId = RunId.from(raw).toOption.get
  def accountSubjectId(raw: String): AccountSubjectId = AccountSubjectId.from(raw).toOption.get

  def fingerprint(raw: String): RangeFingerprint =
    RangeFingerprint.from(AnalyticsDigest.sha256Hex(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8))).toOption.get

  def worker(
      spark: SparkSession,
      database: MongoDatabase[IO],
      stores: MongoAnalyticsErasureStores[IO],
      kafka: KafkaConnection,
      fencerKafka: KafkaConnection,
      topic: String,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      publisher: AnalyticsReportPublisher[IO],
      clock: Clock[IO] = Clock[IO],
      leaseDuration: FiniteDuration = 90.seconds,
      deliveryTimeout: FiniteDuration = 30.seconds,
      pollInterval: FiniteDuration = 5.seconds,
      producerFencer: TransactionalProducerFencer[IO] =
        KafkaProducerFencer[IO](AnalyticsBatchTestSupport.driverExecution),
      kafkaRetention: Option[KafkaRetention[IO]] = None
  ): AnalyticsErasureWorker[IO] = {
    val lock = new MongoAnalyticsLakehouseLock(
      database,
      AnalyticsTestOperationalConfig.streams,
      Some(clock.realTimeInstant),
      Some(clock.monotonic)
    )
    val markers =
      new MongoActiveDeletionMarkerSource[IO](
        database,
        pseudonymizer,
        streams = AnalyticsTestOperationalConfig.streams
      )
    val lakehouseExecution = new LakehouseOperation[IO](
      SparkBlockingExecution.forTests[IO](scala.concurrent.ExecutionContext.parasitic)
    )
    val maintenance = new DeltaAnalyticsErasureLakehouse[IO](
      spark,
      paths,
      pseudonymizer,
      lock,
      new MongoHmacKeyRetirementAuthorizationStore[IO](database, AnalyticsTestOperationalConfig.streams),
      AnalyticsTestOperationalConfig.operational,
      lakehouseExecution,
      Slf4jLogger.getLogger[IO],
      Some(clock.realTimeInstant)
    )
    val refinedTopic = AnalyticsTopic
      .from(topic)
      .toOption
      .getOrElse(
        throw new IllegalArgumentException("test topic must be non-empty")
      )
    val retention = kafkaRetention.getOrElse(
      com.example.hiring.analytics.adapter.kafka.KafkaRetentionAdapter.liveRetention[IO](
        kafka,
        refinedTopic,
        AnalyticsBatchTestSupport.driverExecution
      )
    )
    new AnalyticsErasureWorker[IO](
      stores.queue,
      stores.progress,
      stores.barrier,
      AnalyticsErasureKafkaRuntime(fencerKafka, producerFencer, retention),
      paths,
      publisher,
      markers,
      maintenance,
      lock,
      Slf4jLogger.getLogger[IO],
      AnalyticsErasureWorkerPolicy(
        AnalyticsTestOperationalConfig.operational.retention,
        AnalyticsErasureWorkerTimings(leaseDuration, deliveryTimeout, pollInterval)
      ),
      Some(clock.realTimeInstant)
    )
  }
}
