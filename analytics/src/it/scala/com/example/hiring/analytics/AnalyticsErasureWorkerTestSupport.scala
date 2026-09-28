package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, TransactionalProducerFencer}
import com.example.hiring.analytics.adapter.mongo.{
  MongoActiveDeletionMarkerSource,
  MongoAnalyticsErasureWorkerStore,
  MongoAnalyticsLakehouseLock,
  MongoAnalyticsReportPublisher,
  MongoHmacKeyRetirementAuthorizationStore
}
import com.example.hiring.analytics.adapter.spark.{AnalyticsErasureWorker, DeltaManifestStore, HiringAnalyticsBatch}
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, AnalyticsReportPublisher}
import com.example.hiring.analytics.service.erasure.{KafkaRetention, KafkaRetentionBarrier}
import com.example.hiring.analytics.domain.{AccountSubjectId, RangeFingerprint, RunId}
import com.example.hiring.analytics.domain.AnalyticsDigest
import cats.effect.{Clock, IO}
import com.mongodb.reactivestreams.client.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*

/** Builds the production-shaped required collaborator graph for erasure integration cases. */
private[analytics] object AnalyticsErasureWorkerTestSupport {
  def runId(raw: String): RunId = RunId.from(raw).toEither.toOption.get
  def accountSubjectId(raw: String): AccountSubjectId = AccountSubjectId.from(raw).toOption.get

  def fingerprint(raw: String): RangeFingerprint =
    RangeFingerprint.from(AnalyticsDigest.sha256Hex(raw.getBytes(java.nio.charset.StandardCharsets.UTF_8))).toOption.get

  def worker(
      spark: SparkSession,
      database: MongoDatabase,
      store: MongoAnalyticsErasureWorkerStore[IO],
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
      producerFencer: TransactionalProducerFencer[IO] = KafkaProducerFencer[IO],
      kafkaRetention: KafkaRetention[IO] =
        com.example.hiring.analytics.adapter.kafka.KafkaRetentionAdapter.liveRetention[IO]
  ): AnalyticsErasureWorker[IO] = {
    val lock = new MongoAnalyticsLakehouseLock(database, clock, AnalyticsTestOperationalConfig.streams)
    val markers =
      new MongoActiveDeletionMarkerSource[IO](database, pseudonymizer, streams = AnalyticsTestOperationalConfig.streams)
    val batch = new HiringAnalyticsBatch[IO](
      paths,
      pseudonymizer,
      markers,
      clock,
      publisher,
      new DeltaManifestStore[IO](paths),
      lock,
      new MongoHmacKeyRetirementAuthorizationStore[IO](database, AnalyticsTestOperationalConfig.streams),
      AnalyticsTestOperationalConfig.operational,
      com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
        .forTests[IO](scala.concurrent.ExecutionContext.parasitic),
      Slf4jLogger.getLogger[IO]
    )
    new AnalyticsErasureWorker[IO](
      spark,
      store,
      store,
      store,
      kafka,
      fencerKafka,
      topic,
      paths,
      publisher,
      markers,
      batch,
      lock,
      clock,
      Slf4jLogger.getLogger[IO],
      producerFencer,
      kafkaRetention,
      AnalyticsTestOperationalConfig.operational.retention,
      leaseDuration,
      deliveryTimeout,
      pollInterval
    )
  }
}
