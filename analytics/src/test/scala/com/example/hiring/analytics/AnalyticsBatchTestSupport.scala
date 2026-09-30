package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.{
  AnalyticsKeyContinuityStage,
  DeltaAnalyticsErasureLakehouse,
  BoundedOperationalEventSource,
  DeltaManifestStore,
  SparkAnalyticsBatchLakehouse,
  KeyRetirementLookup,
  SparkExecution,
  AnalyticsBatchIngestionStage,
  AnalyticsBatchSilverStage,
  DeltaBatchReader,
  DeltaBatchWriter,
  LakehouseOperation,
  QuarantineIdentifier
}
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.AnalyticsErasureWorker
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore
import cats.effect.{Clock, IO, Resource}
import org.apache.spark.sql.SparkSession
import java.time.Instant
import org.typelevel.log4cats.slf4j.Slf4jLogger

private[analytics] object AnalyticsBatchTestSupport {
  val reportPublisher: AnalyticsReportPublisher[IO] = new AnalyticsReportPublisher[IO] {
    override def reserve(
        runId: com.example.hiring.analytics.domain.RunId,
        rangeFingerprint: com.example.hiring.analytics.domain.RangeFingerprint,
        now: Instant
    ) =
      IO.pure(AnalyticsReportReservation(runId, rangeFingerprint, 0L, 1L))
    override def publish(
        reservation: AnalyticsReportReservation,
        report: com.example.hiring.analytics.domain.AnalyticsReportOutput,
        expiresAt: Instant
    ) = IO.unit
    override def publishErasure(
        reservation: AnalyticsReportReservation,
        report: com.example.hiring.analytics.domain.AnalyticsReportOutput,
        expiresAt: Instant,
        claim: com.example.hiring.analytics.service.erasure.ErasureClaim,
        completedAt: Instant
    ) = IO.unit
  }

  val lakehouseLock: AnalyticsLakehouseLock[IO] = new AnalyticsLakehouseLock[IO] {
    override def resource(root: String): Resource[IO, Unit] = Resource.unit
  }

  val retirementStore: HmacKeyRetirementAuthorizationStore[IO] = new HmacKeyRetirementAuthorizationStore[IO] {
    override def list(root: String) = IO.pure(Vector.empty)
    override def insert(
        root: String,
        authorization: com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
    ) = IO.unit
  }

  private val sparkExecution = com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
    .forTests[IO](scala.concurrent.ExecutionContext.parasitic)
  private val lakehouseExecution = new LakehouseOperation[IO](sparkExecution)
  val driverExecution: com.example.hiring.analytics.adapter.spark.SparkBlockingExecution[IO] = sparkExecution

  final class BatchHarness private[AnalyticsBatchTestSupport] (
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      markers: ActiveDeletionMarkerSource[IO],
      clock: Clock[IO] = Clock[IO],
      reportPublisher: AnalyticsReportPublisher[IO] = AnalyticsBatchTestSupport.reportPublisher,
      manifests: Option[AnalyticsRunManifestStore[IO]] = None
  ) {
    def run(
        spark: SparkSession,
        source: BoundedOperationalEventSource[IO],
        manifest: com.example.hiring.analytics.domain.AnalyticsRunManifest
    ) = {
      val store = manifests.getOrElse(new DeltaManifestStore[IO](spark, paths, sparkExecution))
      val maintenance = newMaintenance(spark, paths, pseudonymizer, clock)
      val deltaWriter = new DeltaBatchWriter[IO](paths, lakehouseExecution)
      val deltaReader = new DeltaBatchReader[IO](lakehouseExecution)
      val ingestion = new AnalyticsBatchIngestionStage[IO](
        paths,
        pseudonymizer,
        lakehouseExecution,
        store,
        deltaWriter,
        AnalyticsTestOperationalConfig.operational.retention,
        Some(clock.realTimeInstant)
      )
      val silver = new AnalyticsBatchSilverStage[IO](
        paths,
        pseudonymizer,
        lakehouseExecution,
        deltaWriter,
        deltaReader,
        QuarantineIdentifier,
        AnalyticsTestOperationalConfig.operational.retention
      )
      val lakehouse = new SparkAnalyticsBatchLakehouse[IO](
        spark,
        paths,
        source,
        lakehouseExecution,
        maintenance,
        ingestion,
        silver
      )
      new HiringAnalyticsBatch[IO](
        paths,
        markers,
        reportPublisher,
        store,
        lakehouse,
        lakehouseLock,
        com.example.hiring.analytics.service.batch.AnalyticsStreamingRegistry.allowUnregistered[IO],
        AnalyticsTestOperationalConfig.operational,
        Some(clock.realTimeInstant)
      ).run(manifest)
    }
  }

  def newBatch(
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      markers: ActiveDeletionMarkerSource[IO],
      clock: Clock[IO] = Clock[IO],
      reportPublisher: AnalyticsReportPublisher[IO] = AnalyticsBatchTestSupport.reportPublisher,
      manifests: Option[AnalyticsRunManifestStore[IO]] = None
  ): BatchHarness =
    new BatchHarness(paths, pseudonymizer, markers, clock, reportPublisher, manifests)

  def newMaintenance(
      spark: SparkSession,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      clock: Clock[IO] = Clock[IO]
  ): DeltaAnalyticsErasureLakehouse[IO] =
    new DeltaAnalyticsErasureLakehouse[IO](
      spark,
      paths,
      pseudonymizer,
      lakehouseLock,
      retirementStore,
      AnalyticsTestOperationalConfig.operational,
      lakehouseExecution,
      Slf4jLogger.getLogger[IO],
      Some(clock.realTimeInstant)
    )

  def newKeyContinuityStage(
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      clock: Clock[IO] = Clock[IO]
  ): AnalyticsKeyContinuityStage[IO] = {
    val execution = new SparkExecution[IO] {
      override def apply[A](work: => A): IO[A] = IO.blocking(work)
      override def either[A](work: => Either[com.example.hiring.analytics.errors.AnalyticsError, A]): IO[A] =
        IO.blocking(work).flatMap(IO.fromEither)
    }
    val lookup = new KeyRetirementLookup[IO] {
      override def list(lakehouseRoot: String) = retirementStore.list(lakehouseRoot)
    }
    new AnalyticsKeyContinuityStage[IO](paths, pseudonymizer, execution, lookup, Some(clock.realTimeInstant))
  }
}
