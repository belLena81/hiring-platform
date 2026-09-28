package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.spark.{
  AnalyticsKeyContinuityStage,
  DeltaAnalyticsErasureLakehouse,
  DeltaManifestStore,
  HiringAnalyticsBatch,
  KeyContinuityStagePorts,
  KeyRetirementLookup,
  SparkExecution
}
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorizationStore
import cats.effect.{Clock, IO, Resource}
import org.apache.spark.sql.{DataFrame, SparkSession}
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

  def newBatch(
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      markers: ActiveDeletionMarkerSource[IO],
      clock: Clock[IO] = Clock[IO],
      reportPublisher: AnalyticsReportPublisher[IO] = AnalyticsBatchTestSupport.reportPublisher,
      manifests: Option[ManifestStore[IO]] = None
  ): HiringAnalyticsBatch[IO] =
    new HiringAnalyticsBatch[IO](
      paths,
      pseudonymizer,
      markers,
      clock,
      reportPublisher,
      manifests.getOrElse(new DeltaManifestStore[IO](paths)),
      lakehouseLock,
      AnalyticsTestOperationalConfig.operational,
      sparkExecution,
      newMaintenance(paths, pseudonymizer, clock)
    )

  def newMaintenance(
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      clock: Clock[IO] = Clock[IO]
  ): DeltaAnalyticsErasureLakehouse[IO] =
    new DeltaAnalyticsErasureLakehouse[IO](
      paths,
      pseudonymizer,
      clock,
      lakehouseLock,
      retirementStore,
      AnalyticsTestOperationalConfig.operational,
      sparkExecution,
      Slf4jLogger.getLogger[IO]
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
    new AnalyticsKeyContinuityStage[IO](KeyContinuityStagePorts(paths, pseudonymizer, execution, clock, lookup))
  }
}
