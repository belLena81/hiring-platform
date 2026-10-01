package com.example.hiring.analytics.service.batch

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.config.AnalyticsPositiveInt
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError

import java.time.Instant

enum AnalyticsLateFactReplayProgress {
  case Prepared, FactsMerged, Published
}

final case class AnalyticsLateFactReplayRecord(
    request: AnalyticsLateFactReplayRequest,
    progress: AnalyticsLateFactReplayProgress,
    publicationAttempt: Int
)

/** The implementation must bind request ID to the complete immutable selection and make transitions idempotent. */
trait AnalyticsLateFactReplayJournal[F[_]] {
  def load(requestId: AnalyticsReplayRequestId): F[Option[AnalyticsLateFactReplayRecord]]
  def prepare(request: AnalyticsLateFactReplayRequest, at: Instant): F[AnalyticsLateFactReplayRecord]
  def markFactsMerged(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit]
  def advancePublicationAttempt(request: AnalyticsLateFactReplayRequest): F[Int]
  def markPublished(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit]
}

/** Spark-backed validation and report work remains at the adapter boundary. */
trait AnalyticsLateFactReplayStages[F[_]] {
  def validateHmacConfiguration: F[Unit]
  def validateSelectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[Unit]
  def applyActiveDeletions(activeTokens: Vector[SubjectToken]): F[Unit]

  /** Merge only the selected eligible facts; preserve each source row's original ingestion and expiry timestamps. */
  def mergeSelectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[Unit]
  def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput]
}

enum AnalyticsLateFactReplayOutcome {
  case Published, ErasurePending, AlreadyPublished
}

/** Replays one bounded explicit request without touching streaming progress or checkpoint acknowledgement. */
final class AnalyticsLateFactReplayService[F[_]: Async](
    lakehouseRoot: String,
    journal: AnalyticsLateFactReplayJournal[F],
    deletionMarkers: ActiveDeletionMarkerSource[F],
    stages: AnalyticsLateFactReplayStages[F],
    reportPublisher: AnalyticsReportPublisher[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    publishedSnapshotDays: AnalyticsPositiveInt,
    private[analytics] val nowOverride: Option[F[Instant]] = None
) {
  private val F = Async[F]
  private val now = nowOverride.getOrElse(F.realTimeInstant)
  private val MaximumPublicationAttempts = 3

  def run(request: AnalyticsLateFactReplayRequest): F[AnalyticsLateFactReplayOutcome] =
    lakehouseLock.resource(lakehouseRoot).use(_ => runLocked(request))

  private def runLocked(request: AnalyticsLateFactReplayRequest): F[AnalyticsLateFactReplayOutcome] =
    for {
      observedAt <- now
      existing <- journal.load(request.requestId)
      _ <- validateRequestIdentity(request, existing)
      outcome <- existing match {
        case Some(record) if record.progress == AnalyticsLateFactReplayProgress.Published =>
          F.pure(AnalyticsLateFactReplayOutcome.AlreadyPublished)
        case Some(record) if record.progress == AnalyticsLateFactReplayProgress.FactsMerged =>
          reconcilePublication(request, record, observedAt).flatMap {
            case Some(reconciled) => F.pure(reconciled)
            case None             => processUnfinished(request, existing, observedAt)
          }
        case _ => processUnfinished(request, existing, observedAt)
      }
    } yield outcome

  /** Resolve a prior publish before requiring late facts that may have reached expiry after that publish. */
  private def reconcilePublication(
      request: AnalyticsLateFactReplayRequest,
      record: AnalyticsLateFactReplayRecord,
      observedAt: Instant
  ): F[Option[AnalyticsLateFactReplayOutcome]] =
    for {
      identity <- F.fromEither(reservationIdentityFor(request, record.publicationAttempt))
      reservation <- reportPublisher.reserve(identity._1, identity._2, observedAt)
      receipt <- reportPublisher.publicationReceipt(reservation)
      reconciled <- receipt match {
        case AnalyticsReportPublicationReceipt.CurrentGeneration =>
          journal.markPublished(request, observedAt).as(Some(AnalyticsLateFactReplayOutcome.Published))
        case AnalyticsReportPublicationReceipt.Superseded =>
          advanceOrReject(request, observedAt, record.publicationAttempt).map(Some(_))
        case AnalyticsReportPublicationReceipt.Absent => F.pure(None)
      }
    } yield reconciled

  private def processUnfinished(
      request: AnalyticsLateFactReplayRequest,
      existing: Option[AnalyticsLateFactReplayRecord],
      observedAt: Instant
  ): F[AnalyticsLateFactReplayOutcome] =
    for {
      _ <- stages.validateHmacConfiguration
      initialTokens <- deletionMarkers.activeSubjectTokens
      _ <- stages.validateSelectedFacts(request, initialTokens, observedAt)
      prepared <- existing.fold(journal.prepare(request, observedAt))(F.pure)
      result <-
        if (prepared.progress == AnalyticsLateFactReplayProgress.Published)
          F.pure(AnalyticsLateFactReplayOutcome.AlreadyPublished)
        else
          for {
            writeTokens <- deletionMarkers.activeSubjectTokens
            _ <- stages.validateHmacConfiguration
            _ <- stages.validateSelectedFacts(request, writeTokens, observedAt)
            _ <- stages.applyActiveDeletions(writeTokens)
            _ <- stages.mergeSelectedFacts(request, writeTokens, observedAt)
            _ <- journal.markFactsMerged(request, observedAt)
            outcome <- publishAfterFreshDeletionCheck(request, observedAt, prepared.publicationAttempt, 0)
          } yield outcome
    } yield result

  private def publishAfterFreshDeletionCheck(
      request: AnalyticsLateFactReplayRequest,
      observedAt: Instant,
      publicationAttempt: Int,
      deletionRefreshes: Int
  ): F[AnalyticsLateFactReplayOutcome] =
    for {
      beforeReport <- deletionMarkers.activeSubjectTokens
      _ <- stages.validateHmacConfiguration
      _ <- stages.applyActiveDeletions(beforeReport)
      _ <- stages.validateSelectedFacts(request, beforeReport, observedAt)
      report <- stages.rebuildGoldAndExtractReport(observedAt)
      beforePublish <- deletionMarkers.activeSubjectTokens
      outcome <-
        if (beforeReport.toSet != beforePublish.toSet && deletionRefreshes < MaximumPublicationAttempts)
          publishAfterFreshDeletionCheck(request, observedAt, publicationAttempt, deletionRefreshes + 1)
        else if (beforeReport.toSet != beforePublish.toSet)
          F.raiseError[AnalyticsLateFactReplayOutcome](AnalyticsError.LateFactReplayRejected)
        else publishWithReceipt(request, report, observedAt, publicationAttempt)
    } yield outcome

  private def publishWithReceipt(
      request: AnalyticsLateFactReplayRequest,
      report: AnalyticsReportOutput,
      observedAt: Instant,
      publicationAttempt: Int
  ): F[AnalyticsLateFactReplayOutcome] =
    for {
      reservationIdentity <- F.fromEither(reservationIdentityFor(request, publicationAttempt))
      reservation <- reportPublisher.reserve(reservationIdentity._1, reservationIdentity._2, observedAt)
      receipt <- reportPublisher.publicationReceipt(reservation)
      outcome <- receipt match {
        case AnalyticsReportPublicationReceipt.CurrentGeneration =>
          journal.markPublished(request, observedAt).as(AnalyticsLateFactReplayOutcome.Published)
        case AnalyticsReportPublicationReceipt.Superseded =>
          advanceOrReject(request, observedAt, publicationAttempt)
        case AnalyticsReportPublicationReceipt.Absent =>
          val expiresAt = observedAt.plusSeconds(publishedSnapshotDays.toLong * 86400L)
          reportPublisher
            .publish(reservation, report, expiresAt)
            .attempt
            .flatMap {
              case Right(_) =>
                journal.markPublished(request, observedAt).as(AnalyticsLateFactReplayOutcome.Published)
              case Left(AnalyticsError.GuardedErasurePublicationRejected) =>
                F.pure(AnalyticsLateFactReplayOutcome.ErasurePending)
              case Left(error) => F.raiseError(error)
            }
      }
    } yield outcome

  private def advanceOrReject(
      request: AnalyticsLateFactReplayRequest,
      observedAt: Instant,
      publicationAttempt: Int
  ): F[AnalyticsLateFactReplayOutcome] =
    if (publicationAttempt + 1 >= MaximumPublicationAttempts)
      F.raiseError(AnalyticsError.LateFactReplayRejected)
    else
      for {
        nextAttempt <- journal.advancePublicationAttempt(request)
        _ <-
          if (nextAttempt == publicationAttempt + 1) F.unit
          else F.raiseError[Unit](AnalyticsError.LateFactReplayRequestConflict)
        markers <- deletionMarkers.activeSubjectTokens
        _ <- stages.validateHmacConfiguration
        _ <- stages.validateSelectedFacts(request, markers, observedAt)
        _ <- stages.applyActiveDeletions(markers)
        outcome <- publishAfterFreshDeletionCheck(request, observedAt, nextAttempt, 0)
      } yield outcome

  private def reservationIdentityFor(
      request: AnalyticsLateFactReplayRequest,
      publicationAttempt: Int
  ): Either[AnalyticsError, (RunId, RangeFingerprint)] =
    for {
      runId <- RunId
        .from(s"late-replay-${request.requestId.value}-$publicationAttempt")
        .leftMap(_ => AnalyticsError.InvalidConfiguration("late-fact replay request identity is invalid"))
      fingerprint <- RangeFingerprint
        .from(request.selectionDigest)
        .leftMap(_ => AnalyticsError.InvalidConfiguration("late-fact replay selection digest is invalid"))
    } yield runId -> fingerprint

  private def validateRequestIdentity(
      request: AnalyticsLateFactReplayRequest,
      existing: Option[AnalyticsLateFactReplayRecord]
  ): F[Unit] =
    existing match {
      case Some(record) if record.request.selectionDigest != request.selectionDigest =>
        F.raiseError(AnalyticsError.LateFactReplayRequestConflict)
      case Some(record) if record.publicationAttempt < 0 || record.publicationAttempt >= MaximumPublicationAttempts =>
        F.raiseError(AnalyticsError.LateFactReplayRequestConflict)
      case _ => F.unit
    }
}
