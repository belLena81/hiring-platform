package com.example.hiring.analytics.service.batch

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.config.AnalyticsPositiveInt
import com.example.hiring.analytics.domain.{
  AnalyticsLateFactReplayRequest,
  AnalyticsReplayRequestId,
  AnalyticsReportOutput,
  RangeFingerprint,
  RunId,
  SubjectToken
}
import com.example.hiring.analytics.errors.AnalyticsError

import java.time.Instant

enum AnalyticsLateFactReplayProgress {
  case Prepared, FactsMerged, Published
}

/** A completed request may retain only its digest binding and publication receipt. */
final case class AnalyticsLateFactReplayRecord(
    selectionDigest: String,
    progress: AnalyticsLateFactReplayProgress,
    publicationAttempt: Int,
    reservation: AnalyticsReportReservation
)

trait AnalyticsLateFactReplayJournal[F[_]] {
  def load(requestId: AnalyticsReplayRequestId): F[Option[AnalyticsLateFactReplayRecord]]
  def prepare(
      request: AnalyticsLateFactReplayRequest,
      reservation: AnalyticsReportReservation,
      at: Instant
  ): F[AnalyticsLateFactReplayRecord]
  def markFactsMerged(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit]
  def advancePublicationAttempt(
      request: AnalyticsLateFactReplayRequest,
      expectedAttempt: Int,
      reservation: AnalyticsReportReservation,
      at: Instant
  ): F[AnalyticsLateFactReplayRecord]
  def markPublished(request: AnalyticsLateFactReplayRequest, at: Instant): F[Unit]
}

trait AnalyticsLateFactReplayStages[F[_]] {
  def validateHmacConfiguration: F[Unit]
  def validateSelectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[Unit]
  def applyActiveDeletions(activeTokens: Vector[SubjectToken]): F[Unit]

  /** Merge selected eligible facts with their original ingestion and expiry timestamps. */
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

/** One bounded explicit replay, independent of streaming progress and checkpoint acknowledgement. */
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
  import AnalyticsLateFactReplayService.*
  private val F = Async[F]
  private val now = nowOverride.getOrElse(F.realTimeInstant)

  def run(request: AnalyticsLateFactReplayRequest): F[AnalyticsLateFactReplayOutcome] =
    lakehouseLock.resource(lakehouseRoot).use { _ =>
      for {
        existing <- journal.load(request.requestId)
        _ <- existing.traverse_(validateRecord(request, _))
        outcome <- existing match {
          case Some(record) if record.progress == AnalyticsLateFactReplayProgress.Published =>
            F.pure(AnalyticsLateFactReplayOutcome.AlreadyPublished)
          case Some(record) => processAttempt(request, record)
          case None         =>
            for {
              observedAt <- now
              reservation <- reserve(request, 0, observedAt)
              // Pin generation before reading any subject-attributed fact.
              _ <- validateSelection(request, observedAt)
              record <- journal.prepare(request, reservation, observedAt)
              _ <- validateRecord(request, record)
              result <- processAttempt(request, record)
            } yield result
        }
      } yield outcome
    }

  /** Resolve a committed publication before requiring source facts that may since have expired. */
  private def processAttempt(
      request: AnalyticsLateFactReplayRequest,
      record: AnalyticsLateFactReplayRecord
  ): F[AnalyticsLateFactReplayOutcome] =
    reportPublisher.publicationReceipt(record.reservation).flatMap {
      case AnalyticsReportPublicationReceipt.CurrentGeneration =>
        now.flatMap(journal.markPublished(request, _)).as(AnalyticsLateFactReplayOutcome.Published)
      case AnalyticsReportPublicationReceipt.Superseded => newAttempt(request, record)
      case AnalyticsReportPublicationReceipt.Absent     => mergeAndPublish(request, record)
    }

  private def mergeAndPublish(
      request: AnalyticsLateFactReplayRequest,
      record: AnalyticsLateFactReplayRecord
  ): F[AnalyticsLateFactReplayOutcome] =
    for {
      observedAt <- now
      tokens <- deletionMarkers.activeSubjectTokens
      _ <- stages.validateHmacConfiguration
      _ <- stages.validateSelectedFacts(request, tokens, observedAt)
      _ <- stages.applyActiveDeletions(tokens)
      // The adapter revalidates at the actual sink boundary without extending source expiry.
      _ <- stages.mergeSelectedFacts(request, tokens, observedAt)
      _ <- journal.markFactsMerged(request, observedAt)
      result <- rebuildAndPublish(request, record, 0)
    } yield result

  private def rebuildAndPublish(
      request: AnalyticsLateFactReplayRequest,
      record: AnalyticsLateFactReplayRecord,
      refreshes: Int
  ): F[AnalyticsLateFactReplayOutcome] =
    for {
      observedAt <- now
      beforeReport <- deletionMarkers.activeSubjectTokens
      _ <- stages.validateHmacConfiguration
      _ <- stages.validateSelectedFacts(request, beforeReport, observedAt)
      _ <- stages.applyActiveDeletions(beforeReport)
      report <- stages.rebuildGoldAndExtractReport(observedAt)
      beforePublish <- deletionMarkers.activeSubjectTokens
      outcome <-
        if (beforeReport.toSet != beforePublish.toSet && refreshes + 1 < MaximumPublicationAttempts)
          rebuildAndPublish(request, record, refreshes + 1)
        else if (beforeReport.toSet != beforePublish.toSet)
          F.raiseError[AnalyticsLateFactReplayOutcome](AnalyticsError.LateFactReplayRejected)
        else if (beforePublish.nonEmpty) F.pure(AnalyticsLateFactReplayOutcome.ErasurePending)
        else {
          val expiresAt = observedAt.plusSeconds(publishedSnapshotDays.toLong * 86400L)
          reportPublisher
            .publish(record.reservation, report, expiresAt)
            .as(AnalyticsLateFactReplayOutcome.Published)
            .handleErrorWith {
              case AnalyticsError.GuardedErasurePublicationRejected =>
                F.pure(AnalyticsLateFactReplayOutcome.ErasurePending)
              case error => F.raiseError(error)
            }
            .flatTap {
              case AnalyticsLateFactReplayOutcome.Published => now.flatMap(journal.markPublished(request, _))
              case _                                        => F.unit
            }
        }
    } yield outcome

  private def newAttempt(
      request: AnalyticsLateFactReplayRequest,
      record: AnalyticsLateFactReplayRecord
  ): F[AnalyticsLateFactReplayOutcome] =
    if (record.publicationAttempt + 1 >= MaximumPublicationAttempts)
      F.raiseError(AnalyticsError.LateFactReplayRejected)
    else
      for {
        observedAt <- now
        reservation <- reserve(request, record.publicationAttempt + 1, observedAt)
        _ <- validateSelection(request, observedAt)
        next <- journal.advancePublicationAttempt(request, record.publicationAttempt, reservation, observedAt)
        _ <- validateRecord(request, next)
        _ <- F.raiseUnless(next.publicationAttempt == record.publicationAttempt + 1)(
          AnalyticsError.LateFactReplayRequestConflict
        )
        outcome <- processAttempt(request, next)
      } yield outcome

  private def validateSelection(request: AnalyticsLateFactReplayRequest, observedAt: Instant): F[Unit] =
    for {
      _ <- stages.validateHmacConfiguration
      tokens <- deletionMarkers.activeSubjectTokens
      _ <- stages.validateSelectedFacts(request, tokens, observedAt)
    } yield ()

  private def reserve(
      request: AnalyticsLateFactReplayRequest,
      attempt: Int,
      observedAt: Instant
  ): F[AnalyticsReportReservation] =
    F.fromEither(reservationIdentityFor(request, attempt)).flatMap { case (runId, fingerprint) =>
      reportPublisher.reservePinned(runId, fingerprint, observedAt)
    }

  private def validateRecord(request: AnalyticsLateFactReplayRequest, record: AnalyticsLateFactReplayRecord): F[Unit] =
    F.fromEither(reservationIdentityFor(request, record.publicationAttempt)).flatMap { case (runId, fingerprint) =>
      F.raiseUnless(
        record.selectionDigest == request.selectionDigest && record.publicationAttempt >= 0 &&
          record.publicationAttempt < MaximumPublicationAttempts && record.reservation.runId == runId &&
          record.reservation.rangeFingerprint == fingerprint && record.reservation.generation >= 0L &&
          record.reservation.revision >= 0L
      )(AnalyticsError.LateFactReplayRequestConflict)
    }
}

private[analytics] object AnalyticsLateFactReplayService {
  val MaximumPublicationAttempts: Int = 3

  def reservationIdentityFor(
      request: AnalyticsLateFactReplayRequest,
      attempt: Int
  ): Either[AnalyticsError, (RunId, RangeFingerprint)] =
    for {
      runId <- RunId
        .from(s"late-replay-${request.requestId.value}-$attempt")
        .leftMap(_ => AnalyticsError.InvalidConfiguration("late-fact replay request identity is invalid"))
      fingerprint <- RangeFingerprint
        .from(request.selectionDigest)
        .leftMap(_ => AnalyticsError.InvalidConfiguration("late-fact replay selection digest is invalid"))
    } yield runId -> fingerprint
}
