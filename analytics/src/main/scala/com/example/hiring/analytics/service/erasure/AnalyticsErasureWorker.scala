package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.config.AnalyticsPositiveInt.value

import com.example.hiring.analytics.config.AnalyticsErasureWorkerPolicy
import com.example.hiring.analytics.domain.RangeFingerprint
import com.example.hiring.analytics.domain.RunId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.ActiveDeletionMarkerSource
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.batch.AnalyticsReportPublisher
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation

import cats.effect.{Async, Clock, Outcome, Temporal}
import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.duration.*

/** Resumable erasure lifecycle. A marker stays active until both replay horizons have passed. */
final class AnalyticsErasureWorker[F[_]: Async](
    queue: ErasureQueue[F],
    progress: ErasureProgress[F],
    barriers: ErasureBarrier[F],
    kafka: AnalyticsErasureKafkaRuntime[F],
    paths: AnalyticsLakehousePaths,
    publisher: AnalyticsReportPublisher[F],
    markers: ActiveDeletionMarkerSource[F],
    lakehouse: AnalyticsErasureLakehouse[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    logger: Logger[F],
    policy: AnalyticsErasureWorkerPolicy,
    clock: Clock[F]
) {
  private val retention = policy.retention
  private val leaseDuration = policy.timings.leaseDuration
  private val deliveryTimeout = policy.timings.deliveryTimeout
  private val pollInterval = policy.timings.pollInterval
  private val now = clock.realTimeInstant
  private def leaseUntil: F[Instant] = now.map(_.plusMillis(leaseDuration.toMillis))

  def run: F[Unit] =
    for {
      _ <- queue.preflight
      _ <- lakehouse.validateHmacConfiguration
      _ <- kafka.retention.capture()
      current <- now
      until <- leaseUntil
      _ <- queue.heartbeat(current, until)
      _ <- pollForever
    } yield ()

  private[analytics] def pollForever: F[Unit] =
    (for {
      current <- now
      until <- leaseUntil
      _ <- queue.heartbeat(current, until)
      claims <- queue.claim(current, until, limit = 1)
      _ <- claims.headOption.fold(Temporal[F].sleep(pollInterval))(claim =>
        logger.info("claimed analytics erasure request") *> runClaim(claim).void
      )
    } yield ()).foreverM

  private[analytics] def runClaim(claim: ErasureClaim): F[ErasureClaimOutcome] = {
    val work = for {
      current <- now
      runId = {
        import io.github.iltotore.iron.autoRefine
        RunId.prefixed("analytics-erasure-", claim.requestId.value)
      }
      fingerprint = RangeFingerprint.ofSha256("analytics-erasure:" + claim.requestId.value)
      reservation <- publisher.reserve(runId, fingerprint, current)
      outcome <- process(claim, reservation)
    } yield outcome
    withLease(claim)(work).attempt.flatMap {
      case Left(failure) => recordFailure(claim, failure).as(ErasureClaimOutcome.Failed(failure))
      case Right(ErasureClaimOutcome.ReleasedForOthers) =>
        now
          .flatMap(progress.releaseForOtherRequests(claim, _))
          .as(ErasureClaimOutcome.ReleasedForOthers)
      case Right(outcome) => Async[F].pure(outcome)
    }
  }

  private[analytics] def withLease(claim: ErasureClaim)(work: F[ErasureClaimOutcome]): F[ErasureClaimOutcome] =
    Async[F]
      .background(renewForever(claim))
      .use(heartbeat =>
        Async[F]
          .race(
            work,
            heartbeat.flatMap {
              case Outcome.Succeeded(result) => result
              case Outcome.Errored(error)    => Async[F].raiseError(error)
              case Outcome.Canceled()        => Async[F].canceled *> Async[F].pure(ErasureClaimOutcome.Completed)
            }
          )
          .map(_.merge)
      )

  private def recordFailure(claim: ErasureClaim, error: Throwable): F[Unit] = {
    val nextAttempt = claim.attemptCount + 1
    val decision = ErasureFailurePolicy.decide(error, nextAttempt)
    now.flatMap { current =>
      val scheduled = decision.retryAfter.map(delay => current.plusMillis(delay.toMillis))
      progress.recordFailure(claim, decision.category, nextAttempt, scheduled, current).flatMap {
        case ErasureUpdate.Applied =>
          logger.error(
            "analytics erasure failed; persisted category=" + decision.category.persistedName +
              ", errorCode=" + failureCode(error) + ", attempt=" + nextAttempt
          )
        case ErasureUpdate.LeaseLost =>
          logger.warn("analytics erasure failure could not be recorded because the lease is no longer owned")
      }
    }
  }

  /** Emits a stable, non-sensitive error code without logging exception messages or payloads. */
  private def failureCode(error: Throwable): String = error match {
    case _: AnalyticsError.InvalidConfiguration       => "INVALID_CONFIGURATION"
    case AnalyticsError.MalformedMarker               => "MALFORMED_MARKER"
    case AnalyticsError.InvalidSilverSchema           => "INVALID_SILVER_SCHEMA"
    case AnalyticsError.InvalidGoldSchema             => "INVALID_GOLD_SCHEMA"
    case AnalyticsError.PhysicalReclamationUnverified => "PHYSICAL_RECLAMATION_UNVERIFIED"
    case _: AnalyticsError                            => "ANALYTICS_ERROR"
    case _                                            => "UNEXPECTED_ERROR"
  }

  private[analytics] def renewForever(claim: ErasureClaim): F[ErasureClaimOutcome] =
    Async[F].tailRecM(()) { _ =>
      Temporal[F].sleep(leaseDuration / 3) *> (now, leaseUntil).tupled.flatMap { case (current, until) =>
        progress.renew(claim, current, until).flatMap {
          case ErasureUpdate.LeaseLost => Async[F].pure(Right(ErasureClaimOutcome.ReleasedForOthers))
          case ErasureUpdate.Applied   => queue.heartbeat(current, until).as(Left(()))
        }
      }
    }

  private[analytics] def process(
      claim: ErasureClaim,
      reservation: AnalyticsReportReservation
  ): F[ErasureClaimOutcome] =
    processFlow(claim, reservation).value.map(_.fold(identity, _ => ErasureClaimOutcome.Completed))

  private type ClaimFlow[A] = EitherT[F, ErasureClaimOutcome, A]

  private def processFlow(claim: ErasureClaim, reservation: AnalyticsReportReservation): ClaimFlow[Unit] =
    Monad[ClaimFlow].tailRecM(claim)(current => step(current, reservation))

  private def lift[A](effect: F[A]): ClaimFlow[A] = EitherT.liftF(effect)

  private def stop[A](outcome: ErasureClaimOutcome): ClaimFlow[A] = EitherT.leftT(outcome)

  private def fromEither[A](result: Either[AnalyticsError, A]): ClaimFlow[A] =
    lift(Async[F].fromEither(result))

  private def nextPhase(claim: ErasureClaim, phase: ErasurePhase): ClaimFlow[ErasureClaim] =
    claim.advanceTo(phase) match {
      case Right(next) => EitherT.rightT(next)
      case Left(_)     => stop(ErasureClaimOutcome.ReleasedForOthers)
    }

  private def step(
      claim: ErasureClaim,
      reservation: AnalyticsReportReservation
  ): ClaimFlow[Either[ErasureClaim, Unit]] =
    claim.phase match {
      case ErasurePhase.Requested =>
        for {
          next <- nextPhase(claim, ErasurePhase.PublisherDrained)
          transactionalIds <- lift(queue.transactionalIds(claim.requestId))
          _ <-
            if (transactionalIds.isEmpty) EitherT.rightT(())
            else
              lift(kafka.producerFencer.fence(kafka.fencerConnection, transactionalIds)) *>
                lift(now).flatMap(at => lift(queue.markProducersFenced(claim.requestId, transactionalIds, at))) *>
                defer(claim, pollInterval)
          current <- lift(now)
          ready <- lift(queue.publisherDrainReady(claim.requestId, current, deliveryTimeout))
          _ <- if (ready) EitherT.rightT(()) else defer(claim, pollInterval)
          _ <- advance(claim, ErasurePhase.PublisherDrained)
        } yield Left(next)

      case ErasurePhase.PublisherDrained =>
        for {
          next <- nextPhase(claim, ErasurePhase.OutboxPurged)
          current <- lift(now)
          purged <- lift(queue.purgeOutbox(claim.requestId, current, deliveryTimeout))
          _ <- if (purged) EitherT.rightT(()) else defer(claim, pollInterval)
          maybeBarrier <- lift(barriers.readBarrier(claim.requestId))
          barrier <- maybeBarrier match {
            case Some(value) => fromEither(KafkaRetentionBarrier.validate(value))
            case None        =>
              lift(kafka.retention.capture()).flatTap(value =>
                lift(now)
                  .flatMap(current => lift(barriers.persistBarrier(claim, value, current)))
                  .flatMap(requireApplied)
              )
          }
          _ <- fromEither(KafkaRetentionBarrier.validate(barrier))
          _ <- advance(claim, ErasurePhase.OutboxPurged)
        } yield Left(next)

      case ErasurePhase.OutboxPurged =>
        for {
          next <- nextPhase(claim, ErasurePhase.DeltaPurged)
          _ <- purgeAndRecordDelta(claim, reservation.generation)
          _ <- advance(claim, ErasurePhase.DeltaPurged)
        } yield Left(next)

      case ErasurePhase.DeltaPurged =>
        for {
          next <- nextPhase(claim, ErasurePhase.GoldRebuilt)
          barrierOption <- lift(barriers.readBarrier(claim.requestId))
          barrier <- lift(
            barrierOption.fold[F[KafkaRetentionBarrier]](
              Async[F].raiseError(AnalyticsError.MalformedMarker)
            )(Async[F].pure)
          )
          purgedAtOption <- lift(progress.readDeltaPurgedAt(claim.requestId))
          purgedAt <- lift(
            purgedAtOption.fold[F[Instant]](
              Async[F].raiseError(AnalyticsError.MalformedMarker)
            )(Async[F].pure)
          )
          ready <- lift(replayHorizonsPassed(barrier, purgedAt))
          _ <- if (ready) EitherT.rightT(()) else defer(claim, 5.minutes)
          markerTokens <- lift(markers.activeSubjectTokens)
          _ <- lift(lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- lakehouse.reclaimRetainedFiles
              _ <- lakehouse.verifyMarkedSubjectsAbsent(markerTokens)
            } yield ()
          })
          affectedRows <- lift(progress.readAffectedRows(claim.requestId))
          affectedFiles <- lift(progress.readDeltaFiles(claim.requestId))
          _ <-
            if (affectedRows > 0L && affectedFiles.isEmpty)
              lift(Async[F].raiseError[Unit](AnalyticsError.PhysicalReclamationUnverified))
            else EitherT.rightT(())
          _ <- lift(lakehouse.verifyFilesAbsent(affectedFiles))
          _ <- advance(claim, ErasurePhase.GoldRebuilt)
        } yield Left(next)

      case ErasurePhase.GoldRebuilt => advance(claim, ErasurePhase.ReadyToPublish).as(Right(()))

      case ErasurePhase.ReadyToPublish =>
        for {
          otherWork <- lift(queue.hasNonReadyOtherRequests(claim.requestId))
          _ <- if (otherWork) stop(ErasureClaimOutcome.ReleasedForOthers) else EitherT.rightT(())
          current <- lift(now)
          refreshed <- lift(publisher.reserve(reservation.runId, reservation.rangeFingerprint, current))
          deltaGeneration <- lift(progress.readDeltaGeneration(claim.requestId))
          _ <-
            if (deltaGeneration.contains(refreshed.generation)) EitherT.rightT(())
            else refreshErasureProjection(claim, refreshed.generation)
          markerTokens <- lift(markers.activeSubjectTokens)
          report <- lift(lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- lakehouse.verifyMarkedSubjectsAbsent(markerTokens)
              asOf <- now
              result <- lakehouse.rebuildGoldAndExtractReport(asOf)
            } yield result
          })
          completedAt <- lift(now)
          expiry = completedAt.plusSeconds(retention.publishedSnapshotDays.value.toLong * 86400L)
          _ <- lift(publisher.publishErasure(refreshed, report, expiry, claim, completedAt))
          _ <- lift(logger.info("analytics erasure completed and the snapshot was safely revealed"))
        } yield Right(())

      case ErasurePhase.ReportPublished => EitherT.rightT(Right(()))
    }

  private def purgeAndRecordDelta(claim: ErasureClaim, generation: Long): ClaimFlow[Unit] =
    for {
      lockedUpdates <- lift(lakehouseLock.resource(paths.root).use { _ =>
        for {
          markerTokens <- markers.activeSubjectTokens
          affectedRows <- lakehouse.countMarkedRows(markerTokens)
          affectedFiles <- lakehouse.captureMarkedFiles(markerTokens)
          countedAt <- now
          counted <- progress.persistAffectedRows(claim, affectedRows, countedAt)
          result <- counted match {
            case ErasureUpdate.LeaseLost => Async[F].pure(Left(ErasureClaimOutcome.ReleasedForOthers))
            case ErasureUpdate.Applied   =>
              for {
                filesSaved <- progress.persistDeltaFiles(claim, affectedFiles, countedAt)
                next <- filesSaved match {
                  case ErasureUpdate.LeaseLost => Async[F].pure(Left(ErasureClaimOutcome.ReleasedForOthers))
                  case ErasureUpdate.Applied   =>
                    for {
                      _ <- lakehouse.applyDeletionMarkers(markerTokens)
                      retiredLogs <- lakehouse.checkpointPurgedRawLogs
                      checkpointedAt <- now
                      logsSaved <- progress.persistDeltaFiles(claim, retiredLogs, checkpointedAt)
                    } yield logsSaved match {
                      case ErasureUpdate.Applied   => Right(())
                      case ErasureUpdate.LeaseLost => Left(ErasureClaimOutcome.ReleasedForOthers)
                    }
                }
              } yield next
          }
        } yield result
      })
      _ <- lockedUpdates.fold(stop, _ => EitherT.rightT(()))
      purgedAt <- lift(now)
      savedAt <- lift(progress.persistDeltaPurgedAt(claim, purgedAt, purgedAt))
      _ <- requireApplied(savedAt)
      savedGeneration <- lift(progress.persistDeltaGeneration(claim, generation, purgedAt))
      _ <- requireApplied(savedGeneration)
    } yield ()

  private def refreshErasureProjection(claim: ErasureClaim, generation: Long): ClaimFlow[Unit] =
    for {
      _ <- purgeAndRecordDelta(claim, generation)
      barrierOption <- lift(barriers.readBarrier(claim.requestId))
      barrier <- lift(
        barrierOption.fold[F[KafkaRetentionBarrier]](
          Async[F].raiseError(AnalyticsError.MalformedMarker)
        )(Async[F].pure)
      )
      purgedAtOption <- lift(progress.readDeltaPurgedAt(claim.requestId))
      purgedAt <- lift(
        purgedAtOption.fold[F[Instant]](
          Async[F].raiseError(AnalyticsError.MalformedMarker)
        )(Async[F].pure)
      )
      ready <- lift(replayHorizonsPassed(barrier, purgedAt))
      _ <- if (ready) EitherT.rightT(()) else defer(claim, 5.minutes)
      currentMarkers <- lift(markers.activeSubjectTokens)
      _ <- lift(lakehouseLock.resource(paths.root).use { _ =>
        lakehouse.reclaimRetainedFiles *> lakehouse.verifyMarkedSubjectsAbsent(currentMarkers)
      })
      allAffectedRows <- lift(progress.readAffectedRows(claim.requestId))
      allAffectedFiles <- lift(progress.readDeltaFiles(claim.requestId))
      _ <-
        if (allAffectedRows > 0L && allAffectedFiles.isEmpty)
          lift(Async[F].raiseError[Unit](AnalyticsError.PhysicalReclamationUnverified))
        else EitherT.rightT(())
      _ <- lift(lakehouse.verifyFilesAbsent(allAffectedFiles))
    } yield ()

  private[analytics] def replayHorizonsPassed(barrier: KafkaRetentionBarrier, deltaPurgedAt: Instant): F[Boolean] =
    for {
      current <- now
      kafkaExpired <- kafka.retention.retentionPassed(barrier)
      dataDeadline = deltaPurgedAt.plusMillis(retention.deltaVacuumSafety.toMillis)
      // Delta truncates the log-cleanup cutoff to the start of a UTC day.
      // A log written on purge day cannot be reclaimed until the next day plus retention.
      logDeadline = deltaPurgedAt
        .truncatedTo(java.time.temporal.ChronoUnit.DAYS)
        .plus(1L, java.time.temporal.ChronoUnit.DAYS)
        .plusMillis(retention.deltaLogRetention.toMillis)
      deltaDeadline = if (dataDeadline.isAfter(logDeadline)) dataDeadline else logDeadline
      deltaExpired = !current.isBefore(deltaDeadline)
    } yield kafkaExpired && deltaExpired

  private def defer(claim: ErasureClaim, delay: FiniteDuration): ClaimFlow[Unit] =
    lift(now).flatMap { current =>
      lift(progress.defer(claim, current.plusMillis(delay.toMillis), current)).flatMap {
        case ErasureUpdate.Applied   => stop(ErasureClaimOutcome.Deferred)
        case ErasureUpdate.LeaseLost => stop(ErasureClaimOutcome.ReleasedForOthers)
      }
    }

  private def advance(claim: ErasureClaim, phase: ErasurePhase): ClaimFlow[Unit] =
    lift(now).flatMap(current => lift(progress.advance(claim, phase, 0, current))).flatMap {
      case ErasureUpdate.Applied   => EitherT.rightT(())
      case ErasureUpdate.LeaseLost => stop(ErasureClaimOutcome.ReleasedForOthers)
    }

  private def requireApplied(result: ErasureUpdate): ClaimFlow[Unit] = result match {
    case ErasureUpdate.Applied   => EitherT.rightT(())
    case ErasureUpdate.LeaseLost => stop(ErasureClaimOutcome.ReleasedForOthers)
  }
}
