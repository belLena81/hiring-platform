package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.kafka.TransactionalProducerFencer
import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.AnalyticsDigest
import com.example.hiring.analytics.domain.RangeFingerprint
import com.example.hiring.analytics.domain.RunId
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.batch.AnalyticsReportPublisher
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation
import com.example.hiring.analytics.service.erasure.ErasureBarrier
import com.example.hiring.analytics.service.erasure.ErasureClaim
import com.example.hiring.analytics.service.erasure.ErasureFailurePolicy
import com.example.hiring.analytics.service.erasure.ErasurePhase
import com.example.hiring.analytics.service.erasure.ErasureProgress
import com.example.hiring.analytics.service.erasure.ErasureQueue
import com.example.hiring.analytics.service.erasure.ErasureUpdate
import com.example.hiring.analytics.service.erasure.KafkaRetention
import com.example.hiring.analytics.service.erasure.KafkaRetentionBarrier

import cats.effect.{Async, Clock, Temporal}
import cats.Monad
import cats.syntax.all.*
import io.github.iltotore.iron.*
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.duration.*

/** Resumable erasure lifecycle. A marker stays active until both replay horizons have passed. */
final class AnalyticsErasureWorker[F[_]: Async](
    spark: SparkSession,
    queue: ErasureQueue[F],
    progress: ErasureProgress[F],
    barriers: ErasureBarrier[F],
    kafka: KafkaConnection,
    fencerKafka: KafkaConnection,
    topic: String,
    paths: AnalyticsLakehousePaths,
    publisher: AnalyticsReportPublisher[F],
    markers: ActiveDeletionMarkerSource[F],
    lakehouse: AnalyticsErasureLakehouse[F],
    lakehouseLock: AnalyticsLakehouseLock[F],
    clock: Clock[F],
    logger: Logger[F],
    producerFencer: TransactionalProducerFencer[F],
    kafkaRetention: KafkaRetention[F],
    retention: AnalyticsRetentionSettings,
    leaseDuration: FiniteDuration = 90.seconds,
    deliveryTimeout: FiniteDuration = 30.seconds,
    pollInterval: FiniteDuration = 5.seconds
) {
  private def now: F[Instant] = clock.realTimeInstant
  private def leaseUntil: F[Instant] = now.map(_.plusMillis(leaseDuration.toMillis))

  def run: F[Unit] =
    for {
      _ <- queue.preflight
      _ <- lakehouse.validateHmacConfiguration(spark)
      _ <- kafkaRetention.capture(kafka, topic)
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
        logger.info("claimed analytics erasure request") *> runClaim(claim)
      )
    } yield ()).foreverM

  private[analytics] def runClaim(claim: ErasureClaim): F[Unit] = {
    val work = for {
      current <- now
      runId <- Async[F].fromEither(
        RunId
          .from("analytics-erasure-" + claim.requestId.value)
          .toEither
          .leftMap(_ => AnalyticsError.InvalidConfiguration("invalid erasure run id"))
      )
      fingerprint <- Async[F].fromEither(
        RangeFingerprint
          .from(sha256("analytics-erasure:" + claim.requestId.value))
          .leftMap(_ => AnalyticsError.InvalidConfiguration("invalid erasure range fingerprint"))
      )
      reservation <- publisher.reserve(runId, fingerprint, current)
      _ <- process(claim, reservation)
    } yield ()
    withLease(claim)(work).handleErrorWith(handleClaimError(claim))
  }

  private[analytics] def withLease(claim: ErasureClaim)(work: F[Unit]): F[Unit] =
    Async[F]
      .background(renewForever(claim))
      .use(heartbeat =>
        Async[F]
          .race(work, heartbeat.flatMap(_.embedNever))
          .flatMap {
            case Left(_)  => Async[F].unit
            case Right(_) => Async[F].raiseError(AnalyticsError.ErasureNotReady)
          }
      )

  private def handleClaimError(claim: ErasureClaim)(error: Throwable): F[Unit] =
    error match {
      case AnalyticsError.ErasureDeferred => Async[F].unit
      case AnalyticsError.ErasureNotReady =>
        now.flatMap(progress.releaseForOtherRequests(claim, _)).flatMap(requireApplied)
      case failure => recordFailure(claim, failure)
    }

  private def recordFailure(claim: ErasureClaim, error: Throwable): F[Unit] = {
    val nextAttempt = claim.attemptCount + 1
    val decision = ErasureFailurePolicy.decide(error, nextAttempt)
    now.flatMap { current =>
      val scheduled = decision.retryAfter.map(delay => current.plusMillis(delay.toMillis))
      progress.recordFailure(claim, decision.category, nextAttempt, scheduled, current).flatMap {
        case ErasureUpdate.Applied =>
          logger.error(
            "analytics erasure failed; persisted category=" + decision.category.persistedName + ", attempt=" + nextAttempt
          )
        case ErasureUpdate.LeaseLost =>
          logger.warn("analytics erasure failure could not be recorded because the lease is no longer owned")
      }
    }
  }

  private[analytics] def renewForever(claim: ErasureClaim): F[Nothing] =
    (Temporal[F].sleep(leaseDuration / 3) *> (now, leaseUntil).tupled.flatMap { case (current, until) =>
      progress.renew(claim, current, until).flatMap {
        case ErasureUpdate.LeaseLost => Async[F].raiseError(AnalyticsError.ErasureNotReady)
        case ErasureUpdate.Applied   => queue.heartbeat(current, until)
      }
    }).foreverM

  private[analytics] def process(claim: ErasureClaim, reservation: AnalyticsReportReservation): F[Unit] =
    Monad[F].tailRecM(claim)(current => step(current, reservation))

  private def step(
      claim: ErasureClaim,
      reservation: AnalyticsReportReservation
  ): F[Either[ErasureClaim, Unit]] =
    claim.phase match {
      case ErasurePhase.Requested =>
        for {
          next <- Async[F].fromEither(
            claim.advanceTo(ErasurePhase.PublisherDrained).leftMap(_ => AnalyticsError.ErasureNotReady)
          )
          transactionalIds <- queue.transactionalIds(claim.requestId)
          _ <- producerFencer.fence(fencerKafka, transactionalIds)
          current <- now
          ready <- queue.publisherDrainReady(claim.requestId, current, deliveryTimeout)
          _ <- if (ready) Async[F].unit else defer(claim, pollInterval)
          _ <- advance(claim, ErasurePhase.PublisherDrained)
        } yield Left(next)

      case ErasurePhase.PublisherDrained =>
        for {
          next <- Async[F].fromEither(
            claim.advanceTo(ErasurePhase.OutboxPurged).leftMap(_ => AnalyticsError.ErasureNotReady)
          )
          current <- now
          purged <- queue.purgeOutbox(claim.requestId, current, deliveryTimeout)
          _ <- if (purged) Async[F].unit else defer(claim, pollInterval)
          barrier <- barriers.readBarrier(claim.requestId).flatMap {
            case Some(value) => Async[F].fromEither(KafkaRetentionBarrier.validate(value))
            case None        =>
              kafkaRetention
                .capture(kafka, topic)
                .flatTap(value =>
                  now
                    .flatMap(barriers.persistBarrier(claim, value, _))
                    .flatMap(requireApplied)
                )
          }
          _ <- Async[F].fromEither(KafkaRetentionBarrier.validate(barrier))
          _ <- advance(claim, ErasurePhase.OutboxPurged)
        } yield Left(next)

      case ErasurePhase.OutboxPurged =>
        for {
          next <- Async[F].fromEither(
            claim.advanceTo(ErasurePhase.DeltaPurged).leftMap(_ => AnalyticsError.ErasureNotReady)
          )
          _ <- purgeAndRecordDelta(claim, spark, reservation.generation)
          _ <- advance(claim, ErasurePhase.DeltaPurged)
        } yield Left(next)

      case ErasurePhase.DeltaPurged =>
        for {
          next <- Async[F].fromEither(
            claim.advanceTo(ErasurePhase.GoldRebuilt).leftMap(_ => AnalyticsError.ErasureNotReady)
          )
          barrier <- barriers
            .readBarrier(claim.requestId)
            .flatMap(
              _.fold[F[KafkaRetentionBarrier]](
                Async[F].raiseError(AnalyticsError.MalformedMarker)
              )(Async[F].pure)
            )
          purgedAt <- progress
            .readDeltaPurgedAt(claim.requestId)
            .flatMap(
              _.fold[F[Instant]](
                Async[F].raiseError(AnalyticsError.MalformedMarker)
              )(Async[F].pure)
            )
          ready <- replayHorizonsPassed(barrier, purgedAt)
          _ <- if (ready) Async[F].unit else defer(claim, 5.minutes)
          markerFrame <- markers.activeSubjectTokens(spark)
          _ <- lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- lakehouse.reclaimRetainedFiles(spark)
              _ <- lakehouse.verifyMarkedSubjectsAbsent(spark, markerFrame)
            } yield ()
          }
          affectedRows <- progress.readAffectedRows(claim.requestId)
          affectedFiles <- progress.readDeltaFiles(claim.requestId)
          _ <- Async[F].raiseWhen(affectedRows > 0L && affectedFiles.isEmpty)(
            AnalyticsError.PhysicalReclamationUnverified
          )
          _ <- lakehouse.verifyFilesAbsent(spark, affectedFiles)
          _ <- advance(claim, ErasurePhase.GoldRebuilt)
        } yield Left(next)

      case ErasurePhase.GoldRebuilt => advance(claim, ErasurePhase.ReadyToPublish).as(Right(()))

      case ErasurePhase.ReadyToPublish =>
        for {
          otherWork <- queue.hasNonReadyOtherRequests(claim.requestId)
          _ <- Async[F].raiseWhen(otherWork)(AnalyticsError.ErasureNotReady)
          current <- now
          refreshed <- publisher.reserve(reservation.runId, reservation.rangeFingerprint, current)
          deltaGeneration <- progress.readDeltaGeneration(claim.requestId)
          _ <-
            if (deltaGeneration.contains(refreshed.generation)) Async[F].unit
            else refreshErasureProjection(claim, refreshed.generation)
          markerFrame <- markers.activeSubjectTokens(spark)
          report <- lakehouseLock.resource(paths.root).use { _ =>
            for {
              _ <- lakehouse.verifyMarkedSubjectsAbsent(spark, markerFrame)
              asOf <- now
              result <- lakehouse.rebuildGoldAndExtractReport(spark, asOf)
            } yield result
          }
          completedAt <- now
          expiry = completedAt.plusSeconds(retention.publishedSnapshotDays.value.toLong * 86400L)
          _ <- publisher.publishErasure(refreshed, report, expiry, claim, completedAt)
          _ <- logger.info("analytics erasure completed and the snapshot was safely revealed")
        } yield Right(())

      case ErasurePhase.ReportPublished => Async[F].pure(Right(()))
    }

  private def purgeAndRecordDelta(claim: ErasureClaim, spark: SparkSession, generation: Long): F[Unit] =
    for {
      _ <- lakehouseLock.resource(paths.root).use { _ =>
        for {
          markerFrame <- markers.activeSubjectTokens(spark)
          affectedRows <- lakehouse.countMarkedRows(spark, markerFrame)
          affectedFiles <- lakehouse.captureMarkedFiles(spark, markerFrame)
          countedAt <- now
          counted <- progress.persistAffectedRows(claim, affectedRows, countedAt)
          _ <- requireApplied(counted)
          filesSaved <- progress.persistDeltaFiles(claim, affectedFiles, countedAt)
          _ <- requireApplied(filesSaved)
          _ <- lakehouse.applyDeletionMarkers(spark, markerFrame)
          retiredLogs <- lakehouse.checkpointPurgedRawLogs(spark)
          checkpointedAt <- now
          logsSaved <- progress.persistDeltaFiles(claim, retiredLogs, checkpointedAt)
          _ <- requireApplied(logsSaved)
        } yield ()
      }
      purgedAt <- now
      savedAt <- progress.persistDeltaPurgedAt(claim, purgedAt, purgedAt)
      _ <- requireApplied(savedAt)
      savedGeneration <- progress.persistDeltaGeneration(claim, generation, purgedAt)
      _ <- requireApplied(savedGeneration)
    } yield ()

  private def refreshErasureProjection(claim: ErasureClaim, generation: Long): F[Unit] =
    for {
      _ <- purgeAndRecordDelta(claim, spark, generation)
      barrier <- barriers
        .readBarrier(claim.requestId)
        .flatMap(
          _.fold[F[KafkaRetentionBarrier]](
            Async[F].raiseError(AnalyticsError.MalformedMarker)
          )(Async[F].pure)
        )
      purgedAt <- progress
        .readDeltaPurgedAt(claim.requestId)
        .flatMap(
          _.fold[F[Instant]](
            Async[F].raiseError(AnalyticsError.MalformedMarker)
          )(Async[F].pure)
        )
      ready <- replayHorizonsPassed(barrier, purgedAt)
      _ <- if (ready) Async[F].unit else defer(claim, 5.minutes)
      currentMarkers <- markers.activeSubjectTokens(spark)
      _ <- lakehouseLock.resource(paths.root).use { _ =>
        lakehouse.reclaimRetainedFiles(spark) *> lakehouse.verifyMarkedSubjectsAbsent(spark, currentMarkers)
      }
      allAffectedRows <- progress.readAffectedRows(claim.requestId)
      allAffectedFiles <- progress.readDeltaFiles(claim.requestId)
      _ <- Async[F].raiseWhen(allAffectedRows > 0L && allAffectedFiles.isEmpty)(
        AnalyticsError.PhysicalReclamationUnverified
      )
      _ <- lakehouse.verifyFilesAbsent(spark, allAffectedFiles)
    } yield ()

  private def replayHorizonsPassed(barrier: KafkaRetentionBarrier, deltaPurgedAt: Instant): F[Boolean] =
    for {
      current <- now
      kafkaExpired <- kafkaRetention.retentionPassed(kafka, barrier)
      dataDeadline = deltaPurgedAt.plus(java.time.Duration.ofDays(retention.deltaVacuumSafetyDays.value.toLong))
      logDeadline = deltaPurgedAt.plus(java.time.Duration.ofDays(retention.deltaLogRetentionDays.value.toLong))
      deltaDeadline = if (dataDeadline.isAfter(logDeadline)) dataDeadline else logDeadline
      deltaExpired = !current.isBefore(deltaDeadline)
    } yield kafkaExpired && deltaExpired

  private def defer(claim: ErasureClaim, delay: FiniteDuration): F[Unit] =
    now.flatMap { current =>
      progress.defer(claim, current.plusMillis(delay.toMillis), current).flatMap {
        case ErasureUpdate.Applied   => Async[F].raiseError(AnalyticsError.ErasureDeferred)
        case ErasureUpdate.LeaseLost => Async[F].raiseError(AnalyticsError.ErasureNotReady)
      }
    }

  private def advance(claim: ErasureClaim, phase: ErasurePhase): F[Unit] =
    now.flatMap(progress.advance(claim, phase, 0, _)).flatMap {
      case ErasureUpdate.Applied   => Async[F].unit
      case ErasureUpdate.LeaseLost => Async[F].raiseError(AnalyticsError.ErasureNotReady)
    }

  private def requireApplied(result: ErasureUpdate): F[Unit] = result match {
    case ErasureUpdate.Applied   => Async[F].unit
    case ErasureUpdate.LeaseLost => Async[F].raiseError(AnalyticsError.ErasureNotReady)
  }

  private def sha256(value: String): String =
    AnalyticsDigest.sha256Hex(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
}
