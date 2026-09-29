package com.example.hiring.analytics

import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{Clock, IO, Outcome, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite

import java.time.Instant
import scala.concurrent.duration.*

final class AnalyticsErasureWorkerSpec extends CatsEffectSuite {
  test("polling delays empty polls and remains cancellable under TestControl") {
    for {
      state <- Ref.of[IO, Counters](Counters())
      worker = testWorker(state, ErasureUpdate.Applied)
      result <- TestControl.executeEmbed {
        worker.pollForever.start.flatMap { fiber =>
          IO.sleep(3.seconds) *> fiber.cancel *> fiber.join.flatMap { outcome =>
            state.get.map(counters => (outcome, counters))
          }
        }
      }
      (outcome, counters) = result
      _ = assert(outcome match {
        case Outcome.Canceled() => true
        case _                  => false
      })
      _ = assert(counters.claims >= 3, s"expected repeated scheduled polls, saw ${counters.claims}")
    } yield ()
  }

  test("lease renewal is clock controlled, stops on lease loss, and can be cancelled") {
    for {
      applied <- Ref.of[IO, Counters](Counters())
      claim = ErasureClaim(
        AccountSubjectId.from("00000000-0000-0000-0000-000000000001").toOption.get,
        "lease",
        Instant.EPOCH,
        ErasurePhase.Requested,
        0,
        0L
      )
      appliedWorker = testWorker(applied, ErasureUpdate.Applied, leaseDuration = 3.seconds)
      cancellation <- TestControl.executeEmbed {
        appliedWorker.renewForever(claim).start.flatMap { fiber =>
          IO.sleep(1500.millis) *> fiber.cancel *> fiber.join.flatMap(outcome => applied.get.map(outcome -> _))
        }
      }
      (cancelledOutcome, appliedCounts) = cancellation
      _ = assert(cancelledOutcome match {
        case Outcome.Canceled() => true
        case _                  => false
      })
      _ = assertEquals(appliedCounts.renewals, 1)
      _ = assertEquals(appliedCounts.heartbeats, 1)
      lost <- Ref.of[IO, Counters](Counters())
      lostWorker = testWorker(lost, ErasureUpdate.LeaseLost, leaseDuration = 3.seconds)
      (lostOutcome, lostCounts) <- TestControl.executeEmbed {
        lostWorker.renewForever(claim).flatMap(outcome => lost.get.map(outcome -> _))
      }
      _ = assertEquals(lostOutcome, ErasureClaimOutcome.ReleasedForOthers)
      _ = assertEquals(lostCounts.renewals, 1)
      _ = assertEquals(lostCounts.heartbeats, 0)
    } yield ()
  }

  test("claim work owns its heartbeat and lease loss interrupts the work") {
    for {
      completedState <- Ref.of[IO, Counters](Counters())
      claim = ErasureClaim(
        AccountSubjectId.from("00000000-0000-0000-0000-000000000001").toOption.get,
        "lease",
        Instant.EPOCH,
        ErasurePhase.Requested,
        0,
        0L
      )
      completedWorker = testWorker(completedState, ErasureUpdate.Applied, leaseDuration = 3.seconds)
      completedCounts <- TestControl.executeEmbed {
        completedWorker.withLease(claim)(IO.pure(ErasureClaimOutcome.Completed)) *> IO.sleep(
          4.seconds
        ) *> completedState.get
      }
      _ = assertEquals(completedCounts.renewals, 0)
      _ = assertEquals(completedCounts.heartbeats, 0)
      lostState <- Ref.of[IO, Counters](Counters())
      lostWorker = testWorker(lostState, ErasureUpdate.LeaseLost, leaseDuration = 3.seconds)
      (lostOutcome, lostCounts) <- TestControl.executeEmbed {
        lostWorker.withLease(claim)(IO.never).flatMap(outcome => lostState.get.map(outcome -> _))
      }
      _ = assertEquals(lostOutcome, ErasureClaimOutcome.ReleasedForOthers)
      _ = assertEquals(lostCounts.renewals, 1)
      _ = assertEquals(lostCounts.heartbeats, 0)
      renewalFailure = new RuntimeException("renewal failed")
      failedRenewalState <- Ref.of[IO, Counters](Counters())
      failedRenewalWorker = testWorker(
        failedRenewalState,
        ErasureUpdate.Applied,
        leaseDuration = 3.seconds,
        renewalFailure = Some(renewalFailure)
      )
      (failedRenewalOutcome, failedRenewalCounts) <- TestControl.executeEmbed {
        failedRenewalWorker
          .withLease(claim)(IO.never)
          .attempt
          .flatMap(outcome => failedRenewalState.get.map(outcome -> _))
      }
      _ = assert(failedRenewalOutcome.left.toOption.exists(_ eq renewalFailure))
      _ = assertEquals(failedRenewalCounts.renewals, 1)
      _ = assertEquals(failedRenewalCounts.heartbeats, 0)
    } yield ()
  }

  test("erasure waits for both Delta deadlines and the broker earliest offset barrier") {
    val barrier = KafkaRetentionBarrier(
      "hiring.operational-events",
      Vector(KafkaRetentionBarrier.Partition(0, 12L))
    )
    val horizons = AnalyticsTestOperationalConfig.operational.retention.copy(
      deltaVacuumSafety = 1.minute,
      deltaLogRetention = 2.minutes
    )
    for {
      state <- Ref.of[IO, Counters](Counters())
      beforeBothDeltaDeadlines <- TestControl.executeEmbed {
        IO.sleep(70.seconds) *> workerForRetention(horizons, 12L, state)
          .replayHorizonsPassed(barrier, Instant.EPOCH)
      }
      beforeKafkaBarrier <- TestControl.executeEmbed {
        IO.sleep(130.seconds) *> workerForRetention(horizons, 11L, state)
          .replayHorizonsPassed(barrier, Instant.EPOCH)
      }
      afterAllGates <- TestControl.executeEmbed {
        IO.sleep(130.seconds) *> workerForRetention(horizons, 12L, state)
          .replayHorizonsPassed(barrier, Instant.EPOCH)
      }
      _ = assertEquals(beforeBothDeltaDeadlines, false)
      _ = assertEquals(beforeKafkaBarrier, false)
      _ = assertEquals(afterAllGates, true)
    } yield ()
  }

  private final case class Counters(claims: Int = 0, heartbeats: Int = 0, renewals: Int = 0)

  private final class TestErasureStore(
      state: Ref[IO, Counters],
      renewal: ErasureUpdate,
      renewalFailure: Option[Throwable]
  ) extends ErasureQueue[IO],
        ErasureProgress[IO],
        ErasureBarrier[IO] {
    override def claim(now: Instant, leaseUntil: Instant, limit: Int): IO[Vector[ErasureClaim]] =
      state.update(value => value.copy(claims = value.claims + 1)).as(Vector.empty)
    override def publisherDrainReady(
        subjectId: AccountSubjectId,
        now: Instant,
        deliveryTimeout: FiniteDuration
    ): IO[Boolean] =
      IO.pure(false)
    override def transactionalIds(requestId: AccountSubjectId): IO[Vector[String]] = IO.pure(Vector.empty)
    override def purgeOutbox(subjectId: AccountSubjectId, now: Instant, deliveryTimeout: FiniteDuration): IO[Boolean] =
      IO.pure(false)
    override def hasNonReadyOtherRequests(requestId: AccountSubjectId): IO[Boolean] = IO.pure(false)
    override def heartbeat(now: Instant, leaseUntil: Instant): IO[Unit] =
      state.update(value => value.copy(heartbeats = value.heartbeats + 1))
    override def preflight: IO[Unit] = IO.unit

    override def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): IO[ErasureUpdate] =
      IO.pure(ErasureUpdate.LeaseLost)
    override def readBarrier(requestId: AccountSubjectId): IO[Option[KafkaRetentionBarrier]] = IO.pure(None)

    override def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): IO[ErasureUpdate] =
      IO.pure(renewal)
    override def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): IO[ErasureUpdate] =
      IO.pure(renewal)
    override def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): IO[ErasureUpdate] =
      IO.pure(renewal)
    override def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): IO[ErasureUpdate] =
      IO.pure(renewal)
    override def readDeltaFiles(requestId: AccountSubjectId): IO[Vector[String]] = IO.pure(Vector.empty)
    override def readAffectedRows(requestId: AccountSubjectId): IO[Long] = IO.pure(0L)
    override def readDeltaGeneration(requestId: AccountSubjectId): IO[Option[Long]] = IO.pure(None)
    override def readDeltaPurgedAt(requestId: AccountSubjectId): IO[Option[Instant]] = IO.pure(None)
    override def releaseForOtherRequests(claim: ErasureClaim, now: Instant): IO[ErasureUpdate] = IO.pure(renewal)
    override def recordFailure(
        claim: ErasureClaim,
        category: ErasureFailureCategory,
        attempt: Int,
        retryAt: Option[Instant],
        now: Instant
    ): IO[ErasureUpdate] = IO.pure(renewal)
    override def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): IO[ErasureUpdate] = IO.pure(renewal)
    override def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): IO[ErasureUpdate] =
      state.update(value => value.copy(renewals = value.renewals + 1)) *> renewalFailure.fold(IO.pure(renewal))(
        IO.raiseError
      )
    override def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): IO[ErasureUpdate] =
      IO.pure(renewal)
  }

  private def testWorker(
      state: Ref[IO, Counters],
      renewal: ErasureUpdate,
      leaseDuration: FiniteDuration = 90.seconds,
      renewalFailure: Option[Throwable] = None
  ): AnalyticsErasureWorker[IO] = {
    workerForRetention(
      AnalyticsTestOperationalConfig.operational.retention,
      0L,
      state,
      renewal,
      leaseDuration,
      renewalFailure
    )
  }

  private def workerForRetention(
      retention: com.example.hiring.analytics.config.AnalyticsRetentionSettings,
      earliestOffset: Long,
      state: Ref[IO, Counters],
      renewal: ErasureUpdate = ErasureUpdate.Applied,
      leaseDuration: FiniteDuration = 90.seconds,
      renewalFailure: Option[Throwable] = None
  ): AnalyticsErasureWorker[IO] = {
    val store = new TestErasureStore(state, renewal, renewalFailure)
    val lock: AnalyticsLakehouseLock[IO] = (_: String) => Resource.pure[IO, Unit](())
    val markers: ActiveDeletionMarkerSource[IO] = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens: IO[Vector[SubjectToken]] = IO.pure(Vector.empty)
    }
    val lakehouse = new AnalyticsErasureLakehouse[IO] {
      override def validateHmacConfiguration: IO[Unit] = IO.unit
      override def reclaimRetainedFiles: IO[Long] = IO.pure(0L)
      override def verifyMarkedSubjectsAbsent(markerTokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def countMarkedRows(markerTokens: Vector[SubjectToken]): IO[Long] = IO.pure(0L)
      override def captureMarkedFiles(markerTokens: Vector[SubjectToken]): IO[Vector[String]] =
        IO.pure(Vector.empty)
      override def checkpointPurgedRawLogs: IO[Vector[String]] = IO.pure(Vector.empty)
      override def verifyFilesAbsent(files: Vector[String]): IO[Unit] = IO.unit
      override def checkpointRawTableLogs: IO[Unit] = IO.unit
      override def purgeMarkedSubjectRows(path: String, markerTokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def applyDeletionMarkers(markerTokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def rebuildGoldAndExtractReport(asOf: Instant): IO[AnalyticsReportOutput] =
        IO.pure(AnalyticsReportOutput(asOf, Vector.empty, None, Vector.empty))
    }
    val publisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        IO.raiseError(new AssertionError("polling and renewal tests do not reserve reports"))
      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] =
        IO.raiseError(new AssertionError("polling and renewal tests do not publish reports"))
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.raiseError(new AssertionError("polling and renewal tests do not publish erasure reports"))
    }
    val noFencing: TransactionalProducerFencer[IO] = (_: KafkaConnection, _: Vector[String]) => IO.unit
    val noRetention: KafkaRetention[IO] = new KafkaRetention[IO] {
      override def capture(connection: KafkaConnection, topic: String): IO[KafkaRetentionBarrier] =
        IO.raiseError(new AssertionError("polling and renewal tests do not capture retention barriers"))
      override def retentionPassed(connection: KafkaConnection, barrier: KafkaRetentionBarrier): IO[Boolean] =
        IO.pure(barrier.partitions.forall(_.endOffsetExclusive <= earliestOffset))
    }
    new AnalyticsErasureWorker[IO](
      store,
      store,
      store,
      KafkaConnection("localhost:9092"),
      KafkaConnection("localhost:9092"),
      "hiring.operational-events",
      AnalyticsLakehousePaths.unsafe("file:///tmp/analytics-erasure-worker-test"),
      publisher,
      markers,
      lakehouse,
      lock,
      Clock[IO],
      org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO],
      noFencing,
      noRetention,
      retention,
      leaseDuration,
      1.second,
      1.second
    )
  }
}
