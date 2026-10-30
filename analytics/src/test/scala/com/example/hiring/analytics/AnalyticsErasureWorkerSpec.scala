package com.example.hiring.analytics

import com.example.hiring.analytics.config.{
  AnalyticsErasureWorkerPolicy,
  AnalyticsErasureWorkerTimings,
  KafkaConnection
}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{IO, Outcome, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger

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

  test("AEC-06.1 a poll with unfinished requests logs aggregate backlog counts and age without any identifier") {
    val backlog = ErasureBacklog(
      pending = 1L,
      processing = 0L,
      oldestAge = Some(3.days),
      oldestPhase = Some(ErasurePhase.Requested)
    )
    for {
      state <- Ref.of[IO, Counters](Counters())
      lines <- Ref.of[IO, Vector[String]](Vector.empty)
      worker = workerForRetention(
        AnalyticsTestOperationalConfig.operational.retention,
        0L,
        state,
        backlog = backlog,
        logger = capturingLogger(lines)
      )
      _ <- TestControl.executeEmbed {
        worker.pollForever.start.flatMap(fiber => IO.sleep(1.second) *> fiber.cancel)
      }
      captured <- lines.get
      backlogLines = captured.filter(_.startsWith("erasure backlog:"))
      _ = assert(backlogLines.nonEmpty, clues(captured))
      line = backlogLines.head
      _ = assert(line.contains("pending=1"), clue(line))
      hours = "oldestRequestedAgeHours=(\\d+)".r.findFirstMatchIn(line).map(_.group(1).toLong)
      _ = assert(hours.exists(_ >= 72L), clue(line))
      _ = assert(line.contains("phase=Requested"), clue(line))
      uuid = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r
      _ = assert(uuid.findFirstIn(line).isEmpty, clue(line))
    } yield ()
  }

  test("an idle queue logs no backlog line") {
    for {
      state <- Ref.of[IO, Counters](Counters())
      lines <- Ref.of[IO, Vector[String]](Vector.empty)
      worker = workerForRetention(
        AnalyticsTestOperationalConfig.operational.retention,
        0L,
        state,
        logger = capturingLogger(lines)
      )
      _ <- TestControl.executeEmbed {
        worker.pollForever.start.flatMap(fiber => IO.sleep(3.seconds) *> fiber.cancel)
      }
      captured <- lines.get
      _ = assertEquals(captured.filter(_.startsWith("erasure backlog:")), Vector.empty[String])
    } yield ()
  }

  private def backlogLinesAfter(sleep: FiniteDuration, backlog: IO[ErasureBacklog]): IO[(Vector[String], Counters)] =
    for {
      state <- Ref.of[IO, Counters](Counters())
      lines <- Ref.of[IO, Vector[String]](Vector.empty)
      worker = workerForRetention(
        AnalyticsTestOperationalConfig.operational.retention,
        0L,
        state,
        logger = capturingLogger(lines),
        backlogQuery = Some(backlog)
      )
      _ <- TestControl.executeEmbed {
        worker.pollForever.start.flatMap(fiber => IO.sleep(sleep) *> fiber.cancel)
      }
      captured <- lines.get
      counters <- state.get
    } yield (captured, counters)

  private val unchangedBacklog = ErasureBacklog(2L, 1L, Some(5.hours), Some(ErasurePhase.DeltaPurged))

  test("AEC-06.1 an unchanged backlog is logged once while polling continues, then again after the log interval") {
    for {
      (quick, counters) <- backlogLinesAfter(30.seconds, IO.pure(unchangedBacklog))
      _ = assertEquals(quick.count(_.startsWith("erasure backlog:")), 1, clues(quick))
      _ = assert(counters.claims >= 25, s"polling continued: ${counters.claims}")
      (later, _) <- backlogLinesAfter(90.minutes, IO.pure(unchangedBacklog))
      _ = assertEquals(later.count(_.startsWith("erasure backlog:")), 2, clues(later))
    } yield ()
  }

  test("AEC-06.1 a changed backlog summary is logged immediately") {
    for {
      calls <- Ref.of[IO, Int](0)
      backlog = calls
        .getAndUpdate(_ + 1)
        .map(call => if (call < 3) unchangedBacklog else unchangedBacklog.copy(pending = 3L))
      (lines, _) <- backlogLinesAfter(10.seconds, backlog)
      _ = assertEquals(lines.count(_.startsWith("erasure backlog:")), 2, clues(lines))
      _ = assert(lines.exists(_.contains("pending=3")), clues(lines))
    } yield ()
  }

  test("AEC-06.1 a failing backlog query logs a sanitized warning and the poll loop continues") {
    val failure = new IllegalStateException("password=hunter2 credential leak")
    for {
      (lines, counters) <- backlogLinesAfter(5.seconds, IO.raiseError(failure))
      warnings = lines.filter(_.startsWith("erasure backlog could not be read"))
      _ = assert(warnings.nonEmpty, clues(lines))
      _ = assert(lines.forall(!_.contains("hunter2")), clues(lines))
      _ = assert(counters.claims >= 4, s"the loop survived the failing query: ${counters.claims}")
    } yield ()
  }

  test("AEC-02.5 the worker logs its worst-case completion and budget once at start") {
    val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 1L)).toOption.get
    for {
      state <- Ref.of[IO, Counters](Counters())
      lines <- Ref.of[IO, Vector[String]](Vector.empty)
      retention = AnalyticsTestOperationalConfig.operational.retention
      worker = workerForRetention(
        retention,
        0L,
        state,
        logger = capturingLogger(lines),
        barrierCapture = Some(IO.pure(barrier))
      )
      _ <- TestControl.executeEmbed {
        worker.run.start.flatMap(fiber => IO.sleep(2.seconds) *> fiber.cancel)
      }
      captured <- lines.get
      starts = captured.filter(_.startsWith("analytics erasure worker starting:"))
      _ = assertEquals(starts.size, 1, clues(captured))
      _ = assert(starts.head.contains("worstCaseCompletion=" + retention.worstCaseErasureCompletion), clues(starts))
      _ = assert(starts.head.contains("erasureCompletionBudget=" + retention.erasureCompletionBudget), clues(starts))
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
    val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 12L)).toOption.get
    val purgedAt = Instant.parse("1969-12-31T23:57:59Z")
    val horizons = AnalyticsTestOperationalConfig.operational.retention.copy(
      deltaVacuumSafety = 1.minute,
      deltaLogRetention = 2.minutes
    )
    for {
      state <- Ref.of[IO, Counters](Counters())
      beforeBothDeltaDeadlines <- TestControl.executeEmbed {
        IO.sleep(70.seconds) *> workerForRetention(horizons, 12L, state)
          .replayHorizonsPassed(barrier, purgedAt)
      }
      beforeKafkaBarrier <- TestControl.executeEmbed {
        IO.sleep(130.seconds) *> workerForRetention(horizons, 11L, state)
          .replayHorizonsPassed(barrier, purgedAt)
      }
      afterAllGates <- TestControl.executeEmbed {
        IO.sleep(130.seconds) *> workerForRetention(horizons, 12L, state)
          .replayHorizonsPassed(barrier, purgedAt)
      }
      _ = assertEquals(beforeBothDeltaDeadlines, false)
      _ = assertEquals(beforeKafkaBarrier, false)
      _ = assertEquals(afterAllGates, true)
    } yield ()
  }

  test("AC-04.4 the worker holds the Delta horizon until the later of the purge and a following OPTIMIZE") {
    val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 12L)).toOption.get
    val horizons = AnalyticsTestOperationalConfig.operational.retention.copy(
      deltaVacuumSafety = 2.days,
      deltaLogRetention = 1.minute
    )
    val purgedAt = Instant.EPOCH.plusSeconds(12.hours.toSeconds)
    val optimizedAt = Instant.EPOCH.plusSeconds(30.hours.toSeconds)
    def passedAt(at: FiniteDuration, compaction: Option[Instant]): IO[Boolean] =
      Ref.of[IO, Counters](Counters()).flatMap { state =>
        TestControl.executeEmbed(
          IO.sleep(at) *> workerForRetention(horizons, 12L, state, lastCompaction = compaction)
            .replayHorizonsPassed(barrier, purgedAt, compaction)
        )
      }
    for {
      // 12h purge + 2d = 60h without an OPTIMIZE; 30h OPTIMIZE + 2d = 78h with one.
      withoutOptimize <- passedAt(60.hours, None)
      withOptimize <- passedAt(60.hours + 1.minute, Some(optimizedAt))
      beforeOptimizeHorizon <- passedAt(78.hours - 1.nano, Some(optimizedAt))
      atOptimizeHorizon <- passedAt(78.hours, Some(optimizedAt))
      earlierOptimize <- passedAt(60.hours, Some(Instant.EPOCH.plusSeconds(1.hour.toSeconds)))
      _ = assertEquals(withoutOptimize, true)
      _ = assertEquals(withOptimize, false)
      _ = assertEquals(beforeOptimizeHorizon, false)
      _ = assertEquals(atOptimizeHorizon, true)
      _ = assertEquals(earlierOptimize, true)
    } yield ()
  }

  Vector(
    ("data deadline dominates", 12.hours, 2.days, 1.minute, 60.hours),
    ("log deadline dominates", 12.hours, 1.minute, 2.minutes, 1.day + 2.minutes),
    ("purge just before UTC midnight", 1.day - 1.second, 1.second, 1.minute, 1.day + 1.minute),
    ("purge exactly at UTC midnight", 1.day, 1.second, 1.minute, 2.days + 1.minute)
  ).foreach { case (label, purgeOffset, dataRetention, logRetention, deadline) =>
    test(s"retention gate boundaries: $label") {
      val barrier = KafkaRetentionBarrier.from("hiring.operational-events", Vector(0 -> 12L)).toOption.get
      val horizons = AnalyticsTestOperationalConfig.operational.retention.copy(
        deltaVacuumSafety = dataRetention,
        deltaLogRetention = logRetention
      )
      val purgedAt = Instant.EPOCH.plusNanos(purgeOffset.toNanos)
      val observations = for {
        at <- Vector(deadline - 1.nano, deadline, deadline + 1.nano)
        earliest <- Vector(11L, 12L, 13L)
      } yield (at, earliest)
      observations.traverse_ { case (at, earliest) =>
        TestControl.executeEmbed {
          for {
            state <- Ref.of[IO, Counters](Counters())
            _ <- IO.sleep(at)
            passed <- workerForRetention(horizons, earliest, state).replayHorizonsPassed(barrier, purgedAt)
            _ = assertEquals(passed, at >= deadline && earliest >= 12L, clues(label, at, earliest))
          } yield ()
        }
      }
    }
  }

  private def capturingLogger(lines: Ref[IO, Vector[String]]): Logger[IO] = new Logger[IO] {
    private def add(message: => String): IO[Unit] = lines.update(_ :+ message)
    override def error(message: => String): IO[Unit] = add(message)
    override def warn(message: => String): IO[Unit] = add(message)
    override def info(message: => String): IO[Unit] = add(message)
    override def debug(message: => String): IO[Unit] = add(message)
    override def trace(message: => String): IO[Unit] = add(message)
    override def error(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def warn(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def info(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def debug(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def trace(t: Throwable)(message: => String): IO[Unit] = add(message)
  }

  private final case class Counters(claims: Int = 0, heartbeats: Int = 0, renewals: Int = 0)

  private final class TestErasureStore(
      state: Ref[IO, Counters],
      renewal: ErasureUpdate,
      renewalFailure: Option[Throwable],
      backlogQuery: IO[ErasureBacklog]
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
    override def markProducersFenced(requestId: AccountSubjectId, ids: Vector[String], now: Instant): IO[Unit] = IO.unit
    override def purgeOutbox(subjectId: AccountSubjectId, now: Instant, deliveryTimeout: FiniteDuration): IO[Boolean] =
      IO.pure(false)
    override def hasNonReadyOtherRequests(requestId: AccountSubjectId): IO[Boolean] = IO.pure(false)
    override def heartbeat(now: Instant, leaseUntil: Instant): IO[Unit] =
      state.update(value => value.copy(heartbeats = value.heartbeats + 1))
    override def preflight: IO[Unit] = IO.unit
    override def backlog(now: Instant): IO[ErasureBacklog] = backlogQuery

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
      renewalFailure: Option[Throwable] = None,
      backlog: ErasureBacklog = ErasureBacklog(0L, 0L, None, None),
      logger: Logger[IO] = org.typelevel.log4cats.slf4j.Slf4jLogger.getLogger[IO],
      backlogQuery: Option[IO[ErasureBacklog]] = None,
      barrierCapture: Option[IO[KafkaRetentionBarrier]] = None,
      lastCompaction: Option[Instant] = None
  ): AnalyticsErasureWorker[IO] = {
    val store = new TestErasureStore(state, renewal, renewalFailure, backlogQuery.getOrElse(IO.pure(backlog)))
    val lock: AnalyticsLakehouseLock[IO] = (_: String) => Resource.pure[IO, Unit](())
    val markers: ActiveDeletionMarkerSource[IO] = new ActiveDeletionMarkerSource[IO] {
      override def activeDeletionMarkers: IO[ActiveDeletionMarkers] = IO.pure(ActiveDeletionMarkers.none)
    }
    val lakehouse = new AnalyticsErasureLakehouse[IO] {
      override def validateHmacConfiguration: IO[Unit] = IO.unit
      override def reclaimRetainedFiles: IO[Long] = IO.pure(0L)
      override def lastCompactionAt: IO[Option[Instant]] = IO.pure(lastCompaction)
      override def verifyMarkedSubjectsAbsent(markerTokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def countMarkedRows(markerTokens: Vector[SubjectToken]): IO[Long] = IO.pure(0L)
      override def captureMarkedFiles(markerTokens: Vector[SubjectToken]): IO[Vector[String]] =
        IO.pure(Vector.empty)
      override def checkpointPurgedRawLogs: IO[Vector[String]] = IO.pure(Vector.empty)
      override def verifyFilesAbsent(files: Vector[String]): IO[Unit] = IO.unit
      override def applyDeletionMarkers(markerTokens: Vector[SubjectToken]): IO[Unit] = IO.unit
      override def rebuildGoldAndExtractReport(asOf: Instant): IO[AnalyticsReportOutput] =
        IO.pure(AnalyticsReportOutput(asOf, Vector.empty, None, Vector.empty, Vector.empty))
    }
    val publisher = new AnalyticsReportPublisher[IO] {
      override def publicationReceipt(
          reservation: AnalyticsReportReservation
      ): IO[AnalyticsReportPublicationReceipt] = IO.pure(AnalyticsReportPublicationReceipt.Absent)

      override def reservePinned(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        reserve(runId, rangeFingerprint, now)

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
      override def capture(): IO[KafkaRetentionBarrier] =
        barrierCapture.getOrElse(
          IO.raiseError(new AssertionError("polling and renewal tests do not capture retention barriers"))
        )
      override def retentionPassed(barrier: KafkaRetentionBarrier): IO[Boolean] =
        IO.pure(barrier.partitions.forall(_.endOffsetExclusive.asInstanceOf[Long] <= earliestOffset))
    }
    new AnalyticsErasureWorker[IO](
      store,
      store,
      store,
      AnalyticsErasureKafkaRuntime(KafkaConnection("localhost:9092"), noFencing, noRetention),
      TestAnalyticsLakehousePaths.unsafe("file:///tmp/analytics-erasure-worker-test"),
      publisher,
      markers,
      lakehouse,
      lock,
      logger,
      AnalyticsErasureWorkerPolicy(
        retention,
        AnalyticsErasureWorkerTimings(leaseDuration, 1.second, 1.second, 5.minutes, 1.hour)
      ),
      cats.effect.Clock[IO]
    )
  }
}
