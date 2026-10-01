package com.example.hiring.analytics

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.hiring.analytics.config.AnalyticsPositiveInt
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import munit.CatsEffectSuite

import java.time.Instant

final class AnalyticsLateFactReplayServiceSpec extends CatsEffectSuite {
  private val Now = Instant.parse("2026-09-30T12:00:00Z")
  private val Report = AnalyticsReportOutput(Now, Vector.empty, None, Vector.empty)

  test("selection rejection happens before journal preparation, merge, or publication") {
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      lock <- AnalyticsLakehouseLock.processLocal[IO].allocated.map(_._1)
      service = replayService(calls, lock, rejectSelection = true)
      request = replayRequest("replay-reject")
      result <- service.run(request).attempt
      observed <- calls.get
      _ = assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
      _ = assertEquals(observed, Vector("load", "hmac", "markers", "validate"))
    } yield ()
  }

  test("successful replay merges facts before guarded publication and journals completion") {
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      lock <- AnalyticsLakehouseLock.processLocal[IO].allocated.map(_._1)
      service = replayService(calls, lock)
      result <- service.run(replayRequest("replay-success"))
      observed <- calls.get
      _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
      _ = assertEquals(
        observed,
        Vector(
          "load",
          "hmac",
          "markers",
          "validate",
          "prepare",
          "markers",
          "hmac",
          "validate",
          "deletions",
          "merge",
          "facts-merged",
          "markers",
          "hmac",
          "deletions",
          "validate",
          "report",
          "markers",
          "reserve",
          "receipt",
          "publish",
          "published"
        )
      )
    } yield ()
  }

  test("retry reconciles a durable publication before requiring retained selected facts") {
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      lock <- AnalyticsLakehouseLock.processLocal[IO].allocated.map(_._1)
      service = replayService(calls, lock, reconcilePublished = true, rejectSelection = true)
      result <- service.run(replayRequest("replay-reconcile"))
      observed <- calls.get
      _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
      _ = assertEquals(observed, Vector("load", "reserve", "receipt", "published"))
    } yield ()
  }

  test("superseded publication retries under bounded fresh attempts") {
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      lock <- AnalyticsLakehouseLock.processLocal[IO].allocated.map(_._1)
      service = replayService(calls, lock, reconcileSuperseded = true)
      result <- service.run(replayRequest("replay-superseded")).attempt
      observed <- calls.get
      _ = assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
      _ = assertEquals(observed.count(_ == "advance"), 2)
      _ = assert(!observed.contains("published"))
    } yield ()
  }

  private def replayService(
      calls: Ref[IO, Vector[String]],
      lock: AnalyticsLakehouseLock[IO],
      rejectSelection: Boolean = false,
      reconcilePublished: Boolean = false,
      reconcileSuperseded: Boolean = false
  ): AnalyticsLateFactReplayService[IO] = {
    def record(value: String): IO[Unit] = calls.update(_ :+ value)
    val journal = new AnalyticsLateFactReplayJournal[IO] {
      override def load(requestId: AnalyticsReplayRequestId): IO[Option[AnalyticsLateFactReplayRecord]] =
        record("load").as(
          Option.when(reconcilePublished || reconcileSuperseded)(
            AnalyticsLateFactReplayRecord(
              replayRequest(requestId.value),
              AnalyticsLateFactReplayProgress.FactsMerged,
              0
            )
          )
        )
      override def prepare(
          request: AnalyticsLateFactReplayRequest,
          at: Instant
      ): IO[AnalyticsLateFactReplayRecord] =
        record("prepare").as(AnalyticsLateFactReplayRecord(request, AnalyticsLateFactReplayProgress.Prepared, 0))
      override def markFactsMerged(request: AnalyticsLateFactReplayRequest, at: Instant): IO[Unit] =
        record("facts-merged")
      override def advancePublicationAttempt(request: AnalyticsLateFactReplayRequest): IO[Int] =
        record("advance") *> calls.get.map(_.count(_ == "advance"))
      override def markPublished(request: AnalyticsLateFactReplayRequest, at: Instant): IO[Unit] = record("published")
    }
    val markers = new ActiveDeletionMarkerSource[IO] {
      override def activeSubjectTokens: IO[Vector[SubjectToken]] = record("markers").as(Vector.empty)
    }
    val stages = new AnalyticsLateFactReplayStages[IO] {
      override def validateHmacConfiguration: IO[Unit] = record("hmac")
      override def validateSelectedFacts(
          request: AnalyticsLateFactReplayRequest,
          activeTokens: Vector[SubjectToken],
          observedAt: Instant
      ): IO[Unit] =
        record("validate") *> (if (rejectSelection) IO.raiseError(AnalyticsError.LateFactReplayRejected) else IO.unit)
      override def applyActiveDeletions(activeTokens: Vector[SubjectToken]): IO[Unit] = record("deletions")
      override def mergeSelectedFacts(
          request: AnalyticsLateFactReplayRequest,
          activeTokens: Vector[SubjectToken],
          observedAt: Instant
      ): IO[Unit] = record("merge")
      override def rebuildGoldAndExtractReport(asOf: Instant): IO[AnalyticsReportOutput] = record("report").as(Report)
    }
    val publisher = new AnalyticsReportPublisher[IO] {
      override def reserve(
          runId: RunId,
          rangeFingerprint: RangeFingerprint,
          now: Instant
      ): IO[AnalyticsReportReservation] =
        record("reserve").as(AnalyticsReportReservation(runId, rangeFingerprint, 0L, 0L))
      override def publish(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant
      ): IO[Unit] = record("publish")
      override def publicationReceipt(reservation: AnalyticsReportReservation)(using
          cats.Applicative[IO]
      ): IO[AnalyticsReportPublicationReceipt] =
        record("receipt").as(
          if (reconcilePublished) AnalyticsReportPublicationReceipt.CurrentGeneration
          else if (reconcileSuperseded) AnalyticsReportPublicationReceipt.Superseded
          else AnalyticsReportPublicationReceipt.Absent
        )
      override def publishErasure(
          reservation: AnalyticsReportReservation,
          report: AnalyticsReportOutput,
          expiresAt: Instant,
          claim: com.example.hiring.analytics.service.erasure.ErasureClaim,
          completedAt: Instant
      ): IO[Unit] = IO.unit
    }
    new AnalyticsLateFactReplayService[IO](
      "test-lakehouse",
      journal,
      markers,
      stages,
      publisher,
      lock,
      30.asInstanceOf[AnalyticsPositiveInt],
      Some(IO.pure(Now))
    )
  }

  private def replayRequest(id: String): AnalyticsLateFactReplayRequest =
    AnalyticsLateFactReplayRequest.from(id, Vector(("hiring-events", 0, 10L))).toOption.get
}
