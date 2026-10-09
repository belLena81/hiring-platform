package com.example.hiring.analytics

import cats.effect.{IO, Ref}
import com.example.hiring.analytics.config.AnalyticsPositiveInt
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import munit.CatsEffectSuite

import java.time.Instant

final class AnalyticsLateFactReplayServiceSpec extends CatsEffectSuite {
  private val Now = Instant.parse("2026-09-30T12:00:00Z")
  private val Report = AnalyticsReportOutput(Now, Vector.empty, None, Vector.empty)
  private val request = replayRequest("replay-selected")

  test("invalid selection is rejected before journal preparation or fact/report writes") {
    fixture(rejectSelection = true).flatMap { value =>
      for {
        result <- value.service.run(request).attempt
        calls <- value.calls.get
        _ = assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
        _ = assertEquals(calls, Vector("load", "reserve", "hmac", "markers", "validate"))
        record <- value.state.get
        _ = assertEquals(record, None)
      } yield ()
    }
  }

  test("reservation is pinned before selection, merge, and report construction") {
    fixture().flatMap { value =>
      for {
        result <- value.service.run(request)
        calls <- value.calls.get
        _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
        _ = assert(calls.indexOf("reserve") < calls.indexOf("validate"))
        _ = assert(calls.indexOf("validate") < calls.indexOf("merge"))
        _ = assert(calls.indexOf("merge") < calls.indexOf("report"))
        _ = assertEquals(calls.count(_ == "reserve"), 1)
        second <- value.service.run(request)
        _ = assertEquals(second, AnalyticsLateFactReplayOutcome.AlreadyPublished)
      } yield ()
    }
  }

  test("publication committed before journal completion recovers without requiring expired source facts") {
    fixture(rejectSelection = true, publishedReceipt = true).flatMap { value =>
      for {
        _ <- value.state.set(Some(record(0, AnalyticsLateFactReplayProgress.FactsMerged)))
        result <- value.service.run(request)
        calls <- value.calls.get
        _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
        _ = assertEquals(calls, Vector("load", "receipt", "published"))
      } yield ()
    }
  }

  test("new selection cannot reuse an existing request ID") {
    fixture().flatMap { value =>
      for {
        _ <- value.state.set(Some(record(0, AnalyticsLateFactReplayProgress.Prepared)))
        changed = AnalyticsLateFactReplayRequest
          .from(request.requestId.value, Vector(("hiring-events", 0, 11L)))
          .toOption
          .get
        result <- value.service.run(changed).attempt
        calls <- value.calls.get
        _ = assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRequestConflict))
        _ = assertEquals(calls, Vector("load"))
      } yield ()
    }
  }

  test("superseded attempts allocate bounded new pinned reservations and stop after three attempts") {
    fixture(alwaysSuperseded = true).flatMap { value =>
      for {
        _ <- value.state.set(Some(record(0, AnalyticsLateFactReplayProgress.FactsMerged)))
        result <- value.service.run(request).attempt
        calls <- value.calls.get
        _ = assertEquals(result.left.toOption, Some(AnalyticsError.LateFactReplayRejected))
        _ = assertEquals(calls.count(_ == "advance"), 2)
        _ = assertEquals(calls.count(_ == "reserve"), 2)
        _ = assert(!calls.contains("publish"))
      } yield ()
    }
  }

  test("a superseded receipt rebuilds under a new reservation and never refreshes the old attempt") {
    fixture(supersedeFirstReceipt = true).flatMap { value =>
      for {
        _ <- value.state.set(Some(record(0, AnalyticsLateFactReplayProgress.FactsMerged)))
        result <- value.service.run(request)
        calls <- value.calls.get
        finalRecord <- value.state.get
        _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
        _ = assertEquals(calls.count(_ == "advance"), 1)
        _ = assertEquals(calls.count(_ == "reserve"), 1)
        _ = assertEquals(finalRecord.map(_.publicationAttempt), Some(1))
        _ = assert(calls.indexOf("reserve") < calls.indexOf("validate"))
      } yield ()
    }
  }

  test("deletion completes between report read and publication while markers are empty: pinned generation rejects") {
    fixture(deleteDuringReport = true).flatMap { value =>
      for {
        result <- value.service.run(request)
        calls <- value.calls.get
        finalRecord <- value.state.get
        _ = assertEquals(result, AnalyticsLateFactReplayOutcome.ErasurePending)
        _ = assertEquals(calls.count(_ == "reserve"), 1)
        _ = assert(!calls.contains("published"))
        _ = assertEquals(finalRecord.map(_.progress), Some(AnalyticsLateFactReplayProgress.FactsMerged))
        _ = assertEquals(finalRecord.map(_.reservation.generation), Some(0L))
      } yield ()
    }
  }

  test("an interrupted merge resumes using its persisted reservation") {
    fixture().flatMap { value =>
      for {
        _ <- value.state.set(Some(record(0, AnalyticsLateFactReplayProgress.Prepared)))
        result <- value.service.run(request)
        calls <- value.calls.get
        _ = assertEquals(result, AnalyticsLateFactReplayOutcome.Published)
        _ = assert(!calls.contains("reserve"))
        _ = assert(calls.contains("merge"))
      } yield ()
    }
  }

  private case class Fixture(
      service: AnalyticsLateFactReplayService[IO],
      calls: Ref[IO, Vector[String]],
      state: Ref[IO, Option[AnalyticsLateFactReplayRecord]]
  )

  private def fixture(
      rejectSelection: Boolean = false,
      publishedReceipt: Boolean = false,
      alwaysSuperseded: Boolean = false,
      supersedeFirstReceipt: Boolean = false,
      deleteDuringReport: Boolean = false
  ): IO[Fixture] =
    for {
      calls <- Ref.of[IO, Vector[String]](Vector.empty)
      state <- Ref.of[IO, Option[AnalyticsLateFactReplayRecord]](None)
      generation <- Ref.of[IO, Long](0L)
      lock <- AnalyticsTestLakehouseLocks.processLocal[IO].allocated.map(_._1)
    } yield {
      def trace(name: String): IO[Unit] = calls.update(_ :+ name)
      val journal = new AnalyticsLateFactReplayJournal[IO] {
        override def load(requestId: AnalyticsReplayRequestId): IO[Option[AnalyticsLateFactReplayRecord]] =
          trace("load") *> state.get
        override def prepare(
            request: AnalyticsLateFactReplayRequest,
            reservation: AnalyticsReportReservation,
            at: Instant
        ): IO[AnalyticsLateFactReplayRecord] = {
          val value = AnalyticsLateFactReplayRecord(
            request.selectionDigest,
            AnalyticsLateFactReplayProgress.Prepared,
            0,
            reservation
          )
          trace("prepare") *> state.set(Some(value)).as(value)
        }
        override def markFactsMerged(request: AnalyticsLateFactReplayRequest, at: Instant): IO[Unit] =
          trace("facts-merged") *> state.update(_.map(_.copy(progress = AnalyticsLateFactReplayProgress.FactsMerged)))
        override def advancePublicationAttempt(
            request: AnalyticsLateFactReplayRequest,
            expectedAttempt: Int,
            reservation: AnalyticsReportReservation,
            at: Instant
        ): IO[AnalyticsLateFactReplayRecord] = {
          val value = AnalyticsLateFactReplayRecord(
            request.selectionDigest,
            AnalyticsLateFactReplayProgress.Prepared,
            expectedAttempt + 1,
            reservation
          )
          trace("advance") *> state.set(Some(value)).as(value)
        }
        override def markPublished(request: AnalyticsLateFactReplayRequest, at: Instant): IO[Unit] =
          trace("published") *> state.update(_.map(_.copy(progress = AnalyticsLateFactReplayProgress.Published)))
      }
      val markers = new ActiveDeletionMarkerSource[IO] {
        override def activeSubjectTokens: IO[Vector[SubjectToken]] = trace("markers").as(Vector.empty)
      }
      val stages = new AnalyticsLateFactReplayStages[IO] {
        override def validateHmacConfiguration: IO[Unit] = trace("hmac")
        override def validateSelectedFacts(
            request: AnalyticsLateFactReplayRequest,
            activeTokens: Vector[SubjectToken],
            observedAt: Instant
        ): IO[Unit] = trace("validate") *>
          IO.raiseWhen(rejectSelection)(AnalyticsError.LateFactReplayRejected)
        override def applyActiveDeletions(activeTokens: Vector[SubjectToken]): IO[Unit] = trace("deletions")
        override def mergeSelectedFacts(
            request: AnalyticsLateFactReplayRequest,
            activeTokens: Vector[SubjectToken],
            observedAt: Instant
        ): IO[Unit] = trace("merge")
        override def rebuildGoldAndExtractReport(asOf: Instant): IO[AnalyticsReportOutput] = trace("report") *>
          (if (deleteDuringReport) generation.update(_ + 1L) else IO.unit).as(Report)
      }
      val publisher = new AnalyticsReportPublisher[IO] {
        override def reserve(
            runId: RunId,
            fingerprint: RangeFingerprint,
            now: Instant
        ): IO[AnalyticsReportReservation] =
          IO.raiseError(new AssertionError("replay must use reservePinned"))
        override def reservePinned(
            runId: RunId,
            fingerprint: RangeFingerprint,
            now: Instant
        ): IO[AnalyticsReportReservation] =
          trace("reserve") *> generation.get.map(current => AnalyticsReportReservation(runId, fingerprint, current, 1L))
        override def publish(
            reservation: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant
        ): IO[Unit] =
          trace("publish") *> generation.get.flatMap(current =>
            IO.raiseUnless(current == reservation.generation)(AnalyticsError.GuardedErasurePublicationRejected)
          )
        override def publicationReceipt(
            reservation: AnalyticsReportReservation
        ): IO[AnalyticsReportPublicationReceipt] =
          trace("receipt") *> calls.get.map { values =>
            if (publishedReceipt) AnalyticsReportPublicationReceipt.CurrentGeneration
            else if (alwaysSuperseded || (supersedeFirstReceipt && values.count(_ == "receipt") == 1))
              AnalyticsReportPublicationReceipt.Superseded
            else AnalyticsReportPublicationReceipt.Absent
          }
        override def publishErasure(
            reservation: AnalyticsReportReservation,
            report: AnalyticsReportOutput,
            expiresAt: Instant,
            claim: com.example.hiring.analytics.service.erasure.ErasureClaim,
            completedAt: Instant
        ): IO[Unit] = IO.unit
      }
      Fixture(
        new AnalyticsLateFactReplayService[IO](
          "test-lakehouse",
          journal,
          markers,
          stages,
          publisher,
          lock,
          30.asInstanceOf[AnalyticsPositiveInt],
          AnalyticsTestClocks.fixed(Now)
        ),
        calls,
        state
      )
    }

  private def record(attempt: Int, progress: AnalyticsLateFactReplayProgress): AnalyticsLateFactReplayRecord = {
    val identity = AnalyticsLateFactReplayService.reservationIdentityFor(request, attempt).toOption.get
    AnalyticsLateFactReplayRecord(
      request.selectionDigest,
      progress,
      attempt,
      AnalyticsReportReservation(identity._1, identity._2, 0L, 1L)
    )
  }

  private def replayRequest(id: String): AnalyticsLateFactReplayRequest =
    AnalyticsLateFactReplayRequest.from(id, Vector(("hiring-events", 0, 10L))).toOption.get
}
