package com.example.graphQL.cats.service.port

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.{InterviewInterval, InterviewWorkflow}
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import munit.CatsEffectSuite

/** How a calendar implementation is arranged for the shared contract. The platform's ledger needs the workflow to be in
  * the phase that demands each effect; an external adapter has no such precondition and implements the arrangement
  * steps as no-ops.
  */
trait InterviewCalendarContractSubject {
  def provider: InterviewCalendarProvider[IO]

  /** A workflow whose original hold may be reserved now. */
  def newWorkflow(candidate: UserId, recruiter: UserId, interval: InterviewInterval): IO[InterviewWorkflow]

  /** Arranges that the workflow demands a replacement hold over `replacement`; returns the arranged workflow. */
  def requireReplacementHold(workflow: InterviewWorkflow, replacement: InterviewInterval): IO[InterviewWorkflow]

  /** Arranges that the workflow demands the confirmed cancellation of the reservation of `generation`. */
  def requireCancellation(workflow: InterviewWorkflow, generation: Int): IO[InterviewWorkflow]
}

/** Behavior every calendar provider must satisfy before it may be activated: idempotent reserve, lookup by key,
  * confirmed and idempotent cancel, participant conflicts over half-open intervals, and a workflow's own holds never
  * conflicting with each other.
  */
trait InterviewCalendarProviderContract { self: CatsEffectSuite =>

  /** Each test receives an isolated subject. */
  protected def withSubject[A](use: InterviewCalendarContractSubject => IO[A]): IO[A]

  private val base = Instant.now().truncatedTo(ChronoUnit.MILLIS).plus(3, ChronoUnit.DAYS)
  private def slot(startHour: Long, hours: Long = 1L) =
    InterviewInterval(base.plus(startHour, ChronoUnit.HOURS), base.plus(startHour + hours, ChronoUnit.HOURS))
  private def user() = UserId(UUID.randomUUID())

  private def success[A](value: InterviewProviderF[IO, A]): IO[A] =
    value.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Provider failure: $error")), IO.pure))

  /** `Unavailable` is the contract's retryable outcome: a store whose transactions contend (eight callers on one
    * workflow's fence and participant locks) may exhaust its own bounded retries for one caller. A caller retries it,
    * as the scheduling command does; any other error, and an `Unavailable` that outlasts the bound, still fails. Used
    * by the concurrent reserve and cancel tests. It is a bounded mitigation of contention, not a proof of cause.
    */
  private def retryingUnavailable[A](
      call: => InterviewProviderF[IO, A],
      attemptsLeft: Int = 5
  ): IO[A] =
    call.value.flatMap {
      case Right(value)                                                 => IO.pure(value)
      case Left(InterviewProviderError.Unavailable) if attemptsLeft > 1 =>
        retryingUnavailable(call, attemptsLeft - 1)
      case Left(error) => IO.raiseError(new AssertionError(s"Provider failure: $error"))
    }

  private def reserve(subject: InterviewCalendarContractSubject, workflow: InterviewWorkflow, generation: Int) =
    subject.provider.reserve(
      workflow.id,
      InterviewWorkflow.reservationKey(workflow.id, generation),
      workflow.candidateId,
      workflow.recruiterId,
      if (generation == 0) workflow.interval else workflow.pendingInterval.getOrElse(workflow.interval),
      Instant.now(),
      None
    )

  test("DHW-25 reserve is idempotent and lookup finds the reservation by its reserve key and its cancel key") {
    withSubject { subject =>
      for {
        workflow <- subject.newWorkflow(user(), user(), slot(0))
        first <- success(reserve(subject, workflow, 0))
        again <- success(reserve(subject, workflow, 0))
        byReserve <- success(
          subject.provider.lookup(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0))
        )
        byCancel <- success(subject.provider.lookup(workflow.id, InterviewWorkflow.cancellationKey(workflow.id, 0)))
        unrelated <- success(subject.provider.lookup(workflow.id, "unrelated-key"))
      } yield {
        assertEquals(again, first)
        assertEquals(byReserve, Some(first))
        assertEquals(byCancel, Some(first))
        assertEquals(first.releasedAt, None)
        assertEquals(unrelated, None)
      }
    }
  }

  test("DHW-25 cancel is confirmed, idempotent and visible through lookup") {
    withSubject { subject =>
      for {
        arranged <- subject.newWorkflow(user(), user(), slot(0))
        _ <- success(reserve(subject, arranged, 0))
        workflow <- subject.requireCancellation(arranged, 0)
        key = InterviewWorkflow.cancellationKey(workflow.id, 0)
        at = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        first <- success(subject.provider.cancel(workflow.id, key, at, None))
        second <- success(subject.provider.cancel(workflow.id, key, at.plusSeconds(60), None))
        looked <- success(subject.provider.lookup(workflow.id, key))
      } yield {
        assertEquals(first, InterviewCalendarCancellation.Cancelled(at))
        assertEquals(second, InterviewCalendarCancellation.AlreadyCancelled(at))
        assertEquals(looked.flatMap(_.releasedAt), Some(at))
      }
    }
  }

  test("DHW-25 cancelling a reservation that was never made reports an unknown reservation") {
    withSubject { subject =>
      for {
        workflow <- subject.newWorkflow(user(), user(), slot(0))
        outcome <- success(
          subject.provider.cancel(workflow.id, InterviewWorkflow.cancellationKey(workflow.id, 0), Instant.now(), None)
        )
      } yield assertEquals(outcome, InterviewCalendarCancellation.UnknownReservation)
    }
  }

  test(
    "DHW-25 overlapping holds of another workflow conflict, adjacent half-open intervals do not, and cancel frees the slot"
  ) {
    withSubject { subject =>
      val candidate = user()
      val recruiter = user()
      for {
        first <- subject.newWorkflow(candidate, recruiter, slot(0, 2))
        _ <- success(reserve(subject, first, 0))
        overlapping <- subject.newWorkflow(candidate, user(), slot(1))
        conflict <- reserve(subject, overlapping, 0).value
        adjacent <- subject.newWorkflow(user(), recruiter, slot(2))
        _ <- success(reserve(subject, adjacent, 0))
        cancellable <- subject.requireCancellation(first, 0)
        _ <- success(
          subject.provider.cancel(cancellable.id, InterviewWorkflow.cancellationKey(first.id, 0), Instant.now(), None)
        )
        retried <- success(reserve(subject, overlapping, 0))
      } yield {
        assertEquals(conflict, Left(InterviewProviderError.Conflict))
        assertEquals(retried.workflowId, overlapping.id)
      }
    }
  }

  test(
    "DHW-25 a workflow's own holds never conflict and the old reservation is cancelled after the replacement is held"
  ) {
    withSubject { subject =>
      for {
        original <- subject.newWorkflow(user(), user(), slot(0, 2))
        _ <- success(reserve(subject, original, 0))
        holding <- subject.requireReplacementHold(original, slot(1, 2))
        replacement <- success(reserve(subject, holding, 1))
        cancelling <- subject.requireCancellation(holding, 0)
        cancelled <- success(
          subject.provider
            .cancel(cancelling.id, InterviewWorkflow.cancellationKey(cancelling.id, 0), Instant.now(), None)
        )
        newer <- success(
          subject.provider.lookup(holding.id, InterviewWorkflow.reservationKey(holding.id, 1))
        )
      } yield {
        assertEquals(replacement.interval, slot(1, 2))
        assert(cancelled.isInstanceOf[InterviewCalendarCancellation.Cancelled])
        assertEquals(newer.map(_.releasedAt), Some(None))
      }
    }
  }

  test("DDC-04.4 concurrent reserve calls with one reserve key give one reservation") {
    withSubject { subject =>
      for {
        workflow <- subject.newWorkflow(user(), user(), slot(0))
        at = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        reservations <- (0 until 8).toList.parTraverse(attempt =>
          retryingUnavailable(
            subject.provider.reserve(
              workflow.id,
              InterviewWorkflow.reservationKey(workflow.id, 0),
              workflow.candidateId,
              workflow.recruiterId,
              workflow.interval,
              at.plusSeconds(attempt.toLong),
              None
            )
          )
        )
        found <- success(subject.provider.lookup(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0)))
      } yield {
        // Each caller sent a different reservation time: one distinct result means one effect won.
        assertEquals(reservations.distinct.size, 1)
        assertEquals(found, reservations.headOption)
      }
    }
  }

  test("DDC-04.4 concurrent cancel calls with one cancel key confirm one cancellation") {
    withSubject { subject =>
      for {
        arranged <- subject.newWorkflow(user(), user(), slot(0))
        _ <- success(reserve(subject, arranged, 0))
        workflow <- subject.requireCancellation(arranged, 0)
        key = InterviewWorkflow.cancellationKey(workflow.id, 0)
        at = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        outcomes <- (0 until 8).toList.parTraverse(attempt =>
          // Bounded mitigation (retry only Unavailable, as for reserve above), not a proof of the cause of a refusal.
          retryingUnavailable(subject.provider.cancel(workflow.id, key, at.plusSeconds(attempt.toLong), None))
        )
        looked <- success(subject.provider.lookup(workflow.id, key))
      } yield {
        val fresh = outcomes.collect { case InterviewCalendarCancellation.Cancelled(when) => when }
        val repeats = outcomes.collect { case InterviewCalendarCancellation.AlreadyCancelled(when) => when }
        assertEquals(fresh.size, 1)
        assertEquals(repeats.size, outcomes.size - 1)
        assertEquals(repeats.distinct, fresh)
        assertEquals(looked.flatMap(_.releasedAt), fresh.headOption)
      }
    }
  }

  test("DDC-04.3 lookup returns the reservation and the cancellation after outcomes the caller did not see") {
    withSubject { subject =>
      for {
        arranged <- subject.newWorkflow(user(), user(), slot(0))
        before <- success(subject.provider.lookup(arranged.id, InterviewWorkflow.reservationKey(arranged.id, 0)))
        _ <- reserve(subject, arranged, 0).value.void
        held <- success(subject.provider.lookup(arranged.id, InterviewWorkflow.reservationKey(arranged.id, 0)))
        workflow <- subject.requireCancellation(arranged, 0)
        key = InterviewWorkflow.cancellationKey(workflow.id, 0)
        at = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        _ <- subject.provider.cancel(workflow.id, key, at, None).value.void
        released <- success(subject.provider.lookup(workflow.id, key))
      } yield {
        assertEquals(before, None)
        assertEquals(held.map(_.releasedAt), Some(None))
        assertEquals(released.flatMap(_.releasedAt), Some(at))
      }
    }
  }

  test("DDC-04.4 different keys stay independent: another workflow's reserve and cancel keys do not affect this one") {
    withSubject { subject =>
      for {
        first <- subject.newWorkflow(user(), user(), slot(0))
        second <- subject.newWorkflow(user(), user(), slot(0))
        firstReservation <- success(reserve(subject, first, 0))
        secondReservation <- success(reserve(subject, second, 0))
        cancellable <- subject.requireCancellation(first, 0)
        _ <- success(
          subject.provider.cancel(cancellable.id, InterviewWorkflow.cancellationKey(first.id, 0), Instant.now(), None)
        )
        stillHeld <- success(subject.provider.lookup(second.id, InterviewWorkflow.reservationKey(second.id, 0)))
        unknown <- success(
          subject.provider.cancel(second.id, InterviewWorkflow.cancellationKey(second.id, 1), Instant.now(), None)
        )
      } yield {
        assertNotEquals(firstReservation, secondReservation)
        assertEquals(stillHeld, Some(secondReservation))
        assertEquals(unknown, InterviewCalendarCancellation.UnknownReservation)
      }
    }
  }
}
