package com.example.graphQL.cats.service.port

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite

/** How account deletion asks a calendar provider to release a hold before its local evidence is purged. */
final class InterviewHoldCancellerSpec extends CatsEffectSuite {
  private val at = Instant.parse("2026-10-09T12:00:00Z")
  private val id = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-0000000000b1"))
  private val hold =
    InterviewLiveHold(id, InterviewWorkflow.reservationKey(id, 2), InterviewWorkflow.cancellationKey(id, 2))

  test("DHW-30 the platform ledger is its own provider, so the cleanup's local release is the confirmation") {
    InterviewHoldCanceller.ledgerOwned[IO].cancel(hold, at).value.map { answer =>
      assertEquals(answer, Right(InterviewCalendarCancellation.Cancelled(at)))
    }
  }

  test(
    "DHW-30 an external provider is asked for the confirmed cancel under the hold's own cancel key and nothing else"
  ) {
    for {
      asked <- Ref.of[IO, Vector[(InterviewWorkflowId, String)]](Vector.empty)
      calendar = new InterviewCalendarProvider[IO] {
        override def reserve(
            workflowId: InterviewWorkflowId,
            idempotencyKey: String,
            candidateId: UserId,
            recruiterId: UserId,
            interval: InterviewInterval,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderF[IO, InterviewCalendarReservation] =
          EitherT.leftT(InterviewProviderError.Unavailable)
        override def lookup(
            workflowId: InterviewWorkflowId,
            idempotencyKey: String
        ): InterviewProviderF[IO, Option[InterviewCalendarReservation]] =
          EitherT.leftT(InterviewProviderError.Unavailable)
        override def cancel(
            workflowId: InterviewWorkflowId,
            idempotencyKey: String,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderF[IO, InterviewCalendarCancellation] =
          EitherT.liftF[IO, InterviewProviderError, Unit](asked.update(_ :+ (workflowId -> idempotencyKey))) *>
            EitherT.rightT[IO, InterviewProviderError](InterviewCalendarCancellation.AlreadyCancelled(now))
        override def release(
            idempotencyKey: String,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ): InterviewProviderF[IO, Unit] = EitherT.leftT(InterviewProviderError.Unavailable)
      }
      answer <- InterviewHoldCanceller.external(calendar).cancel(hold, at).value
      calls <- asked.get
    } yield {
      assertEquals(answer, Right(InterviewCalendarCancellation.AlreadyCancelled(at)))
      assertEquals(calls, Vector(id -> InterviewWorkflow.cancellationKey(id, 2)))
    }
  }
}
