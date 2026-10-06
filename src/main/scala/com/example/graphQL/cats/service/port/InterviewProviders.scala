package com.example.graphQL.cats.service.port

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{UserId}
import com.example.graphQL.cats.domain.workflow.{InterviewInterval, InterviewParticipant, InterviewWorkflowId}
import java.time.Instant

enum InterviewProviderError {
  case Conflict
  case Unavailable
  case InvalidReceipt
}

type InterviewProviderIO[A] = EitherT[IO, InterviewProviderError, A]

final case class InterviewCalendarReservation(
    workflowId: InterviewWorkflowId,
    idempotencyKey: String,
    candidateId: UserId,
    recruiterId: UserId,
    interval: InterviewInterval,
    reservedAt: Instant,
    releasedAt: Option[Instant]
)

/** Storage owns atomic participant-overlap checks and durable idempotency receipts. It must serialize checks for both
  * participants and reject intersecting half-open `[start, end)` intervals; equal adjacent boundaries remain valid.
  */
trait FakeInterviewCalendarStore {
  def reserveIfAvailable(
      reservation: InterviewCalendarReservation,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarReservation]
  def find(workflowId: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]]
  def release(
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit]
}

trait InterviewCalendarProvider {
  def reserve(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      candidateId: UserId,
      recruiterId: UserId,
      interval: InterviewInterval,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarReservation]
  def lookup(workflowId: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]]
  def release(
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit]
}

object FakeInterviewCalendarProvider {
  def durable(store: FakeInterviewCalendarStore): InterviewCalendarProvider =
    new InterviewCalendarProvider {
      override def reserve(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          candidateId: UserId,
          recruiterId: UserId,
          interval: InterviewInterval,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderIO[InterviewCalendarReservation] =
        store.reserveIfAvailable(
          InterviewCalendarReservation(workflowId, idempotencyKey, candidateId, recruiterId, interval, now, None),
          execution
        )

      override def lookup(workflowId: InterviewWorkflowId): InterviewProviderIO[Option[InterviewCalendarReservation]] =
        store.find(workflowId)

      override def release(
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderIO[Unit] =
        store.release(idempotencyKey, now, execution)
    }
}

final case class InterviewNotificationReceipt(
    workflowId: InterviewWorkflowId,
    recipientId: UserId,
    participant: InterviewParticipant,
    idempotencyKey: String,
    deliveredAt: Instant
)

/** A provider-side unique idempotency receipt is required because delivery is outside the hiring transaction. */
trait FakeInterviewNotificationStore {
  def deliverOnce(
      receipt: InterviewNotificationReceipt,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewNotificationReceipt]
  def find(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]]
}

trait InterviewNotificationProvider {
  def notify(
      workflowId: InterviewWorkflowId,
      recipientId: UserId,
      participant: InterviewParticipant,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewNotificationReceipt]
  def lookup(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]]
}

object FakeInterviewNotificationProvider {
  def durable(store: FakeInterviewNotificationStore): InterviewNotificationProvider =
    new InterviewNotificationProvider {
      override def notify(
          workflowId: InterviewWorkflowId,
          recipientId: UserId,
          participant: InterviewParticipant,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderIO[InterviewNotificationReceipt] =
        store.deliverOnce(
          InterviewNotificationReceipt(workflowId, recipientId, participant, idempotencyKey, now),
          execution
        )

      override def lookup(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
        store.find(idempotencyKey)
    }
}
