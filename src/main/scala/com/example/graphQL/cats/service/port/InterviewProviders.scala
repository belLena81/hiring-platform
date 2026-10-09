package com.example.graphQL.cats.service.port

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{UserId}
import com.example.graphQL.cats.domain.workflow.{
  InterviewInterval,
  InterviewNotificationKind,
  InterviewParticipant,
  InterviewWorkflowId
}
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

/** Confirmed outcome of a provider cancellation. Only `Cancelled` and `AlreadyCancelled` mean the slot is released. */
enum InterviewCalendarCancellation {
  case Cancelled(at: Instant)
  case AlreadyCancelled(at: Instant)
  case UnknownReservation
}

/** Storage owns atomic participant-overlap checks and durable idempotency receipts. It must serialize checks for both
  * participants and reject intersecting half-open `[start, end)` intervals of other workflows; equal adjacent
  * boundaries remain valid and a workflow's own holds never conflict with each other.
  */
trait InterviewCalendarLedger {
  def reserveIfAvailable(
      reservation: InterviewCalendarReservation,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarReservation]

  /** Looks a reservation up by its reserve key or cancel key, never by workflow alone. */
  def find(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String
  ): InterviewProviderIO[Option[InterviewCalendarReservation]]
  def release(
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit]

  /** Idempotent confirmed cancellation of the reservation addressed by a cancel key. */
  def cancel(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarCancellation]
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

  /** Reservation state (held, or cancelled with a timestamp) addressed by its reserve key or its cancel key. */
  def lookup(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String
  ): InterviewProviderIO[Option[InterviewCalendarReservation]]

  /** Confirmed, idempotent cancellation: success means the provider confirmed the slot is released. */
  def cancel(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewCalendarCancellation]

  /** Scheduling compensation; the same confirmed cancellation addressed by the scheduling release key. */
  def release(
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[Unit]
}

/** The platform's own durable calendar ledger acting as the calendar provider when no external provider is configured.
  * Reservations, holds and confirmed cancellations are recorded in the ledger and are authoritative for availability.
  */
object LedgerInterviewCalendarProvider {
  def durable(store: InterviewCalendarLedger): InterviewCalendarProvider =
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

      override def lookup(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String
      ): InterviewProviderIO[Option[InterviewCalendarReservation]] =
        store.find(workflowId, idempotencyKey)

      override def cancel(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderIO[InterviewCalendarCancellation] =
        store.cancel(workflowId, idempotencyKey, now, execution)

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
    kind: InterviewNotificationKind,
    idempotencyKey: String,
    deliveredAt: Instant
)

/** A provider-side unique idempotency receipt is required because delivery is outside the hiring transaction. */
trait InterviewNotificationReceiptLedger {
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
      kind: InterviewNotificationKind,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderIO[InterviewNotificationReceipt]
  def lookup(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]]
}

/** The platform's own durable receipt ledger acting as the notification provider when no external provider is
  * configured. It records exactly one receipt per idempotency key; no message leaves the platform.
  */
object LedgerInterviewNotificationProvider {
  def durable(store: InterviewNotificationReceiptLedger): InterviewNotificationProvider =
    new InterviewNotificationProvider {
      override def notify(
          workflowId: InterviewWorkflowId,
          recipientId: UserId,
          participant: InterviewParticipant,
          kind: InterviewNotificationKind,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderIO[InterviewNotificationReceipt] =
        store.deliverOnce(
          InterviewNotificationReceipt(workflowId, recipientId, participant, kind, idempotencyKey, now),
          execution
        )

      override def lookup(idempotencyKey: String): InterviewProviderIO[Option[InterviewNotificationReceipt]] =
        store.find(idempotencyKey)
    }
}
