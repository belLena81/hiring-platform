package com.example.graphQL.cats.service.port

import cats.data.EitherT
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

type InterviewProviderF[F[_], A] = EitherT[F, InterviewProviderError, A]

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
trait InterviewCalendarLedger[F[_]] {
  def reserveIfAvailable(
      reservation: InterviewCalendarReservation,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewCalendarReservation]

  /** Looks a reservation up by its reserve key or cancel key, never by workflow alone. */
  def find(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String
  ): InterviewProviderF[F, Option[InterviewCalendarReservation]]
  def release(
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, Unit]

  /** Idempotent confirmed cancellation of the reservation addressed by a cancel key. */
  def cancel(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      at: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewCalendarCancellation]
}

/** The calendar effects of interview scheduling: reserve, cancel and compensating release, each addressed by an
  * idempotency key (the reserve key or the cancel key of a workflow generation).
  *
  * Adapter rule: every call is applied at most once per `idempotencyKey`, and a repeated key returns the first outcome.
  * An adapter uses the provider's own idempotency mechanism (an idempotency header or a client-assigned id), or, when
  * the provider has none, records a durable send intent per key before the call and resolves an unknown outcome
  * (timeout, crash, lost response) through `lookup`, never by resending blindly. `lookup` answers whether an effect for
  * the key exists, whatever the caller saw. Adapters prove the rule with the shared provider contract suite.
  */
trait InterviewCalendarProvider[F[_]] {
  def reserve(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      candidateId: UserId,
      recruiterId: UserId,
      interval: InterviewInterval,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewCalendarReservation]

  /** Reservation state (held, or cancelled with a timestamp) addressed by its reserve key or its cancel key. */
  def lookup(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String
  ): InterviewProviderF[F, Option[InterviewCalendarReservation]]

  /** Confirmed, idempotent cancellation: success means the provider confirmed the slot is released. */
  def cancel(
      workflowId: InterviewWorkflowId,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewCalendarCancellation]

  /** Scheduling compensation; the same confirmed cancellation addressed by the scheduling release key. */
  def release(
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, Unit]
}

/** The platform's own durable calendar ledger acting as the calendar provider when no external provider is configured.
  * Reservations, holds and confirmed cancellations are recorded in the ledger and are authoritative for availability.
  */
object LedgerInterviewCalendarProvider {
  def durable[F[_]](store: InterviewCalendarLedger[F]): InterviewCalendarProvider[F] =
    new InterviewCalendarProvider[F] {
      override def reserve(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          candidateId: UserId,
          recruiterId: UserId,
          interval: InterviewInterval,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderF[F, InterviewCalendarReservation] =
        store.reserveIfAvailable(
          InterviewCalendarReservation(workflowId, idempotencyKey, candidateId, recruiterId, interval, now, None),
          execution
        )

      override def lookup(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String
      ): InterviewProviderF[F, Option[InterviewCalendarReservation]] =
        store.find(workflowId, idempotencyKey)

      override def cancel(
          workflowId: InterviewWorkflowId,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderF[F, InterviewCalendarCancellation] =
        store.cancel(workflowId, idempotencyKey, now, execution)

      override def release(
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderF[F, Unit] =
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
trait InterviewNotificationReceiptLedger[F[_]] {
  def deliverOnce(
      receipt: InterviewNotificationReceipt,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewNotificationReceipt]
  def find(idempotencyKey: String): InterviewProviderF[F, Option[InterviewNotificationReceipt]]
}

/** The notification effect of interview scheduling: one delivery per `idempotencyKey`, returned as a receipt.
  *
  * Delivery guarantee (owner decision 2026-11-10), by provider kind:
  *   - Exactly-once receipts: a provider with a durable receipt per key (the platform ledger) or its own idempotency
  *     mechanism (an idempotency header or a client-assigned id) applies a call at most once per `idempotencyKey` and
  *     returns the first outcome for every repeat. `lookup` answers whether an effect for the key exists, whatever the
  *     caller saw. The shared contract suite proves this.
  *   - At-least-once: a provider with neither a remote lookup nor a local receipt (email through SES) cannot tell an
  *     unknown outcome (timeout, lost response, crash after acceptance) from a failure, so `lookup` reports no known
  *     delivery and the interpreter may send again. A duplicate message is allowed; a silent loss is not. Such a
  *     provider proves the weaker rule with the at-least-once variant of the contract suite.
  */
trait InterviewNotificationProvider[F[_]] {
  def notify(
      workflowId: InterviewWorkflowId,
      recipientId: UserId,
      participant: InterviewParticipant,
      kind: InterviewNotificationKind,
      idempotencyKey: String,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): InterviewProviderF[F, InterviewNotificationReceipt]
  def lookup(idempotencyKey: String): InterviewProviderF[F, Option[InterviewNotificationReceipt]]
}

/** The platform's own durable receipt ledger acting as the notification provider when no external provider is
  * configured. It records exactly one receipt per idempotency key; no message leaves the platform.
  */
object LedgerInterviewNotificationProvider {
  def durable[F[_]](store: InterviewNotificationReceiptLedger[F]): InterviewNotificationProvider[F] =
    new InterviewNotificationProvider[F] {
      override def notify(
          workflowId: InterviewWorkflowId,
          recipientId: UserId,
          participant: InterviewParticipant,
          kind: InterviewNotificationKind,
          idempotencyKey: String,
          now: Instant,
          execution: Option[ClaimedInterviewWorkflowCommand] = None
      ): InterviewProviderF[F, InterviewNotificationReceipt] =
        store.deliverOnce(
          InterviewNotificationReceipt(workflowId, recipientId, participant, kind, idempotencyKey, now),
          execution
        )

      override def lookup(idempotencyKey: String): InterviewProviderF[F, Option[InterviewNotificationReceipt]] =
        store.find(idempotencyKey)
    }
}
