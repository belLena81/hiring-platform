package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, RepositoryError}
import com.example.graphQL.cats.service.Diagnostics.emit

import java.util.UUID

/** Which step of an application write produced a `Conflict`. The public error stays `CONFLICT`; the source is only
  * recorded in the diagnostics (`APPLICATION_WRITE_CONFLICT`), so an intermittent refusal of a sole user's valid action
  * can be attributed to a step instead of guessed.
  */
private[mongo] enum ConflictSource(val label: String) {

  /** The idempotency key was reused with a different input. */
  case ReceiptFingerprint extends ConflictSource("receipt-fingerprint")

  /** The mutation receipt insert or completion hit a duplicate key. */
  case ReceiptDuplicateKey extends ConflictSource("receipt-duplicate-key")

  /** The receipt completion matched no pending receipt (lost or expired). */
  case ReceiptCompletionNoMatch extends ConflictSource("receipt-completion-no-match")

  /** The first upsert of the subject fence document lost to another creator. */
  case FenceDuplicateKey extends ConflictSource("fence-duplicate-key")

  /** One attempt of the subject fence write conflicted with another transaction (the runner retries it). */
  case FenceWriteConflict extends ConflictSource("fence-write-conflict")

  /** One attempt of the status, history or outbox write conflicted with another transaction. */
  case StatusWriteConflict extends ConflictSource("status-write-conflict")

  /** The guarded status update matched no document (stale status, owner or job). */
  case StatusGuardNoMatch extends ConflictSource("status-guard-no-match")

  /** The history row insert hit a duplicate key. */
  case HistoryDuplicateKey extends ConflictSource("history-duplicate-key")

  /** The transaction runner gave up after its retries; reported by the runner, whatever the operation. */
  case TransactionExhausted extends ConflictSource("transaction-exhausted")
}

private[mongo] object ApplicationWriteConflict {

  /** Operation name, source and opaque UUIDs only: no names, emails, feedback, reasons or tokens. */
  def fields(
      source: ConflictSource,
      operation: String,
      entity: Option[UUID],
      actor: Option[UUID]
  ): Map[LogField, String] =
    Map(LogField.ConflictSource -> source.label, LogField.SpanName -> operation) ++
      entity.map(LogField.EntityId -> _.toString) ++ actor.map(LogField.ActorId -> _.toString)

  def report(
      diagnostics: Diagnostics[IO],
      source: ConflictSource,
      operation: String,
      entity: Option[UUID] = None,
      actor: Option[UUID] = None
  ): IO[Unit] =
    diagnostics.emit(LogEvent.ApplicationWriteConflict, fields = fields(source, operation, entity, actor))

  /** Records `source` when `step` is refused with `Conflict`; every other outcome passes through unchanged. */
  def onConflict[A](
      diagnostics: Diagnostics[IO],
      source: ConflictSource,
      operation: String,
      entity: Option[UUID] = None,
      actor: Option[UUID] = None
  )(
      step: RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    step.leftSemiflatTap {
      case RepositoryError.Conflict => report(diagnostics, source, operation, entity, actor)
      case _                        => IO.unit
    }

  /** Records `source` when `step` fails with a duplicate-key error, and re-raises it unchanged for the caller's guard.
    */
  def onDuplicateKey[A](
      diagnostics: Diagnostics[IO],
      source: ConflictSource,
      operation: String,
      entity: Option[UUID] = None,
      actor: Option[UUID] = None
  )(step: RepositoryF[IO, A]): RepositoryF[IO, A] =
    RepositoryF.fromFEither(step.value.onError {
      case error if MongoErrors.isDuplicateKey(error) => report(diagnostics, source, operation, entity, actor)
    })

  /** Records `source` for a transient (retried) failure of `step` and re-raises it unchanged for the runner. */
  def onTransient[A](
      diagnostics: Diagnostics[IO],
      source: ConflictSource,
      operation: String,
      entity: Option[UUID] = None,
      actor: Option[UUID] = None
  )(
      step: RepositoryF[IO, A]
  ): RepositoryF[IO, A] =
    RepositoryF.fromFEither(step.value.onError {
      case error if MongoErrors.isTransient(error) => report(diagnostics, source, operation, entity, actor)
    })
}
