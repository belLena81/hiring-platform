package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.shared.crypto.SourceHash
import java.time.Instant
import java.util.UUID

/** A stable, caller-scoped key for replaying a state-changing operation. */
final case class MutationReceiptKey(operation: String, actorScope: String, idempotencyKey: UUID)

/** A one-way fingerprint of the canonical mutation input. Never persist source input here. */
final case class MutationReceiptFingerprint private (value: String)

object MutationReceiptFingerprint {
  def fromCanonicalInput(input: String): MutationReceiptFingerprint =
    MutationReceiptFingerprint(SourceHash.sha256(input))

  private[cats] def stored(value: String): MutationReceiptFingerprint = MutationReceiptFingerprint(value)
}

/** A non-sensitive reference from a completed receipt to its authoritative result. */
final case class MutationEntityReference(entityType: String, entityId: String)

object MutationEntityReference {
  def of(kind: String, id: java.util.UUID): MutationEntityReference = MutationEntityReference(kind, id.toString)
}

enum MutationReceiptState {
  case InProgress, Completed
}

final case class MutationReceipt(
    key: MutationReceiptKey,
    fingerprint: MutationReceiptFingerprint,
    state: MutationReceiptState,
    entity: Option[MutationEntityReference],
    createdAt: Instant,
    completedAt: Option[Instant],
    expiresAt: Instant
)

/** Opaque transaction capability supplied only by a persistence adapter. */
trait MutationWriteContext

object MutationWriteContext {

  /** Explicit direct-write context for operations outside an idempotent mutation. */
  val directWrite: MutationWriteContext = new MutationWriteContext {}
}

final case class MutationReceiptWrite[+A](value: A, entity: MutationEntityReference)

enum MutationWriteOutcome[+A, +E] {
  case Applied(write: MutationReceiptWrite[A])
  case Rejected(error: E)
}

enum MutationReceiptExecution[+A, +E] {
  case Applied(value: A, entity: MutationEntityReference)
  case Replay(entity: MutationEntityReference)
  case Rejected(error: E)
  case FingerprintMismatch
  case InProgress
}

/** Runs a state write and its idempotency receipt atomically when the adapter supports transactions. The callback must
  * use the supplied context for every participating write.
  */
trait MutationReceiptRepository[F[_]] {
  def execute[A, E](
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  )(
      write: MutationWriteContext => RepositoryF[F, MutationWriteOutcome[A, E]]
  ): RepositoryF[F, MutationReceiptExecution[A, E]]
}
