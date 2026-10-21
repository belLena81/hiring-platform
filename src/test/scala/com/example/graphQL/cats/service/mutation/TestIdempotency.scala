package com.example.graphQL.cats.service.mutation

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.service.port.*

import java.time.Instant

private[cats] object TestIdempotency {
  val noopReceipts: MutationReceiptRepository[IO] = new MutationReceiptRepository[IO] {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => RepositoryF[IO, MutationWriteOutcome[A, E]]
    ): RepositoryF[IO, MutationReceiptExecution[A, E]] =
      write(MutationWriteContext.directWrite).map {
        case MutationWriteOutcome.Rejected(error) => MutationReceiptExecution.Rejected(error)
        case MutationWriteOutcome.Applied(value)  => MutationReceiptExecution.Applied(value.value, value.entity)
      }
  }

  val noop: Idempotent[IO] = Idempotent(noopReceipts)

  /** Receipts that remember applied writes by key: a repeated request replays, a changed input is a mismatch. */
  def remembering: IO[MutationReceiptRepository[IO]] =
    Ref.of[IO, Map[MutationReceiptKey, (MutationReceiptFingerprint, MutationEntityReference)]](Map.empty).map {
      stored =>
        new MutationReceiptRepository[IO] {
          override def execute[A, E](
              key: MutationReceiptKey,
              fingerprint: MutationReceiptFingerprint,
              now: Instant,
              expiresAt: Instant
          )(
              write: MutationWriteContext => RepositoryF[IO, MutationWriteOutcome[A, E]]
          ): RepositoryF[IO, MutationReceiptExecution[A, E]] =
            RepositoryF.lift(stored.get.map(_.get(key))).flatMap {
              case Some((existing, entity)) if existing == fingerprint =>
                RepositoryF.pure[IO, MutationReceiptExecution[A, E]](MutationReceiptExecution.Replay(entity))
              case Some(_) =>
                RepositoryF.pure[IO, MutationReceiptExecution[A, E]](MutationReceiptExecution.FingerprintMismatch)
              case None =>
                write(MutationWriteContext.directWrite).flatMap {
                  case MutationWriteOutcome.Rejected(error) =>
                    RepositoryF.pure[IO, MutationReceiptExecution[A, E]](MutationReceiptExecution.Rejected(error))
                  case MutationWriteOutcome.Applied(value) =>
                    RepositoryF
                      .lift(stored.update(_ + (key -> (fingerprint -> value.entity))))
                      .map(_ =>
                        MutationReceiptExecution.Applied(value.value, value.entity): MutationReceiptExecution[A, E]
                      )
                }
            }
        }
    }
}
