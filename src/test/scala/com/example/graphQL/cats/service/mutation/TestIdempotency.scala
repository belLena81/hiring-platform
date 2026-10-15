package com.example.graphQL.cats.service.mutation

import cats.effect.IO
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
}
