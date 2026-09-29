package com.example.graphQL.cats.service.mutation

import cats.effect.IO
import com.example.graphQL.cats.service.port.*

import java.time.Instant

private[cats] object TestIdempotency {
  val noopReceipts: MutationReceiptRepository = new MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => RepositoryIO[MutationWriteOutcome[A, E]]
    ): RepositoryIO[MutationReceiptExecution[A, E]] =
      write(MutationWriteContext.directWrite).map {
        case MutationWriteOutcome.Rejected(error) => MutationReceiptExecution.Rejected(error)
        case MutationWriteOutcome.Applied(value)  => MutationReceiptExecution.Applied(value.value, value.entity)
      }
  }

  val noop: Idempotent = Idempotent(noopReceipts)
}
