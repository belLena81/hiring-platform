package com.example.graphQL.cats.service.protocol

import com.example.graphQL.cats.service.port.MutationReceiptFingerprint
import java.util.UUID

final case class IdempotencyRequest private[service] (
    idempotencyKey: UUID,
    fingerprint: MutationReceiptFingerprint
)

object IdempotencyRequest {
  def fromCanonicalInput(idempotencyKey: UUID, canonicalInput: String): IdempotencyRequest =
    IdempotencyRequest(idempotencyKey, MutationReceiptFingerprint.fromCanonicalInput(canonicalInput))
}
