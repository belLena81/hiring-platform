package com.example.graphQL.cats.service.auth

import com.example.graphQL.cats.service.port.MutationReceiptFingerprint

/** Protects password-bearing canonical-input digests before receipt persistence. */
trait AuthenticationFingerprint {
  def protect(operation: String, actorScope: String, digest: MutationReceiptFingerprint): MutationReceiptFingerprint
  def keyId: String
}
