package com.example.hiring.analytics.service.keyretirement

import com.example.hiring.analytics.domain.AnalyticsDigest
import com.example.hiring.analytics.errors.AnalyticsError

import java.time.Instant
import java.nio.charset.StandardCharsets

/** Permanent evidence that one anchored key may be omitted from this exact lakehouse's runtime key ring. */
private[analytics] final case class HmacKeyRetirementAuthorization(
    lakehouseId: String,
    keyId: String,
    originalVerifier: String,
    evidenceFacts: String,
    evidenceDigest: String,
    authorizedAt: Instant
)

private[analytics] object HmacKeyRetirementAuthorization {
  def digest(facts: String): String = AnalyticsDigest.sha256Hex(facts.getBytes(StandardCharsets.UTF_8))

  def validate(value: HmacKeyRetirementAuthorization): Either[AnalyticsError, HmacKeyRetirementAuthorization] =
    Either.cond(
      value.lakehouseId.matches("[0-9a-f]{64}") &&
        value.keyId.matches("[A-Za-z0-9-]{1,40}") &&
        value.originalVerifier.matches("[A-Za-z0-9_-]{43}") &&
        value.evidenceFacts.nonEmpty && value.evidenceFacts.length <= 8192 &&
        value.evidenceDigest == digest(value.evidenceFacts),
      value,
      AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    )
}

/** The caller must hold the shared lakehouse mutex for all reads and writes. */
private[analytics] trait HmacKeyRetirementAuthorizationStore[F[_]] {
  def list(root: String): F[Vector[HmacKeyRetirementAuthorization]]
  def insert(root: String, authorization: HmacKeyRetirementAuthorization): F[Unit]
}
