package com.example.hiring.analytics.service.keyretirement
import com.example.hiring.analytics.service.keyretirement.*

import com.example.hiring.analytics.domain.AnalyticsDigest
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.IO

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
      value != null && value.lakehouseId != null && value.lakehouseId.matches("[0-9a-f]{64}") &&
        value.keyId != null && value.keyId.matches("[A-Za-z0-9-]{1,40}") &&
        value.originalVerifier != null && value.originalVerifier.matches("[A-Za-z0-9_-]{43}") &&
        value.evidenceFacts != null && value.evidenceFacts.nonEmpty && value.evidenceFacts.length <= 8192 &&
        value.evidenceDigest != null && value.evidenceDigest == digest(value.evidenceFacts) &&
        value.authorizedAt != null,
      value,
      AnalyticsError.InvalidConfiguration("HMAC key retirement authorization is malformed")
    )
}

/** The caller must hold the shared lakehouse mutex for all reads and writes. */
private[analytics] trait HmacKeyRetirementAuthorizationStore {
  def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]]
  def insert(root: String, authorization: HmacKeyRetirementAuthorization): IO[Unit]
}

private[analytics] object HmacKeyRetirementAuthorizationStore {
  val unavailable: HmacKeyRetirementAuthorizationStore = new HmacKeyRetirementAuthorizationStore {
    override def list(root: String): IO[Vector[HmacKeyRetirementAuthorization]] = IO.pure(Vector.empty)
    override def insert(root: String, authorization: HmacKeyRetirementAuthorization): IO[Unit] =
      IO.raiseError(AnalyticsError.InvalidConfiguration("HMAC key retirement authorization store is unavailable"))
  }
}
