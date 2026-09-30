package com.example.hiring.analytics.service.streaming

import com.example.hiring.analytics.domain.StreamingActivationIdentity

import java.time.Instant

trait StreamingActivationGate[F[_]] {

  /** Verifies the selected immutable grant and returns its expiry instant. */
  def requireAuthorized(identity: StreamingActivationIdentity, grantId: String): F[Instant]
}
