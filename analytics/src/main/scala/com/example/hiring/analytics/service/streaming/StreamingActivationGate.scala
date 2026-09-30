package com.example.hiring.analytics.service.streaming

import com.example.hiring.analytics.domain.StreamingActivationIdentity

trait StreamingActivationGate[F[_]] {
  def requireAuthorized(identity: StreamingActivationIdentity): F[Unit]
}
