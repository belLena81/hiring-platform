package com.example.graphQL.cats.config

import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import scala.concurrent.duration.*

/** Durable backlog gauges (embedding work, operational event outbox) are sampled every `backlogSampleSeconds`. The
  * default mirrors the packaged `application.conf` for an absent section.
  */
final case class TelemetryConfig(backlogSampleInterval: FiniteDuration = 15.seconds)

private[config] final case class RawTelemetryConfig(backlogSampleSeconds: BacklogSampleSeconds) derives ConfigReader

private[config] object TelemetryConfigValidation {
  def read(raw: RawTelemetryConfig): TelemetryConfig = TelemetryConfig(raw.backlogSampleSeconds.seconds)
}
