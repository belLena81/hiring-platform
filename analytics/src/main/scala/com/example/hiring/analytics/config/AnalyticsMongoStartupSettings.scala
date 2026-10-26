package com.example.hiring.analytics.config

import scala.concurrent.duration.FiniteDuration

/** How an analytics process waits for MongoDB before it builds anything expensive.
  *
  * `maxWait` bounds the total backoff sleeping of a long-running command; each individual check is bounded by
  * `checkTimeout`, and each backoff grows from `initialBackoff` up to `maxBackoff`.
  */
final case class AnalyticsMongoStartupSettings(
    checkTimeout: FiniteDuration,
    initialBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    maxWait: FiniteDuration
)
