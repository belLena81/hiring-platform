package com.example.graphQL.cats.config

import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import scala.concurrent.duration.*

/** Thresholds beyond which `/ready` lists a component as degraded (it never turns the status into 503). Seconds:
  * `workerStalenessSeconds` is how long a background worker may go without successful progress, `maxOutboxAgeSeconds`
  * how old the oldest due operational event may be, and `maxClosureWorkAgeSeconds` how old the oldest pending recruiter
  * job closure may be (above the row retry cap, so one backing-off row is not an alert). The defaults mirror the
  * packaged `application.conf` for an absent section.
  */
final case class ReadinessConfig(
    workerStaleness: FiniteDuration = 60.seconds,
    maxOutboxAge: FiniteDuration = 300.seconds,
    maxClosureWorkAge: FiniteDuration = 1800.seconds,
    reportCache: FiniteDuration = 1000.millis
)

/** Bounds are decoded by the refined types; each failure maps to its `ConfigError` by path. */
private[config] final case class RawReadinessConfig(
    workerStalenessSeconds: ReadinessSeconds,
    maxOutboxAgeSeconds: ReadinessSeconds,
    maxClosureWorkAgeSeconds: ReadinessSeconds,
    reportCacheMillis: ReadinessCacheMillis
) derives ConfigReader

private[config] object ReadinessConfigValidation {
  def read(raw: RawReadinessConfig): ReadinessConfig =
    ReadinessConfig(
      raw.workerStalenessSeconds.seconds,
      raw.maxOutboxAgeSeconds.seconds,
      raw.maxClosureWorkAgeSeconds.seconds,
      raw.reportCacheMillis.millis
    )
}
