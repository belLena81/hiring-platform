package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import _root_.pureconfig.*

/** Thresholds beyond which `/ready` lists a component as degraded (it never turns the status into 503). Seconds:
  * `workerStalenessSeconds` is how long a background worker may go without successful progress, `maxOutboxAgeSeconds`
  * how old the oldest due operational event may be, and `maxClosureWorkAgeSeconds` how old the oldest pending recruiter
  * job closure may be (above the row retry cap, so one backing-off row is not an alert). The defaults mirror the
  * packaged `application.conf` for an absent section.
  */
final case class ReadinessConfig(
    workerStalenessSeconds: Int = 60,
    maxOutboxAgeSeconds: Int = 300,
    maxClosureWorkAgeSeconds: Int = 1800,
    reportCacheMillis: Int = 1000
)

private[config] final case class RawReadinessConfig(
    workerStalenessSeconds: Int,
    maxOutboxAgeSeconds: Int,
    maxClosureWorkAgeSeconds: Int,
    reportCacheMillis: Int
) derives ConfigReader

private[config] object ReadinessConfigValidation {
  private val Seconds = 1 to 604800
  private val CacheMillis = 100 to 10000

  def read(raw: RawReadinessConfig): ValidatedNel[ConfigError, ReadinessConfig] =
    (
      within(raw.workerStalenessSeconds, ConfigError.InvalidReadinessWorkerStaleness),
      within(raw.maxOutboxAgeSeconds, ConfigError.InvalidReadinessOutboxAge),
      within(raw.maxClosureWorkAgeSeconds, ConfigError.InvalidReadinessClosureWorkAge),
      Either
        .cond(CacheMillis.contains(raw.reportCacheMillis), raw.reportCacheMillis, ConfigError.InvalidReadinessCache)
        .toValidatedNel
    ).mapN(ReadinessConfig.apply)

  private def within(value: Int, error: ConfigError): ValidatedNel[ConfigError, Int] =
    Either.cond(Seconds.contains(value), value, error).toValidatedNel
}
