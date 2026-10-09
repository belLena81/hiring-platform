package com.example.hiring.analytics.config

import com.example.hiring.analytics.errors.AnalyticsError
import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import _root_.pureconfig.{ConfigObjectSource, ConfigSource}
import _root_.pureconfig.backend.ErrorUtil

import scala.jdk.CollectionConverters.*

/** Test-only environment layer: `${?NAME}` placeholders in `hocon` resolve against `env`, never the real process. Parse
  * and substitution failures reach the loaders as the same `ConfigReaderFailures` that `ConfigSource.default` would
  * produce, so diagnostics are exercised through production code.
  */
object AnalyticsConfigFixtures {
  def source(hocon: String, env: Map[String, String] = Map.empty): ConfigSource =
    ConfigObjectSource(
      ErrorUtil.unsafeToReaderResult(
        ConfigFactory
          .parseString(hocon)
          .withFallback(ConfigFactory.parseMap(env.asJava))
          .resolve(ConfigResolveOptions.noSystem())
      )
    )

  def batch(hocon: String, env: Map[String, String] = Map.empty): Either[AnalyticsError, AnalyticsBatchSettings] =
    AnalyticsRuntimeConfig.readBatch(source(hocon, env))

  def worker(hocon: String, env: Map[String, String] = Map.empty): Either[AnalyticsError, AnalyticsWorkerSettings] =
    AnalyticsRuntimeConfig.readWorker(source(hocon, env))

  def streamingRuntime(
      hocon: String,
      env: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsStreamingRuntimeSettings] =
    AnalyticsRuntimeConfig.readStreaming(source(hocon, env))

  def streaming(
      hocon: String,
      env: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsStreamingSettings] =
    AnalyticsConfigReaders.complete(AnalyticsStreamingSettings.read(source(hocon, env)))

  def lateFactReplay(
      hocon: String,
      env: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsLateFactReplaySettings] =
    AnalyticsRuntimeConfig.readLateFactReplay(source(hocon, env))

  def keyRetirementAudit(
      hocon: String,
      env: Map[String, String] = Map.empty
  ): Either[AnalyticsError, AnalyticsKeyRetirementAuditSettings] =
    AnalyticsRuntimeConfig.readKeyRetirementAudit(source(hocon, env))
}
