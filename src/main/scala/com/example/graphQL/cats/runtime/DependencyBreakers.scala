package com.example.graphQL.cats.runtime

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.{CircuitBreakerConfig, ResilienceConfig}
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.Settings
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.resilience.{CircuitBreaker, CircuitBreakerDiagnostics, CircuitBreakerName}
import scala.concurrent.duration.*

/** The process-local breakers guarding outbound dependencies. */
final case class DependencyBreakers(
    voyage: CircuitBreaker[IO],
    atlasSearch: CircuitBreaker[IO],
    kafkaPublisher: CircuitBreaker[IO],
    mongoWrites: CircuitBreaker[IO]
)

object DependencyBreakers {
  def create(config: ResilienceConfig, diagnostics: Diagnostics[IO]): IO[DependencyBreakers] =
    (
      build(CircuitBreakerName.Voyage, config.voyage, diagnostics),
      build(CircuitBreakerName.AtlasSearch, config.atlasSearch, diagnostics),
      build(CircuitBreakerName.KafkaPublisher, config.kafkaPublisher, diagnostics),
      build(CircuitBreakerName.MongoWrites, config.mongoWrites, diagnostics)
    ).mapN(DependencyBreakers.apply)

  def settings(config: CircuitBreakerConfig): Settings =
    Settings(
      config.failureThreshold,
      config.windowMs.millis,
      config.openMs.millis,
      config.maxOpenMs.millis,
      config.halfOpenProbes
    )

  private def build(
      name: CircuitBreakerName,
      config: CircuitBreakerConfig,
      diagnostics: Diagnostics[IO]
  ): IO[CircuitBreaker[IO]] =
    if (!config.enabled) CircuitBreakerDiagnostics.disabled(name, diagnostics).as(CircuitBreaker.disabled[IO])
    else CircuitBreaker.create[IO](settings(config), CircuitBreakerDiagnostics.listener(name, diagnostics))
}
