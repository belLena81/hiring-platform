package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.Semaphore
import scala.concurrent.duration.FiniteDuration

/** Shared, cancellation-safe admission for expensive retrieval roots. Runtime owns allocation. */
final case class DiscoveryQueryPolicy(maxTime: FiniteDuration, permits: Semaphore[IO]) {
  def run[A](query: IO[A]): IO[A] = permits.permit.use(_ => query)
}

object DiscoveryQueryPolicy {
  def create(maxTime: FiniteDuration, concurrency: Int): IO[DiscoveryQueryPolicy] =
    Semaphore[IO](concurrency.toLong).map(DiscoveryQueryPolicy(maxTime, _))
}
