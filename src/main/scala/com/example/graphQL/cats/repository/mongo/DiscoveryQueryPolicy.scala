package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.effect.std.Semaphore
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

/** Shared, cancellation-safe admission for expensive retrieval roots. Runtime owns allocation. An open breaker rejects
  * before a permit is taken, so a failing search deployment never queues callers.
  */
final case class DiscoveryQueryPolicy(
    maxTime: FiniteDuration,
    permits: Semaphore[IO],
    breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
) {

  /** Fails with `DiscoveryQueryPolicy.Rejected` while the breaker is open; the effect is not evaluated then. */
  def run[A](query: IO[A]): IO[A] =
    breaker
      .protect(permits.permit.use(_ => query))(
        CircuitBreaker.classifier[IO, A](DiscoveryQueryPolicy.countsAgainstBreaker)
      )
      .flatMap(_.fold(_ => IO.raiseError(DiscoveryQueryPolicy.Rejected), IO.pure))
}

object DiscoveryQueryPolicy {

  /** The breaker is open: callers map this to their existing unavailable outcome. */
  case object Rejected extends RuntimeException("discovery unavailable") with NoStackTrace

  /** Only an unreachable deployment counts. `maxTime` expiry and query-shape errors depend on the caller's query, so
    * one user must not be able to open the breaker for everyone.
    */
  def countsAgainstBreaker(error: Throwable): Boolean = MongoErrors.isDependencyUnavailable(error)

  /** Failures that already mean `Unavailable` to a repository caller. */
  def isUnavailable(error: Throwable): Boolean = error == Rejected || countsAgainstBreaker(error)

  def create(
      maxTime: FiniteDuration,
      concurrency: Int,
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): IO[DiscoveryQueryPolicy] =
    Semaphore[IO](concurrency.toLong).map(DiscoveryQueryPolicy(maxTime, _, breaker))
}
