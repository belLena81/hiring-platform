package com.example.graphQL.cats.service

import cats.effect.{Clock, Deferred, GenConcurrent, Ref}
import cats.effect.kernel.Poll
import cats.effect.syntax.all.*
import cats.syntax.all.*
import scala.concurrent.duration.FiniteDuration

/** Short-lived, single-flight cache for the readiness report: concurrent probes share one underlying collection, and a
  * result is reused for `ttl`, so an unauthenticated probe flood costs the database one round trip per window and emits
  * at most one set of readiness diagnostics per window. A failed collection is shared with the callers waiting on it
  * but never cached.
  */
final class ReadinessCache[F[_], A] private (
    ttl: FiniteDuration,
    state: Ref[F, ReadinessCache.State[F, A]]
)(using F: GenConcurrent[F, Throwable], clock: Clock[F]) {
  import ReadinessCache.State

  def get(compute: F[A]): F[A] =
    clock.monotonic.flatMap { now =>
      Deferred[F, Either[Throwable, A]].flatMap { mine =>
        F.uncancelable { poll =>
          state
            .modify[F[A]] {
              case State.Fresh(at, value) if now - at <= ttl => (State.Fresh(at, value), F.pure(value))
              case State.Flying(other)                       => (State.Flying(other), wait(compute, other, poll))
              case _                                         => (State.Flying(mine), lead(compute, mine, poll))
            }
            .flatten
        }
      }
    }

  private def wait(compute: F[A], other: Deferred[F, Either[Throwable, A]], poll: Poll[F]): F[A] =
    poll(other.get).flatMap {
      case Left(ReadinessCache.Abandoned) => get(compute)
      case result                         => F.fromEither(result)
    }

  private def lead(compute: F[A], mine: Deferred[F, Either[Throwable, A]], poll: Poll[F]): F[A] =
    poll(compute).attempt
      .flatMap { result =>
        clock.monotonic.flatMap(done =>
          state.set(result.fold(_ => State.Empty[F, A](), value => State.Fresh[F, A](done, value))) *>
            mine.complete(result)
        ) *> F.fromEither(result)
      }
      .onCancel(state.set(State.Empty[F, A]()) *> mine.complete(Left(ReadinessCache.Abandoned)).void)
}

object ReadinessCache {
  private[service] enum State[F[_], A] {
    case Empty()
    case Fresh(at: FiniteDuration, value: A)
    case Flying(done: Deferred[F, Either[Throwable, A]])
  }

  /** The leader was cancelled; waiting callers see an ordinary failure and the next probe computes again. */
  private[service] case object Abandoned
      extends RuntimeException("readiness collection abandoned")
      with scala.util.control.NoStackTrace

  def create[F[_]: Clock, A](ttl: FiniteDuration)(using F: GenConcurrent[F, Throwable]): F[ReadinessCache[F, A]] =
    Ref.of[F, State[F, A]](State.Empty()).map(new ReadinessCache[F, A](ttl, _))
}
