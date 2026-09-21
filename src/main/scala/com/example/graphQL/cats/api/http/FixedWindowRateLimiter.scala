package com.example.graphQL.cats.api.http

import cats.effect.{Clock, IO, Ref, Resource, Temporal}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*

final class FixedWindowRateLimiter private (
    state: Ref[IO, FixedWindowRateLimiter.WindowState],
    config: AuthRateLimitConfig,
    clock: Clock[IO]
) {
  // Buckets are process-local: this mitigates abuse on a single node, not across distributed replicas.
  import FixedWindowRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] =
    clock.realTime.map(_.toMillis).flatMap { now =>
      val windowMillis = config.windowSeconds.seconds.toMillis
      val currentWindow = now / windowMillis
      val retryAfter = ((currentWindow + 1) * windowMillis - now).millis
      state.modify { currentState =>
        val activeState =
          if (currentState.window == currentWindow) currentState
          else WindowState(currentWindow, Map.empty)

        activeState.buckets.get(key) match {
          case Some(bucket) if bucket.count >= config.attempts =>
            activeState -> Left(RateLimited(retryAfter))
          case Some(bucket) =>
            activeState.copy(buckets = activeState.buckets.updated(key, bucket.copy(count = bucket.count + 1))) -> Right(())
          case None if activeState.buckets.size >= config.maxBuckets =>
            activeState -> Left(RateLimited(config.windowSeconds.seconds))
          case None =>
            activeState.copy(buckets = activeState.buckets.updated(key, Bucket(1))) -> Right(())
        }
      }
    }

  private[http] def evictExpired: IO[Unit] =
    clock.realTime.map(_.toMillis).flatMap { now =>
      val currentWindow = now / config.windowSeconds.seconds.toMillis
      state.update { currentState =>
        if (currentState.window == currentWindow) currentState
        else WindowState(currentWindow, Map.empty)
      }
    }

  private[http] def evictionLoop(interval: FiniteDuration): IO[Nothing] =
    (Temporal[IO].sleep(interval) *> evictExpired).foreverM
}

object FixedWindowRateLimiter {
  final case class Key(remoteAddress: String, operation: Operation)
  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long =
      math.max(1L, (retryAfter + 999.millis).toSeconds)
  }

  enum Operation {
    case Login, SignUp, BootstrapAdmin
  }

  private[http] final case class Bucket(count: Int)
  private[http] final case class WindowState(window: Long, buckets: Map[Key, Bucket])

  def create(config: AuthRateLimitConfig): IO[FixedWindowRateLimiter] =
    create(config, Clock[IO])

  private[http] def create(config: AuthRateLimitConfig, clock: Clock[IO]): IO[FixedWindowRateLimiter] =
    Ref.of[IO, WindowState](WindowState(Long.MinValue, Map.empty)).map(new FixedWindowRateLimiter(_, config, clock))

  def resource(config: AuthRateLimitConfig): Resource[IO, FixedWindowRateLimiter] =
    resource(config, config.windowSeconds.seconds)

  private[http] def resource(
      config: AuthRateLimitConfig,
      evictionInterval: FiniteDuration
  ): Resource[IO, FixedWindowRateLimiter] =
    Resource.eval(create(config)).flatMap { limiter =>
      Resource.make(limiter.evictionLoop(evictionInterval).start)(_.cancel).as(limiter)
    }
}
