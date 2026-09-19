package com.example.graphQL.cats.api.http

import cats.effect.{Clock, IO, Ref, Resource, Temporal}
import cats.syntax.all.*
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*

final class FixedWindowRateLimiter private (
    state: Ref[IO, Map[FixedWindowRateLimiter.Key, FixedWindowRateLimiter.Bucket]],
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
      state.modify { buckets =>
        buckets.get(key).filter(_.window == currentWindow) match {
          case Some(bucket) if bucket.count >= config.attempts =>
            buckets -> Left(RateLimited(retryAfter))
          case Some(bucket) =>
            buckets.updated(key, bucket.copy(count = bucket.count + 1)) -> Right(())
          case None if buckets.size >= config.maxBuckets =>
            buckets -> Left(RateLimited(config.windowSeconds.seconds))
          case None =>
            buckets.updated(key, Bucket(currentWindow, 1)) -> Right(())
        }
      }
    }

  private[http] def evictExpired: IO[Unit] =
    clock.realTime.map(_.toMillis).flatMap { now =>
      val currentWindow = now / config.windowSeconds.seconds.toMillis
      state.update(_.filter { case (_, bucket) => bucket.window == currentWindow })
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
    case Login, SignUp
  }

  private[http] final case class Bucket(window: Long, count: Int)

  def create(config: AuthRateLimitConfig): IO[FixedWindowRateLimiter] =
    Ref.of[IO, Map[Key, Bucket]](Map.empty).map(new FixedWindowRateLimiter(_, config, Clock[IO]))

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
