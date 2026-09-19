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
        val activeBuckets = buckets.filter { case (_, bucket) => bucket.window == currentWindow }
        activeBuckets.get(key) match {
          case Some(bucket) if bucket.count >= config.attempts =>
            activeBuckets -> Left(RateLimited(retryAfter))
          case Some(bucket) =>
            activeBuckets.updated(key, bucket.copy(count = bucket.count + 1)) -> Right(())
          case None if activeBuckets.size >= config.maxBuckets =>
            activeBuckets -> Left(RateLimited(config.windowSeconds.seconds))
          case None =>
            activeBuckets.updated(key, Bucket(currentWindow, 1)) -> Right(())
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
    case Login, SignUp, BootstrapAdmin
  }

  private[http] final case class Bucket(window: Long, count: Int)

  def create(config: AuthRateLimitConfig): IO[FixedWindowRateLimiter] =
    create(config, Clock[IO])

  private[http] def create(config: AuthRateLimitConfig, clock: Clock[IO]): IO[FixedWindowRateLimiter] =
    Ref.of[IO, Map[Key, Bucket]](Map.empty).map(new FixedWindowRateLimiter(_, config, clock))

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
