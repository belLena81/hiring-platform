package com.example.graphQL.cats.api.http

import cats.effect.{Clock, IO, Ref}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*

final class FixedWindowRateLimiter private (
    state: Ref[IO, Map[FixedWindowRateLimiter.Key, FixedWindowRateLimiter.Bucket]],
    config: AuthRateLimitConfig
) {
  // Buckets are process-local: this mitigates abuse on a single node, not across distributed replicas.
  import FixedWindowRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] =
    Clock[IO].realTime.map(_.toMillis).flatMap { now =>
      state.modify { buckets =>
        val windowMillis = config.windowSeconds.seconds.toMillis
        val currentWindow = now / windowMillis
        val active = buckets.filter { case (_, bucket) => bucket.window == currentWindow }
        val existing = active.get(key)
        val retryAfter = ((currentWindow + 1) * windowMillis - now).millis

        existing match {
          case Some(bucket) if bucket.count >= config.attempts =>
            active -> Left(RateLimited(retryAfter))
          case Some(bucket) =>
            active.updated(key, bucket.copy(count = bucket.count + 1)) -> Right(())
          case None if active.size >= config.maxBuckets =>
            active -> Left(RateLimited(config.windowSeconds.seconds))
          case None =>
            active.updated(key, Bucket(currentWindow, 1)) -> Right(())
        }
      }
    }
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
    Ref.of[IO, Map[Key, Bucket]](Map.empty).map(new FixedWindowRateLimiter(_, config))
}
