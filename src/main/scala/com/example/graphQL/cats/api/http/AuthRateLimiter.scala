package com.example.graphQL.cats.api.http

import cats.effect.IO
import cats.effect.std.{MapRef, Mutex}
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.config.AuthRateLimitConfig
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*

final class AuthRateLimiter private (
    cache: ConcurrentHashMap[AuthRateLimiter.Key, AuthRateLimiter.Bucket],
    buckets: MapRef[IO, AuthRateLimiter.Key, Option[AuthRateLimiter.Bucket]],
    admission: Mutex[IO],
    config: AuthRateLimitConfig
) {
  import AuthRateLimiter.*

  // Established keys update independently; only a new key takes the bounded-cache admission lock.
  def permit(key: Key): IO[Either[RateLimited, Unit]] =
    IO.monotonic.flatMap(now => permitAt(key, now.toNanos))

  private def permitAt(key: Key, nowNanos: Long): IO[Either[RateLimited, Unit]] =
    buckets(key).get.flatMap {
      case Some(_) => updateExisting(key, nowNanos)
      case None => admit(key, nowNanos)
    }

  private def updateExisting(key: Key, nowNanos: Long): IO[Either[RateLimited, Unit]] =
    buckets(key).modify {
      case Some(bucket) =>
        val (updated, result) = bucket.consume(nowNanos, config)
        Some(updated) -> Some(result)
      case None => None -> None
    }.flatMap {
      case Some(result) => IO.pure(result)
      case None => permitAt(key, nowNanos)
    }

  private def admit(key: Key, nowNanos: Long): IO[Either[RateLimited, Unit]] =
    admission.lock.surround {
      buckets(key).get.flatMap {
        case Some(_) => updateExisting(key, nowNanos)
        case None =>
          IO.delay {
            if (cache.size() >= config.maxBuckets) {
              val keys = cache.keySet().iterator()
              if (keys.hasNext) {
                cache.remove(keys.next())
                ()
              }
            }
          } *> buckets(key).set(Some(Bucket(config.attempts - 1, nowNanos))).as(Right(()))
      }
    }
}

object AuthRateLimiter {
  final case class Key(remoteAddress: Option[IpAddress], operation: Operation)

  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long =
      math.max(1L, (retryAfter + 999.millis).toSeconds)
  }

  enum Operation {
    case Login, SignUp, BootstrapAdmin
  }

  private[http] final case class Bucket(available: Int, refilledAtNanos: Long) {
    def consume(nowNanos: Long, config: AuthRateLimitConfig): (Bucket, Either[RateLimited, Unit]) = {
      val intervalNanos = config.windowSeconds.seconds.toNanos
      val elapsedNanos = math.max(0L, nowNanos - refilledAtNanos)
      val refillBoundary = if (elapsedNanos >= intervalNanos)
        nowNanos - (elapsedNanos % intervalNanos)
      else refilledAtNanos
      val tokens = if (elapsedNanos >= intervalNanos) config.attempts else available

      if (tokens > 0)
        Bucket(tokens - 1, refillBoundary) -> Right(())
      else {
        val retryAfter = (intervalNanos - math.max(0L, nowNanos - refillBoundary)).nanos
        Bucket(tokens, refillBoundary) -> Left(RateLimited(retryAfter))
      }
    }
  }

  def create(config: AuthRateLimitConfig): IO[AuthRateLimiter] =
    for {
      cache <- IO.delay(new ConcurrentHashMap[Key, Bucket]())
      buckets = MapRef.fromConcurrentHashMap[IO, Key, Bucket](cache)
      admission <- Mutex[IO]
    } yield new AuthRateLimiter(cache, buckets, admission, config)
}
