package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, Expiry, Ticker}
import scala.concurrent.duration.*

final class AuthRateLimiter private (
    cache: Cache[AuthRateLimiter.Key, AuthRateLimiter.Bucket],
    config: AuthRateLimitConfig,
    ticker: Ticker
) {
  import AuthRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] =
    IO.delay {
      val normalized = normalize(key)
      val now = ticker.read()
      val bucket = cache
        .asMap()
        .compute(
          normalized,
          (_, existing) =>
            if (existing == null || existing.expiresAtNanos - now <= 0L)
              Bucket(1, now + config.windowSeconds.toLong * NanosPerSecond)
            else existing.copy(hits = existing.hits + 1)
        )
      if (bucket.hits <= config.attempts) Right(())
      else Left(RateLimited(config.windowSeconds.seconds))
    }

  private def normalize(key: Key): Key =
    key.copy(remoteAddress = key.remoteAddress.map(_.fold(ipv4 => ipv4, ipv6 => Cidr(ipv6, 64).prefix)))

  private[admission] def cleanUpAndSize: IO[Long] = IO.delay {
    cache.cleanUp()
    cache.estimatedSize()
  }
}

object AuthRateLimiter {
  final case class Key(remoteAddress: Option[IpAddress], operation: Operation)
  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long = math.max(1L, (retryAfter + 999.millis).toSeconds)
  }
  enum Operation { case Login, SignUp, BootstrapAdmin }

  private final case class Bucket(hits: Int, expiresAtNanos: Long)
  private val NanosPerSecond = 1000000000L

  def create(config: AuthRateLimitConfig): IO[AuthRateLimiter] =
    IO.delay {
      val ticker = Ticker.systemTicker()
      new AuthRateLimiter(buildCache(config, ticker), config, ticker)
    }

  private[admission] def create(config: AuthRateLimitConfig, ticker: Ticker): IO[AuthRateLimiter] =
    IO.delay(new AuthRateLimiter(buildCache(config, ticker), config, ticker))

  private def buildCache(config: AuthRateLimitConfig, ticker: Ticker): Cache[Key, Bucket] =
    Caffeine
      .newBuilder()
      .maximumSize(config.maxBuckets.toLong)
      .ticker(ticker)
      .expireAfter(new Expiry[Key, Bucket] {
        override def expireAfterCreate(key: Key, value: Bucket, currentTime: Long): Long =
          remaining(value, currentTime)

        override def expireAfterUpdate(key: Key, value: Bucket, currentTime: Long, currentDuration: Long): Long =
          remaining(value, currentTime)

        override def expireAfterRead(key: Key, value: Bucket, currentTime: Long, currentDuration: Long): Long =
          currentDuration

        private def remaining(bucket: Bucket, currentTime: Long): Long =
          math.max(0L, bucket.expiresAtNanos - currentTime)
      })
      .build[Key, Bucket]()
}
