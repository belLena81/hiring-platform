package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, Expiry, Ticker}
import scala.concurrent.duration.*

/** A process-local fixed-window counter per key: the first `attempts` calls in a window pass, later ones are refused
  * until the window ends. The cache is bounded by `maxBuckets` and every bucket expires with its window, so memory use
  * does not grow with the number of distinct keys. Keys never share an allowance.
  */
final class FixedWindowRateLimiter[K] private (
    cache: Cache[K, FixedWindowRateLimiter.Bucket],
    windowSeconds: Int,
    attempts: Int,
    maxBuckets: Int,
    failClosedWhenFull: Boolean,
    ticker: Ticker,
    normalize: K => K
) {
  import FixedWindowRateLimiter.*

  def permit(key: K): IO[Either[RateLimited, Unit]] =
    IO.delay {
      val normalized = normalize(key)
      val now = ticker.read()
      // A full cache would evict some bucket to admit a new key, silently resetting that key's count; when configured,
      // refuse unseen keys instead (after dropping expired buckets) so the limit can never be weakened that way.
      if (failClosedWhenFull && cache.estimatedSize() >= maxBuckets) cache.cleanUp()
      val bucket = cache
        .asMap()
        .compute(
          normalized,
          (_, existing) =>
            Option(existing).filter(_.expiresAtNanos - now > 0L) match {
              case Some(live)                                                        => live.copy(hits = live.hits + 1)
              case None if failClosedWhenFull && cache.estimatedSize() >= maxBuckets => null
              case None => Bucket(1, now + windowSeconds.toLong * NanosPerSecond)
            }
        )
      if (bucket != null && bucket.hits <= attempts) Right(())
      else Left(RateLimited(windowSeconds.seconds))
    }

  private[admission] def cleanUpAndSize: IO[Long] = IO.delay {
    cache.cleanUp()
    cache.estimatedSize()
  }
}

object FixedWindowRateLimiter {
  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long = math.max(1L, (retryAfter + 999.millis).toSeconds)
  }

  private[admission] final case class Bucket(hits: Int, expiresAtNanos: Long)
  private val NanosPerSecond = 1000000000L

  private[admission] def create[K](
      windowSeconds: Int,
      attempts: Int,
      maxBuckets: Int,
      ticker: Ticker,
      normalize: K => K,
      failClosedWhenFull: Boolean = false
  ): IO[FixedWindowRateLimiter[K]] =
    IO.delay(
      new FixedWindowRateLimiter[K](
        buildCache[K](maxBuckets, ticker),
        windowSeconds,
        attempts,
        maxBuckets,
        failClosedWhenFull,
        ticker,
        normalize
      )
    )

  private def buildCache[K](maxBuckets: Int, ticker: Ticker): Cache[K, Bucket] =
    Caffeine
      .newBuilder()
      .maximumSize(maxBuckets.toLong)
      .ticker(ticker)
      .expireAfter(new Expiry[K, Bucket] {
        override def expireAfterCreate(key: K, value: Bucket, currentTime: Long): Long =
          remaining(value, currentTime)

        override def expireAfterUpdate(key: K, value: Bucket, currentTime: Long, currentDuration: Long): Long =
          remaining(value, currentTime)

        override def expireAfterRead(key: K, value: Bucket, currentTime: Long, currentDuration: Long): Long =
          currentDuration

        private def remaining(bucket: Bucket, currentTime: Long): Long =
          math.max(0L, bucket.expiresAtNanos - currentTime)
      })
      .build[K, Bucket]()
}
