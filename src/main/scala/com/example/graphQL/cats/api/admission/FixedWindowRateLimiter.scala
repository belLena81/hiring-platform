package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, Expiry, Ticker}
import java.util.concurrent.atomic.{AtomicLong, AtomicReferenceArray}
import org.slf4j.LoggerFactory
import java.security.SecureRandom
import scala.util.hashing.MurmurHash3
import scala.concurrent.duration.*

/** A process-local fixed-window counter per key: calls charging up to `attempts` in total within a window pass, later
  * ones are refused until the window ends. A plain `permit` charges 1; `acquire` charges a caller-chosen weight, so the
  * same bucket can enforce either a call count or a cost budget. A refused call still counts toward the window.
  *
  * Memory is bounded and every bucket expires with its window. The cache has no size-based eviction, so a counted key
  * can never be reset by key flooding. Once `maxBuckets` keys are tracked, unseen keys are charged to one of
  * `overflowBuckets` shared counters, chosen by a per-instance randomly seeded hash so a caller cannot aim at another
  * key's counter, with the same window semantics. Overflow degrades to a shared allowance that can itself be exhausted:
  * an attacker who fills the cache and then spends each counter's allowance makes the unseen keys hashing to the
  * exhausted counters limited for the window. Tracked keys, and keys already exhausted, are unaffected. Concurrent
  * first-time inserts can overshoot `maxBuckets` by at most the number of racing threads; this cannot evict or reset
  * any bucket. Expired buckets are swept at most once per second while the cache is at capacity, not on every call.
  */
final class FixedWindowRateLimiter[K] private (
    cache: Cache[K, FixedWindowRateLimiter.Bucket],
    windowSeconds: Int,
    attempts: Int,
    maxBuckets: Int,
    overflow: AtomicReferenceArray[FixedWindowRateLimiter.Bucket],
    seed: Int,
    label: String,
    ticker: Ticker,
    normalize: K => K,
    shared: K => Boolean
) {
  import FixedWindowRateLimiter.*

  private val lastSweepNanos = new AtomicLong(NeverRecorded)
  private val lastOverflowLogNanos = new AtomicLong(NeverRecorded)
  private val overflowCount = new AtomicLong(0L)
  private val windowNanos = windowSeconds.toLong * NanosPerSecond

  def permit(key: K): IO[Either[RateLimited, Grant]] = acquire(key, 1)

  /** Charges `cost` (at least 1) against the key's window. A refusal reports the time left in that window. */
  def acquire(key: K, cost: Int): IO[Either[RateLimited, Grant]] = IO.delay(acquireNow(key, cost))

  /** The same charge as a bounded in-memory step with no I/O, for synchronous framework hooks (Sangria reducers) that
    * cannot sequence an `IO`. Everything else uses `acquire`. An admitted call returns a `Grant` computed from the
    * caller's own bucket after the charge; a key charged to a shared overflow counter gets `Grant.Shared`, with no
    * numbers, because they would describe other callers' consumption. A key the limiter was created to treat as
    * `shared` (one bucket for callers that cannot be told apart) is charged normally but also gets `Grant.Shared`.
    */
  def acquireNow(key: K, cost: Int): Either[RateLimited, Grant] = {
    val weight = math.max(1, cost).toLong
    val normalized = normalize(key)
    val now = ticker.read()
    if (cache.estimatedSize() >= maxBuckets) sweepIfDue(now)
    val tracked = Option(charge(normalized, weight, now, admitNew = cache.estimatedSize() < maxBuckets))
    val bucket = tracked.getOrElse(chargeOverflow(overflow, normalized, weight, now))
    val remainingNanos = math.max(0L, bucket.expiresAtNanos - now)
    val grant =
      if (tracked.isDefined && !shared(key))
        Grant.Tracked(attempts, math.max(0L, attempts.toLong - bucket.hits), secondsUp(remainingNanos))
      else Grant.Shared
    if (bucket.hits <= attempts.toLong) Right(grant)
    else Left(RateLimited(remainingNanos.nanos, grant))
  }

  /** Charges a tracked bucket; `null` when the key is unseen and new buckets are not admitted. */
  private def charge(key: K, weight: Long, now: Long, admitNew: Boolean): Bucket =
    cache
      .asMap()
      .compute(
        key,
        (_, existing) =>
          if (existing == null) { if (admitNew) Bucket(weight, now + windowNanos) else null }
          else if (existing.expiresAtNanos - now > 0L) existing.copy(hits = saturatingAdd(existing.hits, weight))
          else Bucket(weight, now + windowNanos)
      )

  // The seed is random per instance and never logged; the Murmur finalizer spreads weak or attacker-shaped hash codes.
  private[admission] def stripeOf(key: K): Int =
    Math.floorMod(MurmurHash3.finalizeHash(MurmurHash3.mix(seed, normalize(key).hashCode), 1), overflow.length())

  private def chargeOverflow(stripes: AtomicReferenceArray[Bucket], key: K, weight: Long, now: Long): Bucket = {
    val index = stripeOf(key)
    noteOverflow(now)
    stripes.updateAndGet(
      index,
      existing =>
        if (existing != null && existing.expiresAtNanos - now > 0L)
          existing.copy(hits = saturatingAdd(existing.hits, weight))
        else Bucket(weight, now + windowNanos)
    )
  }

  private def sweepIfDue(now: Long): Unit = {
    val last = lastSweepNanos.get()
    if ((last == NeverRecorded || now - last >= SweepIntervalNanos) && lastSweepNanos.compareAndSet(last, now))
      cache.cleanUp()
  }

  // Counts overflow use and logs at most once per window; the message carries the limiter name only, never a key.
  private def noteOverflow(now: Long): Unit = {
    overflowCount.incrementAndGet()
    val last = lastOverflowLogNanos.get()
    if ((last == NeverRecorded || now - last >= windowNanos) && lastOverflowLogNanos.compareAndSet(last, now))
      logger.warn("Rate limiter {} is at bucket capacity; unseen keys share overflow counters", label)
  }

  /** Charges that were routed to the shared overflow counters since start. */
  private[admission] def overflowCharges: Long = overflowCount.get()

  private[admission] def cleanUpAndSize: IO[Long] = IO.delay {
    cache.cleanUp()
    cache.estimatedSize()
  }
}

object FixedWindowRateLimiter {

  /** What an admitted or refused call may reveal about its allowance. `Tracked` comes from the caller's own bucket and
    * is per process. `Shared` means the key was charged to an overflow counter, which reflects other callers, so it
    * carries no numbers.
    */
  enum Grant {
    case Tracked(limit: Int, remaining: Long, resetsInSeconds: Long)
    case Shared
  }

  /** A refusal; `grant` is `Tracked` with `remaining = 0` for a caller's own bucket. */
  final case class RateLimited(retryAfter: FiniteDuration, grant: Grant) {

    /** The remaining window rounded up to whole seconds, at least 1. */
    def retryAfterSeconds: Long = secondsUp(retryAfter.toNanos)
  }

  private def secondsUp(nanos: Long): Long = math.max(1L, (nanos + NanosPerSecond - 1L) / NanosPerSecond)

  private[admission] final case class Bucket(hits: Long, expiresAtNanos: Long)
  private val NanosPerSecond = 1000000000L
  private val SweepIntervalNanos = NanosPerSecond
  private val NeverRecorded = Long.MinValue
  private val logger = LoggerFactory.getLogger("hiring.admission")

  private def saturatingAdd(left: Long, right: Long): Long = {
    val sum = left + right
    if (sum < 0L) Long.MaxValue else sum
  }

  private[admission] def create[K](
      windowSeconds: Int,
      attempts: Int,
      maxBuckets: Int,
      ticker: Ticker,
      normalize: K => K,
      overflowBuckets: Int,
      label: String,
      shared: K => Boolean = (_: K) => false
  ): IO[FixedWindowRateLimiter[K]] =
    IO.delay(
      // Seeding is an effect: it happens at creation, never in the pure bucket arithmetic.
      new FixedWindowRateLimiter[K](
        buildCache[K](ticker),
        windowSeconds,
        attempts,
        maxBuckets,
        new AtomicReferenceArray[Bucket](overflowBuckets),
        new SecureRandom().nextInt(),
        label,
        ticker,
        normalize,
        shared
      )
    )

  private def buildCache[K](ticker: Ticker): Cache[K, Bucket] = {
    val builder = Caffeine.newBuilder().ticker(ticker)
    builder
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
}
