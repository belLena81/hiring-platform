package com.example.graphQL.cats.api.admission

import cats.effect.{Clock, IO, Ref}
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*

final class AuthRateLimiter private (
    state: Ref[IO, AuthRateLimiter.State],
    config: AuthRateLimitConfig,
    clock: Clock[IO]
) {
  import AuthRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] =
    for {
      now <- clock.monotonic
      result <- state.modify { current =>
        val active = current.buckets.filter { case (_, bucket) => now < bucket.expiresAt }
        val normalized = normalize(key)
        val nextAccess = current.nextAccess
        val (nextBuckets, hits) = active.get(normalized) match {
          case Some(bucket) =>
            (active.updated(normalized, bucket.copy(hits = bucket.hits + 1, lastAccess = nextAccess)), bucket.hits + 1)
          case None =>
            val bounded =
              if (active.size < config.maxBuckets) active
              else active - active.minBy(_._2.lastAccess)._1
            val bucket = Bucket(1, now + config.windowSeconds.seconds, nextAccess)
            (bounded.updated(normalized, bucket), bucket.hits)
        }
        val next = State(nextBuckets, nextAccess + 1L)
        val outcome = if (hits <= config.attempts) Right(()) else Left(RateLimited(config.windowSeconds.seconds))
        (next, outcome)
      }
    } yield result

  private def normalize(key: Key): Key =
    key.copy(remoteAddress = key.remoteAddress.map(_.fold(ipv4 => ipv4, ipv6 => Cidr(ipv6, 64).prefix)))
}

object AuthRateLimiter {
  final case class Key(remoteAddress: Option[IpAddress], operation: Operation)
  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long = math.max(1L, (retryAfter + 999.millis).toSeconds)
  }
  enum Operation { case Login, SignUp, BootstrapAdmin }

  private final case class Bucket(hits: Int, expiresAt: FiniteDuration, lastAccess: Long)
  private final case class State(buckets: Map[Key, Bucket], nextAccess: Long)

  def create(config: AuthRateLimitConfig): IO[AuthRateLimiter] =
    create(config, Clock[IO])

  def create(config: AuthRateLimitConfig, clock: Clock[IO]): IO[AuthRateLimiter] =
    Ref.of[IO, State](State(Map.empty, 0L)).map(new AuthRateLimiter(_, config, clock))
}
