package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

final class AuthRateLimiter private (
    windows: Cache[AuthRateLimiter.Key, AtomicInteger],
    config: AuthRateLimitConfig
) {
  import AuthRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] = IO.delay {
    val hits = windows.get(normalize(key), _ => new AtomicInteger(0)).incrementAndGet()
    if (hits <= config.attempts) Right(()) else Left(RateLimited(config.windowSeconds.seconds))
  }

  private def normalize(key: Key): Key =
    key.copy(remoteAddress = key.remoteAddress.map(_.fold(ipv4 => ipv4, ipv6 => Cidr(ipv6, 64).prefix)))
}

object AuthRateLimiter {
  final case class Key(remoteAddress: Option[IpAddress], operation: Operation)
  final case class RateLimited(retryAfter: FiniteDuration) {
    def retryAfterSeconds: Long = math.max(1L, (retryAfter + 999.millis).toSeconds)
  }
  enum Operation { case Login, SignUp, BootstrapAdmin }
  def create(config: AuthRateLimitConfig): IO[AuthRateLimiter] = IO.delay {
    val windows = Caffeine.newBuilder()
      .maximumSize(config.maxBuckets.toLong)
      .expireAfterWrite(java.time.Duration.ofSeconds(config.windowSeconds.toLong))
      .build[Key, AtomicInteger]()
    new AuthRateLimiter(windows, config)
  }
}
