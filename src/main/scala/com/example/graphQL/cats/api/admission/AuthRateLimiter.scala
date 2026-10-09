package com.example.graphQL.cats.api.admission

import cats.effect.{IO, Resource}
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import com.github.benmanes.caffeine.cache.Ticker

/** Login and sign-up attempts per client network and operation. */
final class AuthRateLimiter private (underlying: FixedWindowRateLimiter[AuthRateLimiter.Key]) {
  import AuthRateLimiter.*

  def permit(key: Key): IO[Either[RateLimited, Unit]] = underlying.permit(key)

  private[admission] def cleanUpAndSize: IO[Long] = underlying.cleanUpAndSize
}

object AuthRateLimiter {
  final case class Key(remoteAddress: Option[IpAddress], operation: Operation)
  type RateLimited = FixedWindowRateLimiter.RateLimited
  val RateLimited: FixedWindowRateLimiter.RateLimited.type = FixedWindowRateLimiter.RateLimited
  enum Operation { case Login, SignUp }

  /** Preferred constructor for composition roots: the limiter is process-local and holds no releasable resources. */
  def resource(config: AuthRateLimitConfig): Resource[IO, AuthRateLimiter] =
    Resource.eval(create(config))

  def create(config: AuthRateLimitConfig): IO[AuthRateLimiter] =
    create(config, Ticker.systemTicker())

  private[admission] def create(config: AuthRateLimitConfig, ticker: Ticker): IO[AuthRateLimiter] =
    FixedWindowRateLimiter
      .create[Key](config.windowSeconds, config.attempts, config.maxBuckets, ticker, normalize)
      .map(new AuthRateLimiter(_))

  // IPv6 clients are limited per /64 so one host cannot multiply its allowance across addresses.
  private def normalize(key: Key): Key =
    key.copy(remoteAddress = key.remoteAddress.map(_.fold(ipv4 => ipv4, ipv6 => Cidr(ipv6, 64).prefix)))
}
