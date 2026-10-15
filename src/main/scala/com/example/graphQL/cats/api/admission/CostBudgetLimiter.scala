package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.CostBudgetConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.github.benmanes.caffeine.cache.Ticker

/** Query cost budget: GraphQL complexity charged per authenticated actor, or per client network when anonymous, within
  * a fixed window. Actors and anonymous clients have separate bucket capacity, so anonymous key flooding cannot take
  * capacity from authenticated actors. When a capacity is full, unseen keys are charged to shared overflow counters
  * rather than refused, and no tracked bucket is ever evicted, so a caller with an exhausted budget cannot be reset.
  */
final class CostBudgetLimiter private (
    actors: FixedWindowRateLimiter[UserId],
    clients: FixedWindowRateLimiter[Option[IpAddress]]
) {
  import CostBudgetLimiter.*

  def acquireNow(key: Key, cost: Int): Either[RateLimited, Unit] =
    key match {
      case Key.Actor(id)       => actors.acquireNow(id, cost)
      case Key.Client(address) => clients.acquireNow(address, cost)
    }

  private[admission] def actorLimiter: FixedWindowRateLimiter[UserId] = actors
  private[admission] def clientLimiter: FixedWindowRateLimiter[Option[IpAddress]] = clients
}

object CostBudgetLimiter {
  type RateLimited = FixedWindowRateLimiter.RateLimited
  type Limiter = CostBudgetLimiter

  enum Key {
    case Actor(id: UserId)
    case Client(address: Option[IpAddress])
  }

  def create(config: CostBudgetConfig): IO[Limiter] =
    create(config, Ticker.systemTicker())

  private[admission] def create(config: CostBudgetConfig, ticker: Ticker): IO[Limiter] =
    for {
      actors <- FixedWindowRateLimiter.create[UserId](
        config.windowSeconds,
        config.maxCost,
        config.maxActorBuckets,
        ticker,
        identity,
        config.overflowBuckets,
        "cost-budget-actor"
      )
      clients <- FixedWindowRateLimiter.create[Option[IpAddress]](
        config.windowSeconds,
        config.maxCost,
        config.maxClientBuckets,
        ticker,
        normalize,
        config.overflowBuckets,
        "cost-budget-client"
      )
    } yield new CostBudgetLimiter(actors, clients)

  // IPv6 clients share one budget per /64, as for authentication attempts.
  private def normalize(address: Option[IpAddress]): Option[IpAddress] =
    address.map(_.fold(ipv4 => ipv4, ipv6 => Cidr(ipv6, 64).prefix))
}
