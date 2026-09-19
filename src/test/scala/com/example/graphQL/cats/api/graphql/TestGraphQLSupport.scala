package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.http.{FixedWindowRateLimiter, HiringApiRoutes}
import com.example.graphQL.cats.config.AuthRateLimitConfig
import com.example.graphQL.cats.service.{ActorContext, ProbeResult}
import org.http4s.Request

import scala.concurrent.duration.*

object TestGraphQLSupport {
  val cursorCodec: CursorCodec =
    CursorCodec.fromSecret("test-cursor-secret-01234567890123456789")

  val emptyServices: HiringGraphQLServices =
    RequestContext.emptyServices

  def context(
      probe: IO[ProbeResult],
      actor: Option[ActorContext] = None,
      hiring: HiringGraphQLServices = RequestContext.emptyServices,
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  ): Resource[IO, RequestContext] =
    RequestContextFactory.resource.flatMap(_.resource(probe, actor, hiring, hiringReady))

  def dependencies(
      hiring: HiringGraphQLServices = RequestContext.emptyServices,
      authenticate: Request[IO] => IO[Either[AuthFailure, Option[ActorContext]]] = _ => IO.pure(Right(None)),
      hiringReady: IO[ProbeResult] = IO.pure(ProbeResult.Ready),
      authRateLimit: AuthRateLimitConfig = AuthRateLimitConfig(60, 100, 1000),
      requestTimeout: FiniteDuration = 5.seconds
  ): Resource[IO, HiringApiRoutes.Dependencies] =
    for {
      factory <- RequestContextFactory.resource
      limiter <- Resource.eval(FixedWindowRateLimiter.create(authRateLimit))
    } yield HiringApiRoutes.Dependencies(hiring, authenticate, hiringReady, factory, limiter, requestTimeout)
}
