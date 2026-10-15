package com.example.graphQL.cats.api.http

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import com.example.graphQL.cats.api.admission.{
  AuthRateLimiter,
  CostBudgetLimiter,
  InterviewActionRateLimiter,
  PersistedQueryPolicy
}
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.graphql.{GraphQLDocumentCache, HiringGraphQLServices, RequestContextFactory}
import com.example.graphQL.cats.shared.HiringHttpPaths
import com.example.graphQL.cats.service.{
  ActorContext,
  Diagnostics,
  HealthService,
  LogFields,
  ProbeResult,
  ReadinessReport
}
import io.circe.Json
import org.http4s.*
import org.http4s.circe.CirceEntityEncoder.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.otel4s.trace.Tracer
import scala.concurrent.duration.*

final class HiringApiRoutes(
    service: HealthService[IO],
    diagnostics: Diagnostics[IO],
    dependencies: HiringApiRoutes.Dependencies,
    tracer: Tracer[IO] = Tracer.noop[IO]
) {
  private object dsl extends Http4sDsl[IO]
  import dsl.*

  private val probePaths = Set(HiringHttpPaths.HealthPath, HiringHttpPaths.ReadyPath)

  private val healthRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "health"          => IO.pure(HttpMiddleware.statusResponse(Status.Ok, "UP"))
    case request @ GET -> Root / "ready" =>
      // Concurrent probes share one cached collection (ReadinessCache), so a flood costs one round trip per window
      // and every caller still receives the real status; nothing is refused here.
      HttpMiddleware.requestId(request).flatMap { correlationId =>
        service.report(Some(correlationId)).map {
          case ReadinessReport(ProbeResult.Ready, degraded) => readyResponse(degraded.map(_.wire))
          case _ => HttpMiddleware.statusResponse(Status.ServiceUnavailable, "NOT_READY")
        }
      }
  }

  /** The status code never depends on `degraded`; the body lists stable component names only. */
  private def readyResponse(degraded: List[String]): Response[IO] =
    Response[IO](Status.Ok).withEntity(
      Json.obj("status" -> Json.fromString("READY"), "degraded" -> Json.fromValues(degraded.map(Json.fromString)))
    )

  def httpRoutes(config: HiringApiRoutes.HttpConfig): IO[HttpRoutes[IO]] = {
    val graphQL = new GraphQLHttpRoutes(diagnostics, dependencies, tracer)
    val onError = (request: Request[IO], failure: Throwable) =>
      graphQL.rejection(HttpRejection.Internal, request, LogFields.failure(failure))
    val onEntityTooLarge = (request: Request[IO]) => graphQL.rejection(HttpRejection.PayloadTooLarge, request)

    val isProbe = (request: Request[IO]) => probePaths.contains(request.uri.path)
    val (probes, guarded) = (healthRoutes.orNotFound, graphQL.routes.orNotFound)
    val routes: HttpApp[IO] = Kleisli(request => (if (isProbe(request)) probes else guarded).run(request))
    HttpMiddleware(config, routes, isProbe, diagnostics, tracer, onError, onEntityTooLarge)
      .map(app => Kleisli(request => OptionT.liftF(app(request))))
  }

  def httpApp(config: HiringApiRoutes.HttpConfig): IO[HttpApp[IO]] =
    httpRoutes(config).map(_.orNotFound)
}

object HiringApiRoutes {
  final case class Dependencies(
      hiring: HiringGraphQLServices,
      authenticate: Kleisli[IO, Request[IO], Either[AuthFailure, Option[ActorContext]]],
      ensureHiringReady: IO[ProbeResult],
      contextFactory: RequestContextFactory,
      documentCache: GraphQLDocumentCache,
      rateLimiter: AuthRateLimiter,
      interviewActionLimiter: InterviewActionRateLimiter.Limiter,
      costBudgetLimiter: CostBudgetLimiter.Limiter,
      clientAddressResolver: ClientAddressResolver,
      discoveryMaxRoots: Int = 4,
      schemaExposure: Boolean = false,
      persistedQueries: PersistedQueryPolicy = PersistedQueryPolicy.Off
  )

  final case class HttpConfig(admissionPermits: Long, requestTimeout: FiniteDuration)

}
