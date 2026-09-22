package com.example.graphQL.cats.api.http

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.graphql.{GraphQLDocumentCache, HiringGraphQLServices, RequestContextFactory}
import com.example.graphQL.cats.shared.HiringHttpPaths
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogFields, ProbeResult}
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.otel4s.trace.Tracer
import scala.concurrent.duration.*

final class HiringApiRoutes(service: HealthService, diagnostics: Diagnostics,
    dependencies: HiringApiRoutes.Dependencies, tracer: Tracer[IO] = Tracer.noop[IO]) {
  private object dsl extends Http4sDsl[IO]
  import dsl.*

  private val probePaths = Set(HiringHttpPaths.HealthPath, HiringHttpPaths.ReadyPath)

  private val healthRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "health" =>
      Ok(io.circe.Json.obj("status" -> io.circe.Json.fromString("UP")))
    case request @ GET -> Root / "ready" =>
      HttpMiddleware.requestId(request).flatMap { correlationId =>
        service.readiness(Some(correlationId)).flatMap {
          case ProbeResult.Ready => Ok(io.circe.Json.obj("status" -> io.circe.Json.fromString("READY")))
          case _ => ServiceUnavailable(io.circe.Json.obj("status" -> io.circe.Json.fromString("NOT_READY")))
        }
      }
  }

  def httpRoutes(config: HiringApiRoutes.HttpConfig): IO[HttpRoutes[IO]] = {
    val graphQL = new GraphQLHttpRoutes(service, diagnostics, dependencies, tracer)
    val onError = (request: Request[IO], failure: Throwable) =>
      graphQL.rejection(HttpRejection.Internal, request, LogFields.failure(failure))
    val onEntityTooLarge = (request: Request[IO]) =>
      graphQL.rejection(HttpRejection.PayloadTooLarge, request)

    for {
      protectedRoutes <- HttpMiddleware(config, graphQL.routes.orNotFound, diagnostics, tracer, onError, onEntityTooLarge)
      probeRoutes <- HttpMiddleware(config, healthRoutes.orNotFound, diagnostics, tracer, onError, onEntityTooLarge,
        applyAdmissionControl = false)
    } yield Kleisli { request =>
      val app = if (probePaths.contains(request.uri.path)) probeRoutes
      else protectedRoutes
      OptionT.liftF(app(request))
    }
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
      clientAddressResolver: ClientAddressResolver
  )

  final case class HttpConfig(admissionPermits: Long, requestTimeout: FiniteDuration)

}
