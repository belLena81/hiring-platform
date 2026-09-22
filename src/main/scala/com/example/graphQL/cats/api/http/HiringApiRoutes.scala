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
import org.typelevel.otel4s.trace.Tracer
import scala.concurrent.duration.*

final class HiringApiRoutes(service: HealthService, diagnostics: Diagnostics,
    dependencies: HiringApiRoutes.Dependencies, tracer: Tracer[IO] = Tracer.noop[IO]) {
  private val probePaths = Set(HiringHttpPaths.Health, HiringHttpPaths.Ready)

  private val healthRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case request if request.method == Method.GET && request.uri.path.renderString == HiringHttpPaths.Health =>
      IO.pure(Response[IO](Status.Ok).withEntity(io.circe.Json.obj("status" -> io.circe.Json.fromString("UP")))
        (using jsonEncoderOf[IO, io.circe.Json]))
    case request if request.method == Method.GET && request.uri.path.renderString == HiringHttpPaths.Ready =>
      HttpMiddleware.requestId(request).flatMap { correlationId =>
        service.readiness(Some(correlationId)).map { result =>
          val ready = result == ProbeResult.Ready
          Response[IO](if (ready) Status.Ok else Status.ServiceUnavailable)
            .withEntity(io.circe.Json.obj("status" -> io.circe.Json.fromString(if (ready) "READY" else "NOT_READY")))
            (using jsonEncoderOf[IO, io.circe.Json])
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
      val app = if (probePaths.contains(request.uri.path.renderString)) probeRoutes
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
