package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.{FoundationSchema, GraphQLRequest, InputBudget}
import com.example.graphQL.cats.application.{Diagnostics, HealthService, LogEvent, ProbeResult}
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

final class FoundationRoutes(service: HealthService, diagnostics: Diagnostics, admission: Admission) {
  private def json(status: Status, body: Json): Response[IO] = Response[IO](status).withEntity(body)

  private def error(status: Status, message: String): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))

  private def rejected(status: Status, message: String, requestId: String): IO[Response[IO]] =
    diagnostics.event(LogEvent.RequestRejected, Some(requestId)).as(error(status, message))

  private def admitted(requestId: String)(action: IO[Response[IO]]): IO[Response[IO]] =
    admission.permit.use { allowed =>
      if (allowed) action.timeoutTo(5.seconds, rejected(Status.GatewayTimeout, "Request deadline exceeded", requestId))
      else rejected(Status.ServiceUnavailable, "Server busy", requestId)
    }

  private def acceptsJson(request: Request[IO]): Boolean =
    request.headers.get(CIString("Accept")).forall(_.toList.flatMap(_.value.split(',')).exists { entry =>
      val parts = entry.trim.toLowerCase(java.util.Locale.ROOT).split(';').map(_.trim)
      Set("*/*", "application/*", "application/json").contains(parts.headOption.getOrElse("")) &&
        !parts.drop(1).exists(parameter => parameter.startsWith("q=") && parameter.drop(2).toDoubleOption.contains(0.0))
    })

  private def graphql(request: Request[IO], requestId: String): IO[Response[IO]] = admitted(requestId) {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(Status.UnsupportedMediaType, "Expected application/json", requestId)
    else if (!acceptsJson(request)) rejected(Status.NotAcceptable, "Expected JSON response media", requestId)
    else request.body.take(InputBudget.MaxBytes.toLong + 1).compile.toVector.flatMap { bytes =>
      if (bytes.size > InputBudget.MaxBytes) rejected(Status.PayloadTooLarge, "Request body too large", requestId)
      else IO(GraphQLRequest.parseBody(new String(bytes.toArray, java.nio.charset.StandardCharsets.UTF_8))).flatMap {
        case None => rejected(Status.BadRequest, "Invalid GraphQL request", requestId)
        case Some(parsed) => FoundationSchema.execute(parsed, service, requestId).flatMap {
          case Right(result) => IO.pure(json(Status.Ok, result))
          case Left(FoundationSchema.Failure.InvalidQuery) => rejected(Status.BadRequest, "Invalid GraphQL query", requestId)
          case Left(FoundationSchema.Failure.Internal) => rejected(Status.InternalServerError, "Request failed", requestId)
        }
      }
    }
  }

  private def route(request: Request[IO], requestId: String): IO[Response[IO]] =
    (request.method, request.uri.path.renderString) match {
      case (Method.GET, "/health") => IO.pure(json(Status.Ok, Json.obj("status" -> Json.fromString("UP"))))
      case (Method.GET, "/schema.graphql") => IO.pure(Response[IO](Status.Ok).withEntity(FoundationSchema.sdl))
      case (Method.GET, "/ready") => admitted(requestId) {
        service.readiness(Some(requestId)).map { result =>
          val ready = result == ProbeResult.Ready
          json(if (ready) Status.Ok else Status.ServiceUnavailable,
            Json.obj("status" -> Json.fromString(if (ready) "READY" else "NOT_READY")))
        }
      }
      case (Method.POST, "/graphql") => graphql(request, requestId)
      case (_, "/graphql") => IO.pure(error(Status.MethodNotAllowed, "Use POST").putHeaders(Header.Raw(CIString("Allow"), "POST")))
      case _ => IO.pure(Response[IO](Status.NotFound))
    }

  val app: HttpApp[IO] = Kleisli { request =>
    IO.randomUUID.map(_.toString).flatMap { requestId =>
      route(request, requestId)
        .handleErrorWith(_ => rejected(Status.InternalServerError, "Request failed", requestId))
        .map(_.putHeaders(
          Header.Raw(CIString("X-Request-ID"), requestId),
          Header.Raw(CIString("X-Content-Type-Options"), "nosniff"),
          Header.Raw(CIString("Cache-Control"), "no-store"),
          Header.Raw(CIString("Content-Security-Policy"),
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
        ))
    }
  }
}
