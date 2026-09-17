package com.example.graphQL.cats.transport.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.transport.graphql.{HiringGraphQLSchema, GraphQLRequest, HiringGraphQLServices}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogEvent, LogField, LogFields, ProbeResult}
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

final class HiringApiRoutes(
    service: HealthService,
    diagnostics: Diagnostics,
    admission: Admission,
    hiring: Option[HiringGraphQLServices] = None,
    authenticate: Request[IO] => IO[Option[ActorContext]] = (_: Request[IO]) => IO.pure(None),
    ensureHiringReady: IO[Boolean] = IO.pure(true)
) {
  private val MaxRequestBytes = 64 * 1024

  private enum RejectionReason {
    case INVALID_REQUEST, INVALID_QUERY, UNSUPPORTED_MEDIA, NOT_ACCEPTABLE, PAYLOAD_TOO_LARGE,
      OVERLOADED, DEADLINE_EXCEEDED, INTERNAL_ERROR, METHOD_NOT_ALLOWED, NOT_FOUND
  }

  private def json(status: Status, body: Json): Response[IO] = Response[IO](status).withEntity(body)

  private def error(status: Status, message: String): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))

  private def rejected(status: Status, message: String, requestId: String, reason: RejectionReason,
      failure: Map[LogField, String] = Map.empty): IO[Response[IO]] =
    Diagnostics.emit(diagnostics, LogEvent.RequestRejected, Some(requestId),
      failure ++ Map(LogField.Reason -> reason.toString, LogField.Status -> status.code.toString)).as(error(status, message))

  private def admitted(requestId: String)(action: IO[Response[IO]]): IO[Response[IO]] =
    admission.permit.use { allowed =>
      if (allowed) action.timeoutTo(5.seconds, rejected(Status.GatewayTimeout, "Request deadline exceeded", requestId,
        RejectionReason.DEADLINE_EXCEEDED))
      else rejected(Status.ServiceUnavailable, "Server busy", requestId, RejectionReason.OVERLOADED)
    }

  private def acceptsJson(request: Request[IO]): Boolean =
    request.headers.get(CIString("Accept")).forall { headers =>
      val entries = headers.toList.flatMap(_.value.split(",", -1)).map { entry =>
        val parts = entry.trim.toLowerCase(java.util.Locale.ROOT).split(";", -1).map(_.trim)
        val specificity = parts.headOption match {
          case Some("application/json") => 2
          case Some("application/*") => 1
          case Some("*/*") => 0
          case _ => -1
        }
        val qualityParameters = parts.drop(1).filter(_.takeWhile(_ != '=').trim == "q")
        val quality = qualityParameters.toList match {
          case Nil => Some(BigDecimal(1))
          case parameter :: Nil =>
            val value = parameter.dropWhile(_ != '=').drop(1).trim
            Option.when(value.matches("(?:0(?:\\.[0-9]{0,3})?|1(?:\\.0{0,3})?)"))(BigDecimal(value))
          case _ => None
        }
        quality.map(specificity -> _)
      }
      entries.forall(_.isDefined) && entries.flatten.filter(_._1 >= 0)
        .maxByOption { case (specificity, quality) => (specificity, quality) }
        .exists(_._2 > 0)
    }

  private def graphql(request: Request[IO], requestId: String): IO[Response[IO]] = admitted(requestId) {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(Status.UnsupportedMediaType, "Expected application/json", requestId, RejectionReason.UNSUPPORTED_MEDIA)
    else if (!acceptsJson(request)) rejected(Status.NotAcceptable, "Expected JSON response media", requestId, RejectionReason.NOT_ACCEPTABLE)
    else request.body.take(MaxRequestBytes.toLong + 1).compile.toVector.flatMap { bytes =>
      if (bytes.size > MaxRequestBytes) rejected(Status.PayloadTooLarge, "Request body too large", requestId, RejectionReason.PAYLOAD_TOO_LARGE)
      else IO(GraphQLRequest.parseBody(new String(bytes.toArray, java.nio.charset.StandardCharsets.UTF_8))).flatMap {
        case None => rejected(Status.BadRequest, "Invalid GraphQL request", requestId, RejectionReason.INVALID_REQUEST)
        case Some(parsed) =>
          def execute(actor: Option[ActorContext], services: Option[HiringGraphQLServices]): IO[Response[IO]] =
            HiringGraphQLSchema.execute(parsed, service, requestId, actor, services, ensureHiringReady).flatMap {
              case Right(result) => completedGraphQL(parsed, result, requestId)
              case Left(HiringGraphQLSchema.Failure.InvalidQuery) => rejected(Status.BadRequest, "Invalid GraphQL query", requestId, RejectionReason.INVALID_QUERY)
              case Left(HiringGraphQLSchema.Failure.Internal) => rejected(Status.InternalServerError, "Request failed", requestId, RejectionReason.INTERNAL_ERROR)
            }

          authenticate(request).flatMap(actor => execute(actor, hiring))
      }
    }
  }

  private[http] def completedGraphQL(parsed: GraphQLRequest, result: Json, requestId: String): IO[Response[IO]] = {
    val operationName = parsed.operationName.orElse(parsed.document.definitions.collectFirst {
      case operation: sangria.ast.OperationDefinition => operation.name
    }.flatten)
    val fields = Map(LogField.Outcome ->
      (if (result.hcursor.downField("errors").succeeded) "FIELD_ERROR" else "COMPLETED")) ++
      operationName.map(LogField.OperationName -> _)
    Diagnostics.emit(diagnostics, LogEvent.GraphQLCompleted, Some(requestId), fields).as(json(Status.Ok, result))
  }

  private def route(request: Request[IO], requestId: String): IO[Response[IO]] =
    (request.method, request.uri.path.renderString) match {
      case (Method.GET, "/health") => IO.pure(json(Status.Ok, Json.obj("status" -> Json.fromString("UP"))))
      case (Method.GET, "/schema.graphql") => IO.pure(Response[IO](Status.Ok).withEntity(HiringGraphQLSchema.sdl))
      case (Method.GET, "/ready") => admitted(requestId) {
        service.readiness(Some(requestId)).map { result =>
          val ready = result == ProbeResult.Ready
          json(if (ready) Status.Ok else Status.ServiceUnavailable,
            Json.obj("status" -> Json.fromString(if (ready) "READY" else "NOT_READY")))
        }
      }
      case (Method.POST, "/graphql") => graphql(request, requestId)
      case (_, "/graphql") => rejected(Status.MethodNotAllowed, "Use POST", requestId, RejectionReason.METHOD_NOT_ALLOWED)
        .map(_.putHeaders(Header.Raw(CIString("Allow"), "POST")))
      case _ => Diagnostics.emit(diagnostics, LogEvent.RequestRejected, Some(requestId),
        Map(LogField.Reason -> RejectionReason.NOT_FOUND.toString, LogField.Status -> "404")).as(Response[IO](Status.NotFound))
    }

  val app: HttpApp[IO] = Kleisli { request =>
    IO.randomUUID.map(_.toString).flatMap { requestId =>
      val method = request.method.name
      val path = request.uri.path.renderString
      val metadata = Map(LogField.Method -> (if (LogFields.validPublic(LogField.Method, method)) method else "OTHER"),
        LogField.Route -> (if (LogFields.validPublic(LogField.Route, path)) path else "_unmatched"))
      IO.monotonic.flatMap { started =>
        def timedFields: IO[Map[LogField, String]] = IO.monotonic.map { now =>
          metadata + (LogField.DurationMs -> (now - started).toMillis.toString)
        }
        route(request, requestId)
        .handleErrorWith(failure => rejected(Status.InternalServerError, "Request failed", requestId,
          RejectionReason.INTERNAL_ERROR, LogFields.failure(failure)))
        .map(_.putHeaders(
          Header.Raw(CIString("X-Request-ID"), requestId),
          Header.Raw(CIString("X-Content-Type-Options"), "nosniff"),
          Header.Raw(CIString("Cache-Control"), "no-store"),
          Header.Raw(CIString("Content-Security-Policy"),
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
        ))
        .flatTap(response => timedFields.flatMap(fields => Diagnostics.emit(diagnostics, LogEvent.RequestCompleted,
          Some(requestId), fields ++ Map(LogField.Status -> response.status.code.toString, LogField.Outcome -> "COMPLETED"))))
        .onCancel(timedFields.flatMap(fields => Diagnostics.emit(diagnostics, LogEvent.RequestCancelled,
          Some(requestId), fields ++ Map(LogField.Reason -> "CANCELLED", LogField.Outcome -> "CANCELLED"))))
      }
    }
  }
}
