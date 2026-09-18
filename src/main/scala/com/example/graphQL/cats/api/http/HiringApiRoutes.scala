package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogEvent, LogField, LogFields, ProbeResult, TraceContext}
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Allow}
import org.http4s.server.middleware.EntityLimiter
import org.http4s.server.middleware.RequestId
import org.http4s.syntax.all.*
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
  private val GraphQLResponseMediaType = MediaType.unsafeParse("application/graphql-response+json")
  private val dsl = new Http4sDsl[IO] {}
  import dsl.*

  private enum RejectionReason {
    case INVALID_REQUEST, INVALID_QUERY, UNSUPPORTED_MEDIA, NOT_ACCEPTABLE, PAYLOAD_TOO_LARGE,
      OVERLOADED, DEADLINE_EXCEEDED, INTERNAL_ERROR, METHOD_NOT_ALLOWED, NOT_FOUND
  }

  private enum Rejection(val status: Status, val message: String, val reason: RejectionReason) {
    case InvalidRequest extends Rejection(Status.BadRequest, "Invalid GraphQL request", RejectionReason.INVALID_REQUEST)
    case InvalidQuery extends Rejection(Status.BadRequest, "Invalid GraphQL query", RejectionReason.INVALID_QUERY)
    case UnsupportedMedia extends Rejection(Status.UnsupportedMediaType, "Expected application/json", RejectionReason.UNSUPPORTED_MEDIA)
    case NotAcceptable extends Rejection(Status.NotAcceptable, "Expected application/graphql-response+json", RejectionReason.NOT_ACCEPTABLE)
    case PayloadTooLarge extends Rejection(Status.PayloadTooLarge, "Request body too large", RejectionReason.PAYLOAD_TOO_LARGE)
    case Overloaded extends Rejection(Status.ServiceUnavailable, "Server busy", RejectionReason.OVERLOADED)
    case DeadlineExceeded extends Rejection(Status.GatewayTimeout, "Request deadline exceeded", RejectionReason.DEADLINE_EXCEEDED)
    case Internal extends Rejection(Status.InternalServerError, "Request failed", RejectionReason.INTERNAL_ERROR)
    case MethodNotAllowed extends Rejection(Status.MethodNotAllowed, "Use POST", RejectionReason.METHOD_NOT_ALLOWED)
    case NotFound extends Rejection(Status.NotFound, "Not found", RejectionReason.NOT_FOUND)
  }

  private def json(status: Status, body: Json): Response[IO] = Response[IO](status).withEntity(body)

  private def error(status: Status, message: String): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))

  private def graphqlJson(status: Status, body: Json): Response[IO] =
    json(status, body).putHeaders(Header.Raw(CIString("Content-Type"), GraphQLResponseMediaType.toString))

  private def rejected(rejection: Rejection, requestId: String,
      failure: Map[LogField, String] = Map.empty): IO[Response[IO]] =
    Diagnostics.emit(diagnostics, LogEvent.RequestRejected, Some(requestId),
      failure ++ Map(LogField.Reason -> rejection.reason.toString, LogField.Status -> rejection.status.code.toString))
      .as(error(rejection.status, rejection.message))

  private def admitted(requestId: String)(action: IO[Response[IO]]): IO[Response[IO]] =
    admission.permit.use { allowed =>
      if (allowed) action.timeoutTo(5.seconds, rejected(Rejection.DeadlineExceeded, requestId))
      else rejected(Rejection.Overloaded, requestId)
    }

  private def acceptsGraphQLResponse(request: Request[IO]): Boolean =
    request.headers.get[Accept].forall(_.values.exists { entry =>
      entry.mediaRange.satisfiedBy(GraphQLResponseMediaType) && entry.qValue > QValue.Zero
    })

  private def graphql(request: Request[IO], requestId: String, trace: TraceContext): IO[Response[IO]] = admitted(requestId) {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(Rejection.UnsupportedMedia, requestId)
    else if (!acceptsGraphQLResponse(request)) rejected(Rejection.NotAcceptable, requestId)
    else request.attemptAs[GraphQLRequest].foldF(
      _ => rejected(Rejection.InvalidRequest, requestId),
      parsed => {
        def execute(actor: Option[ActorContext], services: Option[HiringGraphQLServices]): IO[Response[IO]] =
          HiringGraphQLSchema.execute(parsed, service, requestId, actor, services, ensureHiringReady, Some(trace)).flatMap {
            case Right(result) => completedGraphQL(parsed, result, requestId)
            case Left(HiringGraphQLSchema.Failure.InvalidQuery) => rejected(Rejection.InvalidQuery, requestId)
            case Left(HiringGraphQLSchema.Failure.Internal) => rejected(Rejection.Internal, requestId)
          }

        authenticate(request).flatMap(actor => execute(actor, hiring))
      }
    )
  }

  private[http] def completedGraphQL(parsed: GraphQLRequest, result: Json, requestId: String): IO[Response[IO]] = {
    val operationName = parsed.operationName.orElse(parsed.document.definitions.collectFirst {
      case operation: sangria.ast.OperationDefinition => operation.name
    }.flatten)
    val fields = Map(LogField.Outcome ->
      (if (result.hcursor.downField("errors").succeeded) "FIELD_ERROR" else "COMPLETED")) ++
      operationName.map(LogField.OperationName -> _)
    Diagnostics.emit(diagnostics, LogEvent.GraphQLCompleted, Some(requestId), fields).as(graphqlJson(Status.Ok, result))
  }

  private def route(request: Request[IO], requestId: String, trace: TraceContext): IO[Response[IO]] =
    HttpRoutes.of[IO] {
      case GET -> Root / "health" => IO.pure(json(Status.Ok, Json.obj("status" -> Json.fromString("UP"))))
      case GET -> Root / "schema.graphql" => IO.pure(Response[IO](Status.Ok).withEntity(HiringGraphQLSchema.sdl)(using EntityEncoder.stringEncoder[IO]))
      case GET -> Root / "ready" => admitted(requestId) {
        service.readiness(Some(requestId)).map { result =>
          val ready = result == ProbeResult.Ready
          json(if (ready) Status.Ok else Status.ServiceUnavailable,
            Json.obj("status" -> Json.fromString(if (ready) "READY" else "NOT_READY")))
        }
      }
      case POST -> Root / "graphql" => graphql(request, requestId, trace)
      case _ -> Root / "graphql" => rejected(Rejection.MethodNotAllowed, requestId)
        .map(_.putHeaders(Allow(Method.POST)))
      case _ => rejected(Rejection.NotFound, requestId)
    }.orNotFound(request)

  private val tracedApp: HttpApp[IO] = Kleisli[IO, Request[IO], Response[IO]] { request =>
    val suppliedRequestId = request.headers.get(CIString("X-Request-ID")).map(_.head.value)
      .filter(value => scala.util.Try(java.util.UUID.fromString(value)).isSuccess)
    suppliedRequestId.fold(IO.randomUUID.map(_.toString))(IO.pure).flatMap { requestId =>
      TraceContext.root(requestId).flatMap { trace =>
      val method = request.method.name
      val path = request.uri.path.renderString
      val metadata = Map(LogField.Method -> (if (LogFields.validPublic(LogField.Method, method)) method else "OTHER"),
        LogField.Route -> (if (LogFields.validPublic(LogField.Route, path)) path else "_unmatched"))
      IO.monotonic.flatMap { started =>
        def timedFields: IO[Map[LogField, String]] = IO.monotonic.map { now =>
          metadata + (LogField.DurationMs -> (now - started).toMillis.toString)
        }
        Diagnostics.spanWith(diagnostics, trace, "http.request", metadata) { requestTrace =>
          route(request, requestId, requestTrace)
          .handleErrorWith {
            case _: EntityLimiter.EntityTooLarge => rejected(Rejection.PayloadTooLarge, requestId)
            case failure => rejected(Rejection.Internal, requestId, LogFields.failure(failure))
          }
          .map(_.putHeaders(
            Header.Raw(CIString("X-Request-ID"), requestId),
            Header.Raw(CIString("X-Content-Type-Options"), "nosniff"),
            Header.Raw(CIString("Cache-Control"), "no-store"),
            Header.Raw(CIString("Content-Security-Policy"),
              "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
          ))
          .flatTap(response => timedFields.flatMap(fields => Diagnostics.emit(diagnostics, LogEvent.RequestCompleted,
            Some(requestId), fields ++ Map(LogField.Status -> response.status.code.toString,
              LogField.Outcome -> (if (response.status.isSuccess) "COMPLETED" else "REJECTED")))))
          .onCancel(timedFields.flatMap(fields => Diagnostics.emit(diagnostics, LogEvent.RequestCancelled,
            Some(requestId), fields ++ Map(LogField.Reason -> "CANCELLED", LogField.Outcome -> "CANCELLED"))))
        }
      }
      }
      }
  }

  private val requestIdApp: HttpApp[IO] = RequestId.httpApp(tracedApp)

  private val rawApp: HttpApp[IO] = Kleisli { request =>
    val validRequestId = request.headers.get(CIString("X-Request-ID")).exists { header =>
      scala.util.Try(java.util.UUID.fromString(header.head.value)).isSuccess
    }
    requestIdApp(if (validRequestId) request else request.removeHeader(CIString("X-Request-ID")))
  }

  val app: HttpApp[IO] = EntityLimiter.httpApp(rawApp, MaxRequestBytes)
}
