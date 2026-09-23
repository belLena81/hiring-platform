package com.example.graphQL.cats.api.http

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, RequestContextParameters}
import com.example.graphQL.cats.shared.HiringHttpPaths
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogEvent, LogField}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.FailureReason
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Allow, `Content-Type`, `WWW-Authenticate`}
import org.http4s.server.AuthMiddleware
import org.typelevel.otel4s.trace.Tracer

private[http] enum HttpRejection(val status: Status, val message: String, val reason: FailureReason) {
  case InvalidRequest extends HttpRejection(Status.BadRequest, "Invalid GraphQL request", FailureReason.InvalidRequest)
  case InvalidQuery extends HttpRejection(Status.BadRequest, "Invalid GraphQL query", FailureReason.InvalidQuery)
  case UnsupportedMedia
      extends HttpRejection(Status.UnsupportedMediaType, "Expected application/json", FailureReason.UnsupportedMedia)
  case NotAcceptable
      extends HttpRejection(
        Status.NotAcceptable,
        s"Expected ${MediaTypeNegotiation.supportedMessage}",
        FailureReason.NotAcceptable
      )
  case AuthenticationFailed
      extends HttpRejection(Status.Unauthorized, "Authentication failed", FailureReason.AuthenticationFailed)
  case Unavailable extends HttpRejection(Status.ServiceUnavailable, "Service unavailable", FailureReason.InternalError)
  case PayloadTooLarge
      extends HttpRejection(Status.PayloadTooLarge, "Request body too large", FailureReason.PayloadTooLarge)
  case Internal extends HttpRejection(Status.InternalServerError, "Request failed", FailureReason.InternalError)
  case MethodNotAllowed extends HttpRejection(Status.MethodNotAllowed, "Use POST", FailureReason.MethodNotAllowed)
  case NotFound extends HttpRejection(Status.NotFound, "Not found", FailureReason.NotFound)
}

private[http] final class GraphQLHttpRoutes(
    service: HealthService,
    diagnostics: Diagnostics,
    dependencies: HiringApiRoutes.Dependencies,
    tracer: Tracer[IO] = Tracer.noop[IO]
) {
  private object dsl extends Http4sDsl[IO]
  import dsl.*

  private def json(status: Status, body: Json): Response[IO] =
    Response[IO](status).withEntity(body)(using jsonEncoderOf[IO, Json])

  private def error(status: Status, message: String, mediaType: MediaType): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))
      .putHeaders(`Content-Type`(mediaType))

  private def graphqlJson(status: Status, body: Json, mediaType: MediaType): Response[IO] =
    json(status, body).putHeaders(`Content-Type`(mediaType))

  private def rejected(
      rejection: HttpRejection,
      requestId: String,
      failure: Map[LogField, String] = Map.empty,
      mediaType: MediaType = MediaType.application.json
  ): IO[Response[IO]] =
    diagnostics
      .emit(
        LogEvent.RequestRejected,
        Some(requestId),
        fields =
          failure ++ Map(LogField.Reason -> rejection.reason.reason, LogField.Status -> rejection.status.code.toString)
      )
      .as(error(rejection.status, rejection.message, mediaType))

  private def graphql(
      request: Request[IO],
      requestId: String,
      mediaType: MediaType,
      actor: Option[ActorContext]
  ): IO[Response[IO]] = {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(HttpRejection.UnsupportedMedia, requestId, mediaType = mediaType)
    else {
      request
        .attemptAs[GraphQLRequest](using jsonOf[IO, GraphQLRequest])
        .foldF(
          _ => rejected(HttpRejection.InvalidRequest, requestId, mediaType = mediaType),
          parsed =>
            dependencies.documentCache.document(parsed.query).flatMap {
              case Left(_)         => rejected(HttpRejection.InvalidQuery, requestId, mediaType = mediaType)
              case Right(document) => {
                val context = dependencies.contextFactory.resource(
                  RequestContextParameters(
                    service.readiness(Some(requestId)),
                    actor,
                    dependencies.hiring,
                    dependencies.ensureHiringReady,
                    tracer,
                    diagnostics,
                    Some(requestId),
                    dependencies.clientAddressResolver.resolve(request),
                    key => dependencies.rateLimiter.permit(key)
                  )
                )
                context
                  .use(HiringGraphQLSchema.executeInContext(parsed, document, _))
                  .flatTap {
                    case Right(_) if !document.cached =>
                      dependencies.documentCache.store(parsed.query, document.document)
                    case _ => IO.unit
                  }
                  .flatMap {
                    case Right(result) =>
                      completedGraphQL(parsed, result, requestId, mediaType, Some(document.document))
                    case Left(HiringGraphQLSchema.Failure.InvalidQuery) =>
                      rejected(HttpRejection.InvalidQuery, requestId, mediaType = mediaType)
                    case Left(HiringGraphQLSchema.Failure.Internal) =>
                      rejected(HttpRejection.Internal, requestId, mediaType = mediaType)
                  }
              }
            }
        )
    }
  }

  private[http] def completedGraphQL(
      parsed: GraphQLRequest,
      result: Json,
      requestId: String,
      mediaType: MediaType = MediaTypeNegotiation.graphqlResponse,
      document: Option[sangria.ast.Document] = None
  ): IO[Response[IO]] = {
    val operationName = document.flatMap(_.operation(parsed.operationName).flatMap(_.name)).orElse(parsed.operationName)
    val fields = Map(
      LogField.Outcome ->
        (if (result.hcursor.downField("errors").succeeded) "FIELD_ERROR" else "COMPLETED")
    ) ++
      operationName.map(LogField.OperationName -> _)
    diagnostics
      .emit(LogEvent.GraphQLCompleted, Some(requestId), fields = fields)
      .as(graphqlJson(Status.Ok, result, mediaType))
  }

  private val authenticationFailures: AuthedRoutes[AuthFailure, IO] = AuthedRoutes.of { case request as failure =>
    HttpMiddleware.requestId(request).flatMap { requestId =>
      MediaTypeNegotiation.selectResponseMediaType(request.headers.get[Accept]) match {
        case None            => rejected(HttpRejection.NotAcceptable, requestId)
        case Some(mediaType) =>
          val rejection = failure match {
            case AuthFailure.Unavailable => HttpRejection.Unavailable
            case _                       => HttpRejection.AuthenticationFailed
          }
          rejected(rejection, requestId, mediaType = mediaType).map { response =>
            if (failure == AuthFailure.Unavailable) response
            else response.putHeaders(`WWW-Authenticate`(Challenge("Bearer", "hiring")))
          }
      }
    }
  }

  private val authenticatedGraphQL: HttpRoutes[IO] = {
    val graphqlRoutes: AuthedRoutes[Option[ActorContext], IO] = AuthedRoutes.of {
      // Method and path are already enforced by the outer Kleisli guard below.
      case request as actor =>
        HttpMiddleware.requestId(request).flatMap { requestId =>
          MediaTypeNegotiation.selectResponseMediaType(request.headers.get[Accept]) match {
            case Some(mediaType) => graphql(request, requestId, mediaType, actor)
            case None            => rejected(HttpRejection.NotAcceptable, requestId)
          }
        }
    }
    val middleware: AuthMiddleware[IO, Option[ActorContext]] =
      AuthMiddleware(dependencies.authenticate, authenticationFailures)
    Kleisli { request =>
      // AuthMiddleware authenticates every request passed to it. Keep this guard before
      // the middleware so unrelated requests fall through instead of returning 401.
      if (request.method == Method.POST && request.uri.path == HiringHttpPaths.GraphQLPath)
        middleware(graphqlRoutes)(request)
      else OptionT.none[IO, Response[IO]]
    }
  }

  def routes: HttpRoutes[IO] = {
    val schema = HttpRoutes.of[IO] { case GET -> Root / "schema.graphql" =>
      Ok(HiringGraphQLSchema.sdl)
    }
    val fallback = HttpRoutes.of[IO] {
      case request if request.uri.path == HiringHttpPaths.GraphQLPath =>
        HttpMiddleware.requestId(request).flatMap { requestId =>
          rejected(HttpRejection.MethodNotAllowed, requestId).map(_.putHeaders(Allow(Method.POST)))
        }
      case request =>
        HttpMiddleware.requestId(request).flatMap { requestId => rejected(HttpRejection.NotFound, requestId) }
    }
    schema <+> authenticatedGraphQL <+> fallback
  }

  private[http] def rejection(
      rejection: HttpRejection,
      request: Request[IO],
      failure: Map[LogField, String] = Map.empty
  ): IO[Response[IO]] =
    HttpMiddleware.requestId(request).flatMap(id => rejected(rejection, id, failure))
}
