package com.example.graphQL.cats.api.http

import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, RequestContextParameters}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogEvent, LogField}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.Rejection as LogRejection
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Allow, `Content-Type`, `Retry-After`, `WWW-Authenticate`}
import org.http4s.server.AuthMiddleware
import org.typelevel.otel4s.trace.Tracer

private[http] enum HttpRejection(val status: Status, val message: String, val reason: LogRejection) {
  case InvalidRequest extends HttpRejection(Status.BadRequest, "Invalid GraphQL request", LogRejection.InvalidRequest)
  case InvalidQuery extends HttpRejection(Status.BadRequest, "Invalid GraphQL query", LogRejection.InvalidQuery)
  case UnsupportedMedia extends HttpRejection(Status.UnsupportedMediaType, "Expected application/json", LogRejection.UnsupportedMedia)
  case NotAcceptable extends HttpRejection(Status.NotAcceptable,
    s"Expected ${MediaTypeNegotiation.supportedMessage}", LogRejection.NotAcceptable)
  case AuthenticationFailed extends HttpRejection(Status.Unauthorized, "Authentication failed", LogRejection.AuthenticationFailed)
  case Unavailable extends HttpRejection(Status.ServiceUnavailable, "Service unavailable", LogRejection.InternalError)
  case RateLimited extends HttpRejection(Status.TooManyRequests, "Too many authentication attempts", LogRejection.RateLimited)
  case PayloadTooLarge extends HttpRejection(Status.PayloadTooLarge, "Request body too large", LogRejection.PayloadTooLarge)
  case Overloaded extends HttpRejection(Status.ServiceUnavailable, "Server busy", LogRejection.Overloaded)
  case DeadlineExceeded extends HttpRejection(Status.GatewayTimeout, "Request deadline exceeded", LogRejection.DeadlineExceeded)
  case Internal extends HttpRejection(Status.InternalServerError, "Request failed", LogRejection.InternalError)
  case MethodNotAllowed extends HttpRejection(Status.MethodNotAllowed, "Use POST", LogRejection.MethodNotAllowed)
  case NotFound extends HttpRejection(Status.NotFound, "Not found", LogRejection.NotFound)
}

private[http] final class GraphQLHttpRoutes(service: HealthService, diagnostics: Diagnostics,
    dependencies: HiringApiRoutes.Dependencies, tracer: Tracer[IO] = Tracer.noop[IO]) {
  private val dsl = new Http4sDsl[IO] {}
  import dsl.*

  private def json(status: Status, body: Json): Response[IO] =
    Response[IO](status).withEntity(body)(using jsonEncoderOf[IO, Json])

  private def error(status: Status, message: String, mediaType: MediaType): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))
      .putHeaders(`Content-Type`(mediaType))

  private def graphqlJson(status: Status, body: Json, mediaType: MediaType): Response[IO] =
    json(status, body).putHeaders(`Content-Type`(mediaType))

  private def rejected(rejection: HttpRejection, requestId: String,
      failure: Map[LogField, String] = Map.empty,
      mediaType: MediaType = MediaType.application.json): IO[Response[IO]] =
    diagnostics.emit(LogEvent.RequestRejected, Some(requestId), fields =
      failure ++ Map(LogField.Reason -> rejection.reason.reason, LogField.Status -> rejection.status.code.toString))
      .as(error(rejection.status, rejection.message, mediaType))

  private def graphql(request: Request[IO], requestId: String, mediaType: MediaType,
      actor: Option[ActorContext]): IO[Response[IO]] = {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(HttpRejection.UnsupportedMedia, requestId, mediaType = mediaType)
    else {
      request.attemptAs[GraphQLRequest](using jsonOf[IO, GraphQLRequest]).foldF(
        _ => rejected(HttpRejection.InvalidRequest, requestId, mediaType = mediaType),
        parsed => {
          def limited(operation: AuthRateLimiter.Operation): IO[Either[Response[IO], Unit]] =
            dependencies.rateLimiter.permit(AuthRateLimiter.Key(dependencies.clientAddressResolver.resolve(request), operation)).flatMap {
              case Right(()) => IO.pure(Right(()))
              case Left(rateLimited) =>
                rejected(HttpRejection.RateLimited, requestId)
                  .map(_.putHeaders(`Retry-After`.unsafeFromLong(rateLimited.retryAfterSeconds)))
                  .map(Left(_))
            }

          def execute: IO[Response[IO]] = {
            val context = dependencies.contextFactory.resource(RequestContextParameters(
              service.readiness(Some(requestId)), actor, dependencies.hiring, dependencies.ensureHiringReady,
              tracer, diagnostics, Some(requestId)))
            HiringGraphQLSchema.execute(parsed, context).flatMap {
              case Right(result) => completedGraphQL(parsed, result, requestId, mediaType)
              case Left(HiringGraphQLSchema.Failure.InvalidQuery) => rejected(HttpRejection.InvalidQuery, requestId, mediaType = mediaType)
              case Left(HiringGraphQLSchema.Failure.Internal) => rejected(HttpRejection.Internal, requestId, mediaType = mediaType)
            }
          }

          accountOperation(parsed) match {
            case AccountOperation.Single(operation) =>
              limited(operation).flatMap {
                case Left(response) => IO.pure(response)
                case Right(()) => execute
              }
            case AccountOperation.Multiple => rejected(HttpRejection.InvalidQuery, requestId, mediaType = mediaType)
            case AccountOperation.None => execute
          }
        }
      )
    }
  }

  private enum AccountOperation {
    case None
    case Single(operation: AuthRateLimiter.Operation)
    case Multiple
  }

  private object AccountOperation {
    val byFieldName: Map[String, AuthRateLimiter.Operation] = Map(
      "signUp" -> AuthRateLimiter.Operation.SignUp,
      "login" -> AuthRateLimiter.Operation.Login,
      "bootstrapAdmin" -> AuthRateLimiter.Operation.BootstrapAdmin
    )
  }

  private def accountOperation(request: GraphQLRequest): AccountOperation = {
    val operations = request.document.definitions.collect { case operation: sangria.ast.OperationDefinition => operation }
    val selected = request.operationName match {
      case Some(name) => operations.find(_.name.contains(name))
      case None => Option.when(operations.size == 1)(operations.head)
    }

    def sensitiveFields(selections: Vector[sangria.ast.Selection], expanding: Set[String]): Vector[AuthRateLimiter.Operation] =
      selections.foldLeft(Vector.empty[AuthRateLimiter.Operation]) { (found, selection) =>
        if (found.size >= 2) found
        else {
          val next = selection match {
            case field: sangria.ast.Field => AccountOperation.byFieldName.get(field.name).toVector
            case inline: sangria.ast.InlineFragment => sensitiveFields(inline.selections, expanding)
            case spread: sangria.ast.FragmentSpread if !expanding(spread.name) =>
              request.document.fragments.get(spread.name).toVector.flatMap(fragment =>
                sensitiveFields(fragment.selections, expanding + spread.name))
            case _ => Vector.empty
          }
          (found ++ next).take(2)
        }
      }

    selected.map(operation => sensitiveFields(operation.selections, Set.empty)) match {
      case Some(Vector(operation)) => AccountOperation.Single(operation)
      case Some(fields) if fields.size >= 2 => AccountOperation.Multiple
      case _ => AccountOperation.None
    }
  }

  private[http] def completedGraphQL(parsed: GraphQLRequest, result: Json, requestId: String,
      mediaType: MediaType = MediaTypeNegotiation.graphqlResponse): IO[Response[IO]] = {
    val operationName = parsed.operationName.orElse(parsed.document.definitions.collectFirst {
      case operation: sangria.ast.OperationDefinition => operation.name
    }.flatten)
    val fields = Map(LogField.Outcome ->
      (if (result.hcursor.downField("errors").succeeded) "FIELD_ERROR" else "COMPLETED")) ++
      operationName.map(LogField.OperationName -> _)
    diagnostics.emit(LogEvent.GraphQLCompleted, Some(requestId), fields = fields)
      .as(graphqlJson(Status.Ok, result, mediaType))
  }

  private def correlationId(request: Request[IO]): IO[String] =
    tracer.currentSpanContext.map(_.fold(
      request.attributes.lookup(org.http4s.server.middleware.RequestId.requestIdAttrKey).getOrElse("unknown")
    )(_.traceIdHex))

  private val authenticationFailures: AuthedRoutes[AuthFailure, IO] = AuthedRoutes.of {
    case request as failure =>
      correlationId(request).flatMap { requestId =>
        MediaTypeNegotiation.selectResponseMediaType(request.headers.get[Accept]) match {
          case None => rejected(HttpRejection.NotAcceptable, requestId)
          case Some(mediaType) =>
            val rejection = failure match {
              case AuthFailure.Unavailable => HttpRejection.Unavailable
              case _ => HttpRejection.AuthenticationFailed
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
      case request @ POST -> Root / "graphql" as actor =>
        correlationId(request.req).flatMap { requestId =>
          MediaTypeNegotiation.selectResponseMediaType(request.req.headers.get[Accept]) match {
            case Some(mediaType) => graphql(request.req, requestId, mediaType, actor)
            case None => rejected(HttpRejection.NotAcceptable, requestId)
          }
        }
    }
    val middleware: AuthMiddleware[IO, Option[ActorContext]] =
      AuthMiddleware(dependencies.authenticate, authenticationFailures)
    Kleisli { request =>
      if (request.method == Method.POST && request.uri.path.renderString == "/graphql") middleware(graphqlRoutes)(request)
      else OptionT.none[IO, Response[IO]]
    }
  }

  def routes: HttpRoutes[IO] = {
    val schema = HttpRoutes.of[IO] {
      case GET -> Root / "schema.graphql" =>
        IO.pure(Response[IO](Status.Ok).withEntity(HiringGraphQLSchema.sdl)(using EntityEncoder.stringEncoder[IO]))
    }
    val fallback = HttpRoutes.of[IO] {
      case request @ _ -> Root / "graphql" => correlationId(request).flatMap { requestId =>
        rejected(HttpRejection.MethodNotAllowed, requestId).map(_.putHeaders(Allow(Method.POST)))
      }
      case request => correlationId(request).flatMap { requestId => rejected(HttpRejection.NotFound, requestId) }
    }
    schema <+> authenticatedGraphQL <+> fallback
  }

  private[http] def rejection(rejection: HttpRejection, request: Request[IO],
      failure: Map[LogField, String] = Map.empty): IO[Response[IO]] =
    correlationId(request).flatMap(id => rejected(rejection, id, failure))
}
