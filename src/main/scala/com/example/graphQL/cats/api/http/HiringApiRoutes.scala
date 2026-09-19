package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Unique
import com.example.graphQL.cats.api.auth.AuthFailure
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, HiringGraphQLServices, RequestContextFactory}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService, LogEvent, LogField, LogFields, ProbeResult, TraceContext}
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Allow, `Content-Type`}
import org.http4s.server.middleware.EntityLimiter
import org.http4s.server.middleware.RequestId
import org.http4s.syntax.all.*
import org.typelevel.ci.CIString
import org.typelevel.vault.Key
import scala.concurrent.duration.*

final class HiringApiRoutes(service: HealthService, diagnostics: Diagnostics, admission: Admission,
    dependencies: HiringApiRoutes.Dependencies) {
  private val MaxRequestBytes = 64 * 1024
  private val dsl = new Http4sDsl[IO] {}
  import dsl.*

  // These stable uppercase values are part of the public diagnostics schema and intentionally differ from case names.
  private enum RejectionReason {
    case INVALID_REQUEST, INVALID_QUERY, UNSUPPORTED_MEDIA, NOT_ACCEPTABLE, PAYLOAD_TOO_LARGE,
      AUTHENTICATION_FAILED, RATE_LIMITED, OVERLOADED, DEADLINE_EXCEEDED, INTERNAL_ERROR, METHOD_NOT_ALLOWED, NOT_FOUND
  }

  private enum Rejection(val status: Status, val message: String, val reason: RejectionReason) {
    case InvalidRequest extends Rejection(Status.BadRequest, "Invalid GraphQL request", RejectionReason.INVALID_REQUEST)
    case InvalidQuery extends Rejection(Status.BadRequest, "Invalid GraphQL query", RejectionReason.INVALID_QUERY)
    case UnsupportedMedia extends Rejection(Status.UnsupportedMediaType, "Expected application/json", RejectionReason.UNSUPPORTED_MEDIA)
    case NotAcceptable extends Rejection(Status.NotAcceptable,
      s"Expected ${HiringApiRoutes.SupportedResponseMediaTypesMessage}", RejectionReason.NOT_ACCEPTABLE)
    case AuthenticationFailed extends Rejection(Status.Unauthorized, "Authentication failed", RejectionReason.AUTHENTICATION_FAILED)
    case RateLimited extends Rejection(Status.TooManyRequests, "Too many authentication attempts", RejectionReason.RATE_LIMITED)
    case PayloadTooLarge extends Rejection(Status.PayloadTooLarge, "Request body too large", RejectionReason.PAYLOAD_TOO_LARGE)
    case Overloaded extends Rejection(Status.ServiceUnavailable, "Server busy", RejectionReason.OVERLOADED)
    case DeadlineExceeded extends Rejection(Status.GatewayTimeout, "Request deadline exceeded", RejectionReason.DEADLINE_EXCEEDED)
    case Internal extends Rejection(Status.InternalServerError, "Request failed", RejectionReason.INTERNAL_ERROR)
    case MethodNotAllowed extends Rejection(Status.MethodNotAllowed, "Use POST", RejectionReason.METHOD_NOT_ALLOWED)
    case NotFound extends Rejection(Status.NotFound, "Not found", RejectionReason.NOT_FOUND)
  }

  private def json(status: Status, body: Json): Response[IO] = Response[IO](status).withEntity(body)

  private def error(status: Status, message: String, mediaType: MediaType): Response[IO] =
    json(status, Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message)))))
      .putHeaders(`Content-Type`(mediaType))

  private def graphqlJson(status: Status, body: Json, mediaType: MediaType): Response[IO] =
    json(status, body).putHeaders(`Content-Type`(mediaType))

  private def rejected(rejection: Rejection, requestId: String,
      failure: Map[LogField, String] = Map.empty,
      mediaType: MediaType = MediaType.application.json): IO[Response[IO]] =
    Diagnostics.emit(diagnostics, LogEvent.RequestRejected, Some(requestId),
      failure ++ Map(LogField.Reason -> rejection.reason.toString, LogField.Status -> rejection.status.code.toString))
      .as(error(rejection.status, rejection.message, mediaType))

  private def admitted(requestId: String)(action: IO[Response[IO]]): IO[Response[IO]] =
    admission.permit.use { allowed =>
      // Sangria leaf actions run as Futures; this deadline cannot cancel them after the bridge.
      if (allowed) action.timeoutTo(dependencies.requestTimeout, rejected(Rejection.DeadlineExceeded, requestId))
      else rejected(Rejection.Overloaded, requestId)
    }

  private def responseMediaType(request: Request[IO]): Option[MediaType] =
    request.headers.get[Accept].fold(Option(HiringApiRoutes.SupportedResponseMediaTypes.head._1)) { accept =>
      HiringApiRoutes.SupportedResponseMediaTypes.flatMap { case (mediaType, declarationIndex) =>
        accept.values.toList.flatMap { entry =>
          Option.when(entry.mediaRange.satisfiedBy(mediaType) && entry.qValue > QValue.Zero)(
            (mediaType, entry.qValue, declarationIndex))
        }
      }.maxByOption { case (_, quality, declarationIndex) => (quality, -declarationIndex) }.map(_._1)
    }

  private def graphql(request: Request[IO], requestId: String, trace: TraceContext,
      mediaType: MediaType): IO[Response[IO]] = {
    if (!request.contentType.exists(_.mediaType == MediaType.application.json))
      rejected(Rejection.UnsupportedMedia, requestId, mediaType = mediaType)
    else admitted(requestId) {
      request.attemptAs[GraphQLRequest].foldF(
      _ => rejected(Rejection.InvalidRequest, requestId, mediaType = mediaType),
      parsed => {
        def limited(operation: FixedWindowRateLimiter.Operation): IO[Either[Response[IO], Unit]] =
          dependencies.rateLimiter.permit(FixedWindowRateLimiter.Key(dependencies.clientAddressResolver.resolve(request), operation)).flatMap {
            case Right(()) => IO.pure(Right(()))
            case Left(rateLimited) =>
              rejected(Rejection.RateLimited, requestId)
                .map(_.putHeaders(Header.Raw(CIString("Retry-After"), rateLimited.retryAfterSeconds.toString)))
                .map(Left(_))
          }

        def execute(actor: Option[ActorContext]): IO[Response[IO]] =
          HiringGraphQLSchema.execute(parsed, service, requestId, actor, dependencies.hiring,
            dependencies.ensureHiringReady, dependencies.contextFactory, Some(trace)).flatMap {
            case Right(result) => completedGraphQL(parsed, result, requestId, mediaType)
            case Left(HiringGraphQLSchema.Failure.InvalidQuery) => rejected(Rejection.InvalidQuery, requestId, mediaType = mediaType)
            case Left(HiringGraphQLSchema.Failure.Internal) => rejected(Rejection.Internal, requestId, mediaType = mediaType)
          }

        accountOperation(parsed) match {
          case Some(operation) =>
            limited(operation).flatMap {
              case Left(response) => IO.pure(response)
              case Right(()) => authenticateAndExecute(request, requestId, mediaType, execute)
            }
          case None => authenticateAndExecute(request, requestId, mediaType, execute)
        }
      }
      )
    }
  }

  private def authenticateAndExecute(request: Request[IO], requestId: String, mediaType: MediaType,
      execute: Option[ActorContext] => IO[Response[IO]]): IO[Response[IO]] =
    dependencies.authenticate(request).flatMap {
      case Left(_) => rejected(Rejection.AuthenticationFailed, requestId, mediaType = mediaType)
        .map(_.putHeaders(Header.Raw(CIString("WWW-Authenticate"), "Bearer")))
      case Right(actor) => execute(actor)
    }

  private def accountOperation(request: GraphQLRequest): Option[FixedWindowRateLimiter.Operation] =
    def containsField(
        selections: Vector[sangria.ast.Selection],
        visited: Set[String],
        name: String
    ): Boolean =
      selections.zipWithIndex.exists { case (selection, index) =>
        val branchVisited = visited ++ selections.take(index).collect {
          case spread: sangria.ast.FragmentSpread => spread.name
        }
        selection match {
        case field: sangria.ast.Field => field.name == name
        case spread: sangria.ast.FragmentSpread if !branchVisited(spread.name) =>
          request.document.fragments.get(spread.name).exists { fragment =>
            containsField(fragment.selections, branchVisited + spread.name, name)
          }
        case inline: sangria.ast.InlineFragment => containsField(inline.selections, branchVisited, name)
        case _ => false
        }
      }
    def hasField(name: String): Boolean =
      request.document.definitions.collect { case operation: sangria.ast.OperationDefinition => operation.selections }
        .exists(containsField(_, Set.empty, name))

    List(
      "signUp" -> FixedWindowRateLimiter.Operation.SignUp,
      "login" -> FixedWindowRateLimiter.Operation.Login
    ).collectFirst { case (name, operation) if hasField(name) => operation }

  private[http] def completedGraphQL(parsed: GraphQLRequest, result: Json, requestId: String,
      mediaType: MediaType = HiringApiRoutes.GraphQLResponseMediaType): IO[Response[IO]] = {
    val operationName = parsed.operationName.orElse(parsed.document.definitions.collectFirst {
      case operation: sangria.ast.OperationDefinition => operation.name
    }.flatten)
    val fields = Map(LogField.Outcome ->
      (if (result.hcursor.downField("errors").succeeded) "FIELD_ERROR" else "COMPLETED")) ++
      operationName.map(LogField.OperationName -> _)
    Diagnostics.emit(diagnostics, LogEvent.GraphQLCompleted, Some(requestId), fields).as(graphqlJson(Status.Ok, result, mediaType))
  }

  private def requestIdOf(request: Request[IO]): String =
    request.attributes.lookup(RequestId.requestIdAttrKey).getOrElse("unknown")

  private final case class RequestScope(requestId: String, trace: TraceContext)

  private val requestScopeKey: Key[RequestScope] = new Key[RequestScope](new Unique.Token())

  private def requestScope(request: Request[IO]): IO[RequestScope] =
    IO.fromOption(request.attributes.lookup(requestScopeKey))(
      new IllegalStateException("Missing HTTP request scope"))

  private val routedApp: HttpApp[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "health" =>
      IO.pure(json(Status.Ok, Json.obj("status" -> Json.fromString("UP"))))
    case GET -> Root / "schema.graphql" =>
      IO.pure(Response[IO](Status.Ok).withEntity(HiringGraphQLSchema.sdl)(using EntityEncoder.stringEncoder[IO]))
    case request @ GET -> Root / "ready" => requestScope(request).flatMap { scope =>
      admitted(scope.requestId) {
        service.readiness(Some(scope.requestId)).map { result =>
          val ready = result == ProbeResult.Ready
          json(if (ready) Status.Ok else Status.ServiceUnavailable,
            Json.obj("status" -> Json.fromString(if (ready) "READY" else "NOT_READY")))
        }
      }
    }
    case request @ POST -> Root / "graphql" => requestScope(request).flatMap { scope =>
      responseMediaType(request) match {
        case Some(mediaType) => graphql(request, scope.requestId, scope.trace, mediaType)
        case None => rejected(Rejection.NotAcceptable, scope.requestId)
      }
    }
    case request @ _ -> Root / "graphql" => requestScope(request).flatMap { scope =>
      rejected(Rejection.MethodNotAllowed, scope.requestId)
        .map(_.putHeaders(Allow(Method.POST)))
    }
    case request => requestScope(request).flatMap { scope =>
      rejected(Rejection.NotFound, scope.requestId)
    }
  }.orNotFound

  private val tracedApp: HttpApp[IO] = Kleisli[IO, Request[IO], Response[IO]] { request =>
    val requestId = requestIdOf(request)
    IO.randomUUID.flatMap(traceId => TraceContext.root(traceId.toString)).flatMap { trace =>
      val method = request.method.name
      val path = request.uri.path.renderString
      val metadata = Map(LogField.Method -> (if (LogFields.validPublic(LogField.Method, method)) method else "OTHER"),
        LogField.Route -> (if (LogFields.validPublic(LogField.Route, path)) path else "_unmatched"))
      IO.monotonic.flatMap { started =>
        def timedFields: IO[Map[LogField, String]] = IO.monotonic.map { now =>
          metadata + (LogField.DurationMs -> (now - started).toMillis.toString)
        }
        Diagnostics.spanWith(diagnostics, trace, "http.request", metadata) { child =>
          val scopedRequest = request.withAttribute(requestScopeKey, RequestScope(requestId, child))
          routedApp(scopedRequest)
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

  private val requestIdApp: HttpApp[IO] = RequestId.httpApp(tracedApp)

  private val rawApp: HttpApp[IO] = Kleisli { request =>
    val validRequestId = request.headers.get(CIString("X-Request-ID")).exists { header =>
      scala.util.Try(java.util.UUID.fromString(header.head.value)).isSuccess
    }
    requestIdApp(if (validRequestId) request else request.removeHeader(CIString("X-Request-ID")))
  }

  val app: HttpApp[IO] = EntityLimiter.httpApp(rawApp, MaxRequestBytes)
}

object HiringApiRoutes {
  private val GraphQLResponseMediaType = MediaType.unsafeParse("application/graphql-response+json")
  private val SupportedResponseMediaTypes = List(
    GraphQLResponseMediaType,
    MediaType.application.json
  ).zipWithIndex
  private val SupportedResponseMediaTypesMessage =
    SupportedResponseMediaTypes.map { case (mediaType, _) => s"${mediaType.mainType}/${mediaType.subType}" }.mkString(" or ")

  final case class Dependencies(
      hiring: HiringGraphQLServices,
      authenticate: Request[IO] => IO[Either[AuthFailure, Option[ActorContext]]],
      ensureHiringReady: IO[ProbeResult],
      contextFactory: RequestContextFactory,
      rateLimiter: FixedWindowRateLimiter,
      clientAddressResolver: ClientAddressResolver,
      requestTimeout: FiniteDuration
  )

}
