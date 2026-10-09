package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, FailureReason, LogEvent, LogField}
import com.example.graphQL.cats.service.Diagnostics.*
import io.circe.Json
import org.http4s.*
import org.http4s.circe.CirceEntityEncoder.circeEntityEncoder
import org.http4s.headers.`Cache-Control`
import org.http4s.server.middleware.{EntityLimiter, MaxActiveRequests, RequestId, Timeout}
import org.typelevel.ci.{CIString, CIStringSyntax}
import org.typelevel.otel4s.trace.Tracer

object HttpMiddleware {
  private val MaxRequestBytes = 64 * 1024
  private val SecurityHeaders = Headers(
    `Cache-Control`(CacheDirective.`no-store`),
    Header.Raw(ci"X-Content-Type-Options", "nosniff"),
    Header.Raw(ci"Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
  )

  def correlation(diagnostics: Diagnostics, tracer: Tracer[IO])(next: HttpApp[IO]): HttpApp[IO] =
    Kleisli { request =>
      tracer.currentSpanContext
        .flatMap(_.fold(IO.randomUUID.map(_.toString))(context => IO.pure(context.traceIdHex)))
        .flatMap { correlationId =>
          next(request.withAttribute(RequestId.requestIdAttrKey, correlationId))
            .flatTap { response =>
              val reason = response.status match {
                case Status.ServiceUnavailable => Some(FailureReason.Overloaded)
                case Status.GatewayTimeout     => Some(FailureReason.DeadlineExceeded)
                case _                         => None
              }
              reason.fold(IO.unit)(failure =>
                diagnostics.emit(
                  LogEvent.RequestRejected,
                  Some(correlationId),
                  fields = Map(LogField.Reason -> failure.reason, LogField.Status -> response.status.code.toString)
                )
              )
            }
            .map { response =>
              response
                .removeHeader(CIString("X-Request-ID"))
                .putHeaders(SecurityHeaders, Header.Raw(ci"X-Request-ID", correlationId))
            }
        }
    }

  private[http] def requestId(request: Request[IO]): IO[String] =
    IO.fromOption(request.attributes.lookup(RequestId.requestIdAttrKey))(
      new IllegalStateException("Missing request correlation ID")
    )

  private[http] def errorResponse(status: Status, message: String): Response[IO] =
    Response[IO](status).withEntity(
      Json.obj("errors" -> Json.arr(Json.obj("message" -> Json.fromString(message))))
    )

  private[http] def statusResponse(status: Status, value: String): Response[IO] =
    Response[IO](status).withEntity(Json.obj("status" -> Json.fromString(value)))

  /** `routes` run behind the shared limits; requests matching `unmetered` skip only the active-request cap. */
  def apply(
      config: HiringApiRoutes.HttpConfig,
      routes: HttpApp[IO],
      unmetered: Request[IO] => Boolean,
      diagnostics: Diagnostics,
      tracer: Tracer[IO],
      onError: (Request[IO], Throwable) => IO[Response[IO]],
      onEntityTooLarge: Request[IO] => IO[Response[IO]]
  ): IO[HttpApp[IO]] = {
    val limited = EntityLimiter.httpApp[IO](routes, MaxRequestBytes)
    val timed = Timeout.httpApp[IO](
      config.requestTimeout,
      IO.pure(errorResponse(Status.GatewayTimeout, "Request deadline exceeded"))
    )(limited)

    MaxActiveRequests
      .forHttpApp[IO](config.admissionPermits, errorResponse(Status.ServiceUnavailable, "Server busy"))
      .map { limitActive =>
        val metered = limitActive(timed)
        val handled: HttpApp[IO] = Kleisli { request =>
          (if (unmetered(request)) timed else metered).run(request).handleErrorWith {
            case _: EntityLimiter.EntityTooLarge => onEntityTooLarge(request)
            case failure                         => onError(request, failure)
          }
        }
        correlation(diagnostics, tracer)(handled)
      }
  }
}
