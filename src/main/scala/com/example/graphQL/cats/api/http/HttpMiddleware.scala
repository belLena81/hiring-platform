package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, Rejection}
import org.http4s.*
import org.http4s.headers.`Cache-Control`
import org.http4s.server.middleware.{EntityLimiter, ErrorHandling, MaxActiveRequests, RequestId, Timeout}
import org.http4s.syntax.all.*
import org.typelevel.ci.{CIString, CIStringSyntax}
import org.typelevel.otel4s.trace.Tracer
import scala.concurrent.duration.*

object HttpMiddleware {
  private val MaxRequestBytes = 64 * 1024
  private val SecurityHeaders = Headers(
    `Cache-Control`(CacheDirective.`no-store`),
    Header.Raw(ci"X-Content-Type-Options", "nosniff"),
    Header.Raw(ci"Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"))

  def requestId(next: HttpApp[IO]): HttpApp[IO] = Kleisli { request =>
    RequestId.httpApp[IO](next).run(request.removeHeader(CIString("X-Request-ID")))
  }

  def correlation(diagnostics: Diagnostics, tracer: Tracer[IO])(next: HttpApp[IO]): HttpApp[IO] = Kleisli { request =>
    val requestId = request.attributes.lookup(RequestId.requestIdAttrKey).getOrElse("unknown")
    tracer.currentSpanContext.map(_.fold(requestId)(_.traceIdHex)).flatMap { correlationId =>
      next(request).flatTap { response =>
        val rejection = if (response.status == Status.GatewayTimeout)
          diagnostics.emit(LogEvent.RequestRejected, Some(correlationId), fields = Map(
            LogField.Reason -> Rejection.DeadlineExceeded.reason,
            LogField.Status -> response.status.code.toString))
        else IO.unit
        rejection
      }.map(_.putHeaders(SecurityHeaders, Header.Raw(ci"X-Request-ID", correlationId)))
    }
  }

  def apply(config: HiringApiRoutes.HttpConfig, routes: HttpApp[IO], diagnostics: Diagnostics, tracer: Tracer[IO],
      onError: (Request[IO], Throwable) => IO[Response[IO]],
      onEntityTooLarge: Request[IO] => IO[Response[IO]]): IO[HttpApp[IO]] = {
    val timeoutResponse = IO.pure(Response[IO](Status.GatewayTimeout))
    MaxActiveRequests.forHttpApp[IO](config.admissionPermits,
      Response[IO](Status.ServiceUnavailable)).map { limitActive =>
      val limited = EntityLimiter.httpApp[IO](_, MaxRequestBytes)(routes)
      val timed = Timeout.httpApp[IO](config.requestTimeout, timeoutResponse)(limited)
      val active = limitActive(timed)
      val handled = Kleisli { request =>
        ErrorHandling.Custom.recoverWith(active) {
          case _: EntityLimiter.EntityTooLarge => onEntityTooLarge(request)
          case failure => onError(request, failure)
        }.run(request)
      }
      requestId(correlation(diagnostics, tracer)(handled))
    }
  }
}
