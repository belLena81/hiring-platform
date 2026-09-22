package com.example.graphQL.cats.api.http

import cats.data.Kleisli
import cats.effect.IO
import com.example.graphQL.cats.service.Diagnostics
import munit.CatsEffectSuite
import org.http4s.*
import org.typelevel.ci.CIString
import org.typelevel.otel4s.trace.Tracer

final class HttpMiddlewareSpec extends CatsEffectSuite {
  private def values(response: Response[IO], name: String): Option[List[String]] =
    response.headers.get(CIString(name)).map(_.toList.map(_.value))

  test("correlation replaces downstream security headers without duplicates") {
    val next: HttpApp[IO] = Kleisli { (_: Request[IO]) => IO.pure(Response[IO](Status.Ok).putHeaders(
      Header.Raw(CIString("Cache-Control"), "public"),
      Header.Raw(CIString("X-Content-Type-Options"), "old-value"),
      Header.Raw(CIString("Content-Security-Policy"), "unsafe-inline"),
      Header.Raw(CIString("X-Request-ID"), "downstream-id")
    )) }

    val request = Request[IO](Method.GET, Uri.unsafeFromString("/health"))

    HttpMiddleware.correlation(Diagnostics.noop, Tracer.noop[IO])(next)(request).map { response =>
      assertEquals(values(response, "Cache-Control"), Some(List("no-store")))
      assertEquals(values(response, "X-Content-Type-Options"), Some(List("nosniff")))
      assertEquals(values(response, "Content-Security-Policy"),
        Some(List("default-src 'none'; frame-ancestors 'none'; base-uri 'none'")))
      assert(values(response, "X-Request-ID").exists(_.size == 1))
      assert(values(response, "X-Request-ID").exists(_.head.matches("[0-9a-fA-F-]{36}")))
    }
  }
}
