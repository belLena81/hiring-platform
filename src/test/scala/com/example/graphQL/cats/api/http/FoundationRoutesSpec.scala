package com.example.graphQL.cats.api.http

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.application.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import org.typelevel.ci.CIString
import scala.concurrent.duration.*

final class FoundationRoutesSpec extends CatsEffectSuite {
  private def app(effect: IO[ProbeResult]): IO[HttpApp[IO]] =
    Admission.create.map { admission =>
      val probe = new DatabaseProbe { def check: IO[ProbeResult] = effect }
      new FoundationRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, admission).app
    }

  private def request(query: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))

  private val health = request("{ health { status } }")

  test("API schema download matches the served schema without database access") {
    for {
      http <- app(IO.raiseError(new IllegalStateException("Schema must not query MongoDB")))
      response <- http(Request[IO](Method.GET, Uri.unsafeFromString("/schema.graphql")))
      schema <- response.as[String]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(schema, com.example.graphQL.cats.api.graphql.FoundationSchema.sdl)
      assert(!schema.contains("users"))
    }
  }

  test("health contract, correlation, and no database access") {
    for {
      count <- Ref.of[IO, Int](0)
      http <- app(count.update(_ + 1).as(ProbeResult.Ready))
      response <- http(health)
      body <- response.as[Json]
      calls <- count.get
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("data").downField("health").get[String]("status"), Right("UP"))
      assertEquals(calls, 0)
      assert(response.headers.get(CIString("X-Request-ID")).isDefined)
      assert(response.headers.get(CIString("Access-Control-Allow-Origin")).isEmpty)
    }
  }

  test("readiness aliases share a ping, recover, and retain HTTP200 on GraphQL failure") {
    for {
      count <- Ref.of[IO, Int](0)
      result <- Ref.of[IO, ProbeResult](ProbeResult.Unavailable)
      http <- app(count.update(_ + 1) *> result.get)
      response <- http(request("{ first: readiness { status } second: readiness { status } }"))
      body <- response.as[Json]
      firstCount <- count.get
      _ <- result.set(ProbeResult.Ready)
      recovered <- http(Request[IO](Method.GET, Uri.unsafeFromString("/ready")))
      ready <- recovered.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body.hcursor.downField("data").downField("first").get[String]("status"), Right("NOT_READY"))
      assertEquals(firstCount, 1)
      assertEquals(recovered.status, Status.Ok)
      assertEquals(ready.hcursor.get[String]("status"), Right("READY"))
    }
  }

  test("JSON envelope and GraphQL failures are sanitized, before probe execution") {
    val malformed = List(
      "not-json", "[]", "{\"query\":1}", "{\"query\":\"{ health { status } }\",\"variables\":[]}",
      "{\"query\":\"{ health { status } }\",\"operationName\":2}"
    ).map(body => health.withEntity(body).putHeaders(Header.Raw(CIString("Content-Type"), "application/json")))
    val queries = List("{ privateSecret }", "{ users { email } }", "query A { health { status } } query B { health { status } }",
      "query Test($include: Boolean!) { readiness @include(if: $include) { status } }", "{ health").map(request)
    for {
      count <- Ref.of[IO, Int](0)
      http <- app(count.update(_ + 1).as(ProbeResult.Ready))
      responses <- (malformed ++ queries).traverse(http.run)
      bodies <- responses.traverse(_.as[String])
      calls <- count.get
    } yield {
      assert(responses.forall(_.status == Status.BadRequest))
      assert(bodies.forall(!_.contains("privateSecret")))
      assertEquals(calls, 0)
    }
  }

  test("named operation, variables, explicit nulls, methods and media negotiation") {
    val selected = health.withEntity(Json.obj(
      "query" -> Json.fromString("query First { readiness { status } } query Second($include: Boolean!) { health @include(if: $include) { status } }"),
      "operationName" -> Json.fromString("Second"), "variables" -> Json.obj("include" -> Json.True)))
    for {
      http <- app(IO.raiseError(new IllegalStateException("probe must not execute")))
      named <- http(selected)
      nulls <- http(health.withEntity(Json.obj("query" -> Json.fromString("{health{status}}"),
        "operationName" -> Json.Null, "variables" -> Json.Null)))
      missingOperation <- http(selected.withEntity(Json.obj("query" -> Json.fromString("query Named { health { status } }"),
        "operationName" -> Json.fromString("Missing"))))
      method <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphql")))
      media <- http(health.withEntity("query"))
      accept <- http(health.putHeaders(Header.Raw(CIString("Accept"), "text/html")))
      zeroQuality <- http(health.putHeaders(Header.Raw(CIString("Accept"), "application/json;q=0")))
      charset <- http(health.putHeaders(Header.Raw(CIString("Content-Type"), "application/json; charset=utf-8")))
    } yield {
      assertEquals(named.status, Status.Ok)
      assertEquals(nulls.status, Status.Ok)
      assertEquals(missingOperation.status, Status.BadRequest)
      assertEquals(method.status, Status.MethodNotAllowed)
      assertEquals(media.status, Status.UnsupportedMediaType)
      assertEquals(accept.status, Status.NotAcceptable)
      assertEquals(zeroQuality.status, Status.NotAcceptable)
      assertEquals(charset.status, Status.Ok)
    }
  }

  test("streamed size limit, slow body deadline and cancellation release capacity") {
    for {
      http <- app(IO.pure(ProbeResult.Ready))
      large <- http(health.withBodyStream(fs2.Stream.repeatEval(IO.pure(32.toByte)).take(65537)))
      finalized <- Deferred[IO, Unit]
      slow = health.withBodyStream(fs2.Stream.eval(IO.never[Byte]).onFinalize(finalized.complete(()).void))
      response <- http(slow)
      _ <- finalized.get.timeout(1.second)
      successful <- http(health)
    } yield {
      assertEquals(large.status, Status.PayloadTooLarge)
      assertEquals(response.status, Status.GatewayTimeout)
      assertEquals(successful.status, Status.Ok)
    }
  }

  test("saturation rejects work, preserves liveness, and cancellation returns every permit") {
    for {
      started <- Ref.of[IO, Int](0)
      allStarted <- Deferred[IO, Unit]
      canceled <- Ref.of[IO, Int](0)
      http <- app(IO.pure(ProbeResult.Ready))
      slow = health.withBodyStream(fs2.Stream.eval(
        started.updateAndGet(_ + 1).flatMap(count => if (count == 16) allStarted.complete(()).void else IO.unit) *>
          IO.never[Byte]).onFinalize(canceled.update(_ + 1)))
      fibers <- List.fill(16)(http(slow)).traverse(_.start)
      _ <- allStarted.get.timeout(2.seconds)
      rejected <- http(health)
      liveness <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
      _ <- fibers.traverse_(_.cancel)
      canceledCount <- canceled.get
      successful <- List.fill(16)(http(health)).parSequence
    } yield {
      assertEquals(rejected.status, Status.ServiceUnavailable)
      assertEquals(liveness.status, Status.Ok)
      assertEquals(canceledCount, 16)
      assert(successful.forall(_.status == Status.Ok))
    }
  }

  test("UI and assets are absent, and closing admission preserves liveness") {
    for {
      admission <- Admission.create
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      http = new FoundationRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, admission).app
      ui <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql")))
      asset <- http(Request[IO](Method.GET, Uri.unsafeFromString("/graphiql/assets/graphiql.js")))
      _ <- admission.close
      work <- http(health)
      live <- http(Request[IO](Method.GET, Uri.unsafeFromString("/health")))
    } yield {
      assertEquals(ui.status, Status.NotFound)
      assertEquals(asset.status, Status.NotFound)
      assertEquals(work.status, Status.ServiceUnavailable)
      assertEquals(live.status, Status.Ok)
    }
  }
}
