package com.example.graphQL.cats.api.http

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.TestGraphQLSupport
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult, ReadinessComponent}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import scala.concurrent.duration.*

/** ARR-07.1: `/ready` keeps its status rule; degraded components are named in the body and never change the status. */
final class ReadinessDegradedSpec extends CatsEffectSuite {
  private def ready(
      ping: ProbeResult,
      degraded: IO[List[ReadinessComponent]]
  ): IO[Response[IO]] = {
    val probe = new DatabaseProbe[IO] { def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ping) }
    TestGraphQLSupport.dependencies().allocated.flatMap { case (dependencies, _) =>
      new HiringApiRoutes(new HealthService(probe, Diagnostics.noop, Some(degraded)), Diagnostics.noop, dependencies)
        .httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
        .flatMap(_.run(Request[IO](Method.GET, Uri.unsafeFromString("/ready"))))
    }
  }

  test("ARR-07.1 a ready service with a stopped publisher answers 200 and names the publisher as degraded") {
    for {
      response <- ready(
        ProbeResult.Ready,
        IO.pure(List(ReadinessComponent.KafkaPublisher, ReadinessComponent.OutboxAge))
      )
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(
        body,
        Json.obj(
          "status" -> Json.fromString("READY"),
          "degraded" -> Json.arr(Json.fromString("kafka-publisher"), Json.fromString("outbox-age"))
        )
      )
    }
  }

  test("ARR-07.1 a healthy service answers 200 with an empty degraded list") {
    for {
      response <- ready(ProbeResult.Ready, IO.pure(Nil))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Json.obj("status" -> Json.fromString("READY"), "degraded" -> Json.arr()))
    }
  }

  test("ARR-07.1 an unavailable database answers 503 without a component list") {
    for {
      response <- ready(ProbeResult.Unavailable, IO.pure(List(ReadinessComponent.KafkaPublisher)))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.ServiceUnavailable)
      assertEquals(body, Json.obj("status" -> Json.fromString("NOT_READY")))
    }
  }

  test("ARR-07.1 a failing degraded collector cannot change a ready answer") {
    for {
      response <- ready(ProbeResult.Ready, IO.raiseError(new IllegalStateException("synthetic-secret")))
      body <- response.as[Json]
    } yield {
      assertEquals(response.status, Status.Ok)
      assertEquals(body, Json.obj("status" -> Json.fromString("READY"), "degraded" -> Json.arr()))
    }
  }

  test("ARR-07.1 a flood of concurrent probes all receive the real status from one collection") {
    for {
      collections <- cats.effect.Ref.of[IO, Int](0)
      probe = new DatabaseProbe[IO] {
        def check(requestId: Option[String]): IO[ProbeResult] =
          collections.update(_ + 1) *> IO.sleep(100.millis).as(ProbeResult.Ready)
      }
      built <- TestGraphQLSupport.dependencies().allocated
      cache <- com.example.graphQL.cats.service.ReadinessCache
        .create[IO, com.example.graphQL.cats.service.ReadinessReport](1.second)
      http <- new HiringApiRoutes(
        new HealthService(probe, Diagnostics.noop, None, Some(cache)),
        Diagnostics.noop,
        built._1
      ).httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
      responses <- http.run(Request[IO](Method.GET, Uri.unsafeFromString("/ready"))).parReplicateA(64)
      total <- collections.get
    } yield {
      assertEquals(responses.map(_.status).toSet, Set(Status.Ok))
      assertEquals(total, 1)
    }
  }
}
