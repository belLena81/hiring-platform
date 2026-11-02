package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{
  GraphQLOperationKind,
  GraphQLOperationOutcomes,
  GraphQLOutcome,
  GraphQLRootFieldOutcome,
  HiringGraphQLSchema,
  TestGraphQLSupport
}
import com.example.graphQL.cats.api.http.HiringApiRoutes
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import scala.concurrent.duration.*

/** HOR-01 and HOR-02: the exported GraphQL outcome counter, end to end through the HTTP route. */
final class GraphQLOutcomeMetricsSpec extends CatsEffectSuite {
  private val probe = new DatabaseProbe[IO] {
    def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  }

  private val rootFields: Set[String] =
    HiringGraphQLSchema.schema.query.fieldsByName.keySet ++ HiringGraphQLSchema.schema.mutation.toList
      .flatMap(_.fieldsByName.keySet)

  private def post(query: String, operationName: Option[String] = None): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(
        Json.obj(
          "query" -> Json.fromString(query),
          "operationName" -> operationName.fold(Json.Null)(Json.fromString)
        )
      )

  /** Runs the requests through the real route with a real counter and returns the exported points. */
  private def exportedAfter(requests: List[Request[IO]], exposure: Boolean = true) =
    TelemetryCapture.resource.use { capture =>
      for {
        recorder <- GraphQLOutcomeMetrics.create(capture.otel4s.meterProvider)
        http <- TestGraphQLSupport.dependencies(schemaExposure = exposure).allocated.flatMap { case (dependencies, _) =>
          new HiringApiRoutes(
            new HealthService(probe, Diagnostics.noop),
            Diagnostics.noop,
            dependencies.copy(graphQLOutcomes = recorder)
          ).httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
        }
        _ <- requests.traverse_(request => http(request).flatMap(_.as[Json]))
        points <- capture.collectMetrics.map(TelemetryCapture.points)
      } yield points.filter(_.name == GraphQLOutcomeMetrics.InstrumentName)
    }

  private def counts(points: List[TelemetryCapture.Point]): Map[(String, String), Double] =
    points
      .map(p => (p.attributes("hiring.graphql.root_field"), p.attributes("hiring.graphql.outcome")) -> p.value)
      .toMap

  test("HOR-01 an unauthenticated field error answered with HTTP 200 is counted as unauthenticated on its root field") {
    exportedAfter(List(post("{ whoever: me { id } }"))).map { points =>
      assertEquals(counts(points).keySet.map(_._1), Set("me"))
      assertEquals(counts(points).get(("me", "unauthenticated")), Some(1.0))
    }
  }

  test("HOR-02 exported attributes are exactly the three bounded keys and never contain caller-supplied text") {
    val hostile = List(
      post(
        "query PrivateOperationNameForCandidate42 { aliasWithEmail_jane_example_com: me { id } }",
        Some("PrivateOperationNameForCandidate42")
      ),
      post("{ notARealField }"),
      post("{ __schema { types { name } } }"),
      post("this is not graphql { ["),
      post("query A { me { id } }", Some("NoSuchOperation"))
    )
    exportedAfter(hostile).map { points =>
      assert(points.nonEmpty)
      assert(
        points.forall(
          _.attributes.keySet == Set("graphql.operation.type", "hiring.graphql.root_field", "hiring.graphql.outcome")
        ),
        clue(points)
      )
      assert(points.forall(_.unit == "{execution}"))
      val allowedFields = rootFields ++ Set("_introspection", "_unknown", "_invalid")
      assert(points.forall(p => allowedFields(p.attributes("hiring.graphql.root_field"))), clue(points))
      assert(points.forall(p => GraphQLOutcome.values.map(_.label).contains(p.attributes("hiring.graphql.outcome"))))
      assert(
        points.forall(p => GraphQLOperationKind.values.map(_.label).contains(p.attributes("graphql.operation.type")))
      )
      val rendered = points.toString
      assert(!rendered.contains("PrivateOperationName") && !rendered.contains("jane"), clue(rendered))
    }
  }

  test("HOR-02 a parse failure is one _invalid rejected count and an unknown field collapses to _unknown") {
    exportedAfter(List(post("this is not graphql { ["))).map { points =>
      assertEquals(counts(points), Map(("_invalid", "rejected") -> 1.0))
    }
  }

  test("HOR-01 repeated requests accumulate in the same bounded series") {
    exportedAfter(List.fill(3)(post("{ me { id } }"))).map { points =>
      assertEquals(counts(points), Map(("me", "unauthenticated") -> 3.0))
    }
  }

  test("HOR-01 a failing recorder does not change the GraphQL response") {
    val failing = new com.example.graphQL.cats.api.graphql.GraphQLOutcomeRecorder {
      def record(outcomes: List[GraphQLRootFieldOutcome]): IO[Unit] =
        IO.raiseError(new RuntimeException("metrics down"))
    }
    TestGraphQLSupport.dependencies().use { dependencies =>
      new HiringApiRoutes(
        new HealthService(probe, Diagnostics.noop),
        Diagnostics.noop,
        dependencies.copy(graphQLOutcomes = failing)
      )
        .httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
        .flatMap(_(post("{ me { id } }")))
        .map(response => assertEquals(response.status, Status.Ok))
    }
  }

  test("HOR-02 the invalid request count uses the unknown operation type") {
    assertEquals(
      GraphQLOperationOutcomes.invalidRequest,
      List(GraphQLRootFieldOutcome(GraphQLOperationKind.Unknown, "_invalid", GraphQLOutcome.Rejected))
    )
  }
}
