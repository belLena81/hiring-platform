package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite

final class HiringGraphQLContractSpec extends CatsEffectSuite {
  private def fixture(name: String): IO[String] = IO.blocking {
    val stream = Option(getClass.getResourceAsStream(s"/graphql/$name"))
      .getOrElse(throw new IllegalArgumentException("Missing contract fixture"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }

  private val probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
  private val service = new HealthService(probe, Diagnostics.noop)

  private def parseRequest(query: String): IO[GraphQLRequest] =
    IO.fromEither(
      Json
        .obj("query" -> Json.fromString(query))
        .as[GraphQLRequest]
        .left
        .map(error => new IllegalArgumentException("Invalid test operation", error))
    )

  private def executeRequest(
      request: GraphQLRequest,
      documentCache: GraphQLDocumentCache,
      context: Resource[IO, RequestContext]
  ): IO[Either[HiringGraphQLSchema.Failure, Json]] =
    documentCache.document(request.query).flatMap {
      case Left(failure)   => IO.pure(Left(failure))
      case Right(document) => context.use(HiringGraphQLSchema.executeInContext(request, document, _))
    }

  test("served SDL matches the deterministic contract fixture") {
    fixture("hiring.graphql").map(expected => assertEquals(HiringGraphQLSchema.sdl, expected))
  }

  test("a closed request context causes a sanitized GraphQL field execution error") {
    for {
      parsed <- parseRequest("{ readiness { status } }")
      closed <- TestGraphQLSupport.context(IO.pure(ProbeResult.Ready)).use(IO.pure)
      result <- TestGraphQLSupport.parseAndExecute(parsed, closed)
    } yield {
      val body = result.fold(failure => fail(failure.toString), identity)
      val errors = body.hcursor.downField("errors").as[Vector[Json]]
      assertEquals(errors.map(_.size), Right(1))
      val error = errors.toOption.get.head
      assertEquals(error.hcursor.get[String]("message"), Right("Execution failed"))
      assertEquals(error.hcursor.get[Vector[String]]("path"), Right(Vector("readiness")))
      assert(error.hcursor.downField("locations").succeeded)
      assert(!body.noSpaces.contains("Request context is closed"))
      assert(!body.noSpaces.contains("IllegalStateException"))
    }
  }

  test("malformed typed identifiers fail GraphQL coercion before resolver execution") {
    for {
      parsed <- parseRequest("{ job(id: \"not-a-uuid\") { id } }")
      result <- TestGraphQLSupport
        .dependencies()
        .use(dependencies =>
          executeRequest(
            parsed,
            dependencies.documentCache,
            dependencies.contextFactory.resource(
              RequestContextParameters(
                service.readiness(Some("00000000-0000-0000-0000-000000000001")),
                None,
                dependencies.hiring,
                dependencies.ensureHiringReady,
                requestId = Some("00000000-0000-0000-0000-000000000001")
              )
            )
          )
        )
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("malformed UUID interaction inputs fail standard GraphQL validation") {
    val operations = List(
      "mutation { recordJobView(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"invalid\", jobId: \"00000000-0000-0000-0000-000000000001\" }) { recorded } }",
      "mutation { recordJobView(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"00000000-0000-0000-0000-000000000001\", jobId: \"00000000-0000-0000-0000-000000000001\", searchId: \"invalid\" }) { recorded } }",
      "mutation { recordSearchResultClick(input: { idempotencyKey: \"00000000-0000-0000-0000-000000000001\", eventId: \"invalid\", searchId: \"invalid\", resultId: \"invalid\" }) { recorded } }"
    )

    operations
      .traverse(parseRequest)
      .flatMap { requests =>
        TestGraphQLSupport.dependencies().use { dependencies =>
          requests.traverse { request =>
            executeRequest(
              request,
              dependencies.documentCache,
              dependencies.contextFactory.resource(
                RequestContextParameters(
                  service.readiness(Some("00000000-0000-0000-0000-000000000001")),
                  None,
                  dependencies.hiring,
                  dependencies.ensureHiringReady,
                  requestId = Some("00000000-0000-0000-0000-000000000001")
                )
              )
            )
          }
        }
      }
      .map(results => assert(results.forall(_ == Left(HiringGraphQLSchema.Failure.InvalidQuery))))
  }

  List("health.graphql", "readiness.graphql", "introspection.graphql").foreach { name =>
    test(s"execute consumer fixture $name") {
      for {
        query <- fixture(name)
        parsed <- parseRequest(query)
        result <- TestGraphQLSupport
          .dependencies()
          .use(dependencies =>
            executeRequest(
              parsed,
              dependencies.documentCache,
              dependencies.contextFactory.resource(
                RequestContextParameters(
                  service.readiness(Some("00000000-0000-0000-0000-000000000001")),
                  None,
                  dependencies.hiring,
                  dependencies.ensureHiringReady,
                  requestId = Some("00000000-0000-0000-0000-000000000001")
                )
              )
            )
          )
      } yield {
        val json = result.fold(failure => fail(failure.toString), identity)
        assert(json.hcursor.downField("data").succeeded)
        assert(!json.hcursor.downField("errors").succeeded)
      }
    }
  }

}
