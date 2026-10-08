package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref, Resource}
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

  test("interview scheduling operations match the active public schema") {
    fixture("interview-scheduling.graphql").map { operation =>
      val document = sangria.parser.QueryParser.parse(operation).get
      val violations =
        sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
      assertEquals(violations.toList, Nil)
    }
  }

  test("embedding coverage operation matches the active public schema") {
    fixture("embedding-coverage.graphql").map { operation =>
      val document = sangria.parser.QueryParser.parse(operation).get
      val violations =
        sangria.validation.QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None)
      assertEquals(violations.toList, Nil)
    }
  }

  test("ECR-07 the embedding coverage report exposes no identifiers, names, text, vectors or payloads") {
    val reportTypes = HiringGraphQLSchema.schema.allTypes.values.collect {
      case objectType: sangria.schema.ObjectType[?, ?] if objectType.name.startsWith("EmbeddingCoverage") => objectType
      case objectType: sangria.schema.ObjectType[?, ?] if objectType.name == "EmbeddingObservedModel"     => objectType
    }.toList
    val fieldNames = reportTypes.flatMap(_.fieldsByName.keys)
    assertEquals(reportTypes.size, 6)
    val forbidden = Set("id", "ids", "name", "text", "vector", "embedding", "payload", "entityId", "title")
    assertEquals(fieldNames.filter(forbidden.contains), Nil)
    assertEquals(
      HiringGraphQLSchema.schema.allTypes.values
        .collect { case enumType: sangria.schema.EnumType[?] => enumType }
        .filter(_.name.startsWith("Embedding"))
        .flatMap(_.values.map(_.name))
        .forall(label => label == label.toUpperCase),
      true
    )
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

  test("unexpected resolver failures are sanitized and reported after Sangria handles them") {
    for {
      reportCount <- Ref.of[IO, Int](0)
      diagnostics = new Diagnostics {
        def event(
            event: com.example.graphQL.cats.service.LogEvent,
            requestId: Option[String],
            fields: => Map[com.example.graphQL.cats.service.LogField, String]
        ) = reportCount.update(_ + 1)
      }
      request <- parseRequest("{ readiness { status } }")
      result <- TestGraphQLSupport
        .context(
          IO.raiseError[ProbeResult](new IllegalStateException("private resolver detail")),
          diagnostics = diagnostics
        )
        .use(context => TestGraphQLSupport.parseAndExecute(request, context))
      reports <- reportCount.get
    } yield {
      val body = result.fold(failure => fail(failure.toString), identity)
      assertEquals(body.hcursor.downField("errors").downArray.get[String]("message"), Right("Execution failed"))
      assert(!body.noSpaces.contains("private resolver detail"))
      assertEquals(reports, 1)
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
                diagnostics = Diagnostics.noop,
                requestId = Some("00000000-0000-0000-0000-000000000001")
              )
            )
          )
        )
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  test("query complexity rejection returns the typed invalid-query failure") {
    val fields = (1 to 510).map(index => s"health$index: health { status }").mkString(" ")
    for {
      request <- parseRequest(s"{ $fields }")
      result <- TestGraphQLSupport.dependencies().use { dependencies =>
        executeRequest(
          request,
          dependencies.documentCache,
          dependencies.contextFactory.resource(
            RequestContextParameters(
              service.readiness(Some("00000000-0000-0000-0000-000000000001")),
              None,
              dependencies.hiring,
              dependencies.ensureHiringReady,
              diagnostics = Diagnostics.noop
            )
          )
        )
      }
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
                  diagnostics = Diagnostics.noop,
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
                  diagnostics = Diagnostics.noop,
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
