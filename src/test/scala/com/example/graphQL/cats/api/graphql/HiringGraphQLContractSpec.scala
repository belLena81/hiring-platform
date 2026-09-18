package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.{GraphQLRequest, HiringGraphQLSchema, RequestContext}
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

  test("served SDL matches the deterministic contract fixture") {
    fixture("hiring.graphql").map(expected => assertEquals(HiringGraphQLSchema.sdl, expected))
  }

  test("a closed request context causes a sanitized GraphQL field execution error") {
    for {
      parsed <- IO.fromOption(GraphQLRequest.parseBody(Json.obj(
        "query" -> Json.fromString("{ readiness { status } }")
      ).noSpaces))(new IllegalArgumentException("Invalid test operation"))
      closed <- RequestContext.resource(IO.pure(ProbeResult.Ready)).use(IO.pure)
      result <- HiringGraphQLSchema.executeInContext(parsed, closed)
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
      parsed <- IO.fromOption(GraphQLRequest.parseBody(Json.obj(
        "query" -> Json.fromString("{ job(id: \"not-a-uuid\") { id } }")
      ).noSpaces))(new IllegalArgumentException("Invalid test operation"))
      result <- HiringGraphQLSchema.execute(parsed, service, "00000000-0000-0000-0000-000000000001")
    } yield assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery))
  }

  List("health.graphql", "readiness.graphql", "introspection.graphql").foreach { name =>
    test(s"execute consumer fixture $name") {
      for {
        query <- fixture(name)
        parsed <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
          new IllegalArgumentException("Fixture exceeds request contract"))
        result <- HiringGraphQLSchema.execute(parsed, service, "00000000-0000-0000-0000-000000000001")
      } yield {
        val json = result.fold(failure => fail(failure.toString), identity)
        assert(json.hcursor.downField("data").succeeded)
        assert(!json.hcursor.downField("errors").succeeded)
      }
    }
  }

}
