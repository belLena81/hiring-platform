package com.example.graphQL.cats.api.http

import cats.effect.IO
import com.example.graphQL.cats.api.graphql.{HiringGraphQLSchema, TestGraphQLSupport}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.circe.*
import scala.concurrent.duration.*

/** ARR-15: introspection and the SDL download exist only while schema exposure is on. */
final class SchemaExposureSpec extends CatsEffectSuite {
  private val probe = new DatabaseProbe[IO] {
    def check(requestId: Option[String]): IO[ProbeResult] = IO.pure(ProbeResult.Ready)
  }

  private def build(exposure: Boolean): IO[HttpApp[IO]] =
    TestGraphQLSupport.dependencies(schemaExposure = exposure).allocated.flatMap { case (dependencies, _) =>
      new HiringApiRoutes(new HealthService(probe, Diagnostics.noop), Diagnostics.noop, dependencies)
        .httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
    }

  private def post(query: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))

  private def code(body: Json): Option[String] =
    body.hcursor.downField("errors").downArray.downField("extensions").get[String]("code").toOption

  private val snapshot = scala.io.Source.fromResource("graphql/hiring.graphql").mkString

  private val introspectionFixture =
    scala.io.Source.fromResource("graphql/introspection.graphql").mkString

  test("ARR-15.1 with exposure off the introspection fixture is refused with a sanitized error") {
    for {
      http <- build(exposure = false)
      response <- http(post(introspectionFixture))
      body <- response.as[Json]
    } yield {
      assertEquals(code(body), Some("FORBIDDEN"))
      assertEquals(body.hcursor.downField("data").focus, Some(Json.Null))
      assert(!body.noSpaces.contains("Query"))
    }
  }

  test("ARR-15.1 with exposure off __type is refused, including through a fragment") {
    for {
      http <- build(exposure = false)
      direct <- http(post("""{ __type(name: "User") { name } }""")).flatMap(_.as[Json])
      fragment <- http(post("query { ...F } fragment F on Query { ... { __schema { types { name } } } }"))
        .flatMap(_.as[Json])
    } yield {
      assertEquals(code(direct), Some("FORBIDDEN"))
      assertEquals(code(fragment), Some("FORBIDDEN"))
    }
  }

  test("ARR-15.1 with exposure off __typename is still allowed") {
    for {
      http <- build(exposure = false)
      response <- http(post("{ __typename }"))
      body <- response.as[Json]
    } yield assertEquals(body.hcursor.downField("data").get[String]("__typename"), Right("Query"))
  }

  test("ARR-15.1 with exposure off GET /schema.graphql is 404") {
    for {
      http <- build(exposure = false)
      response <- http(Request[IO](Method.GET, Uri.unsafeFromString("/schema.graphql")))
      text <- response.as[String]
    } yield {
      assertEquals(response.status, Status.NotFound)
      assert(!text.contains("type Query"))
    }
  }

  test("ARR-15.2 with exposure on the introspection fixture runs and the SDL download matches the snapshot") {
    for {
      http <- build(exposure = true)
      introspection <- http(post(introspectionFixture)).flatMap(_.as[Json])
      sdl <- http(Request[IO](Method.GET, Uri.unsafeFromString("/schema.graphql")))
      text <- sdl.as[String]
    } yield {
      assertEquals(code(introspection), None)
      assert(introspection.hcursor.downField("data").downField("__schema").succeeded)
      assertEquals(sdl.status, Status.Ok)
      assertEquals(text, snapshot)
      assertEquals(text, HiringGraphQLSchema.sdl)
    }
  }

  private def variablePost(query: String, variables: Json, operationName: Option[String] = None): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql")).withEntity(
      Json
        .obj("query" -> Json.fromString(query), "variables" -> variables)
        .deepMerge(operationName.fold(Json.obj())(name => Json.obj("operationName" -> Json.fromString(name))))
    )

  test("ARR-15.1 with exposure off introspection is refused under an alias") {
    for {
      http <- build(exposure = false)
      body <- http(post("{ innocent: __schema { queryType { name } } other: __type(name: \"User\") { name } }"))
        .flatMap(_.as[Json])
    } yield {
      assertEquals(code(body), Some("FORBIDDEN"))
      assert(!body.noSpaces.contains("queryType") && !body.noSpaces.contains("\"User\""), body.noSpaces)
    }
  }

  test("ARR-15.1 with exposure off introspection is refused behind a variable-driven @include") {
    for {
      http <- build(exposure = false)
      body <- http(
        variablePost(
          "query Q($on: Boolean!) { __schema @include(if: $on) { queryType { name } } readiness { status } }",
          Json.obj("on" -> Json.False)
        )
      ).flatMap(_.as[Json])
    } yield assertEquals(code(body), Some("FORBIDDEN"), clue(body))
  }

  test(
    "ARR-15.1 with exposure off a document carrying introspection is refused even when operationName picks another"
  ) {
    for {
      http <- build(exposure = false)
      response <- http(
        variablePost(
          "query Harmless { readiness { status } } query Probe { __schema { queryType { name } } }",
          Json.obj(),
          Some("Harmless")
        )
      )
      body <- response.as[Json]
    } yield {
      assertEquals(code(body), Some("FORBIDDEN"), clue(body))
      assert(!body.noSpaces.contains("queryType"), body.noSpaces)
    }
  }

  test("ARR-15.1 with exposure off an unknown field or type gets the generic 400 body without suggestions") {
    for {
      http <- build(exposure = false)
      field <- http(post("{ jobz { edges { cursor } } }"))
      fieldText <- field.as[String]
      unknownType <- http(post("{ ... on Jobb { id } }"))
      typeText <- unknownType.as[String]
    } yield {
      assertEquals(field.status, Status.BadRequest)
      assertEquals(unknownType.status, Status.BadRequest)
      List(fieldText, typeText).foreach { text =>
        assert(!text.contains("Did you mean"), text)
        assert(!text.contains("jobs") && !text.contains("Job"), text)
      }
      assertEquals(fieldText, typeText)
    }
  }

  test("ARR-15.1 with exposure on aliased introspection is bounded by its complexity weight") {
    val aliased = (1 to 6).map(index => s"i$index: __schema { queryType { name } }").mkString("{ ", " ", " }")
    val few = (1 to 2).map(index => s"i$index: __schema { queryType { name } }").mkString("{ ", " ", " }")
    for {
      http <- build(exposure = true)
      refused <- http(post(aliased))
      admitted <- http(post(few)).flatMap(_.as[Json])
    } yield {
      assertEquals(refused.status, Status.BadRequest)
      assert(admitted.hcursor.downField("data").downField("i2").succeeded, admitted.noSpaces)
    }
  }
}
