package com.example.graphQL.cats.api.graphql

import io.circe.Json
import munit.CatsEffectSuite

final class GraphQLRequestSpec extends CatsEffectSuite {
  private val query = Json.fromString("{ health { status } }")

  test("defaults absent and null optional envelope fields identically") {
    val absent = Json.obj("query" -> query).as[GraphQLRequest]
    val explicitNull = Json.obj("query" -> query,
      "variables" -> Json.Null, "operationName" -> Json.Null).as[GraphQLRequest]

    assertEquals(absent.map(request => (request.variables, request.operationName)),
      Right((Json.obj(), None)))
    assertEquals(explicitNull.map(request => (request.variables, request.operationName)),
      Right((Json.obj(), None)))
  }

  test("decodes variables and operation name through the typed envelope") {
    val request = Json.obj(
      "query" -> Json.fromString("query Health { health { status } }"),
      "variables" -> Json.obj("include" -> Json.True),
      "operationName" -> Json.fromString("Health")
    ).as[GraphQLRequest]

    assertEquals(request.map(parsed => (parsed.variables, parsed.operationName)),
      Right((Json.obj("include" -> Json.True), Some("Health"))))
  }

  test("uses Circe failures for missing and wrongly typed envelope fields") {
    val malformed = List(
      Json.obj(),
      Json.obj("query" -> Json.fromString("{ health { status } }"), "variables" -> Json.arr()),
      Json.obj("query" -> Json.fromString("{ health { status } }"), "operationName" -> Json.fromInt(1))
    )

    malformed.foreach { json =>
      val failure = json.as[GraphQLRequest].left.toOption
      assert(failure.nonEmpty)
      assert(!failure.exists(_.message.contains("GraphQL variables must be an object")))
      assert(!failure.exists(_.message.contains("GraphQL operationName must be a string")))
    }
  }

  test("maps GraphQL parser failures to the sanitized message") {
    val failure = Json.obj("query" -> Json.fromString("{ health")).as[GraphQLRequest].left.toOption

    assertEquals(failure.map(_.message), Some("Invalid GraphQL query"))
  }
}
