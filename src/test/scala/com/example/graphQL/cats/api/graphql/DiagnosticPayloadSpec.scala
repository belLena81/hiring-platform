package com.example.graphQL.cats.api.graphql

import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite
import sangria.parser.QueryParser
import java.nio.charset.StandardCharsets.UTF_8

final class DiagnosticPayloadSpec extends FunSuite {
  private def request(query: String, variables: Json = Json.obj(), operation: Option[String] = None): GraphQLRequest =
    GraphQLRequest(QueryParser.parse(query).toOption.getOrElse(fail("Invalid test query")), variables, operation)

  private def captured(value: GraphQLRequest): Json = {
    val serialized = DiagnosticPayload.capture(value).getOrElse(fail("Capture unexpectedly omitted"))
    assert(serialized.getBytes(UTF_8).length <= 2048)
    parse(serialized).getOrElse(fail("Capture is not valid JSON"))
  }

  private def query(value: Json): String =
    value.hcursor.get[String]("query").getOrElse(fail("Missing reconstructed query"))

  test("comments, source, defaults, directives and every literal kind are reconstructed without values") {
    val source = """# COMMENT_SECRET
      query Selected($value: String = "DEFAULT_SECRET") @tag(value: "DIRECTIVE_SECRET") {
        field(text: "TEXT_SECRET", block: """ + "\"\"\"BLOCK_SECRET\"\"\"" + """,
          enum: ENUM_SECRET, number: 987654, float: 987.654, flag: true, nil: null,
          items: ["LIST_SECRET", {ordinary: "OBJECT_SECRET"}]) @other(value: ENUM_DIRECTIVE_SECRET)
      } # TRAILING_SECRET
    """
    val output = captured(request(source))
    val reconstructed = query(output)
    Vector("COMMENT_SECRET", "DEFAULT_SECRET", "DIRECTIVE_SECRET", "TEXT_SECRET", "BLOCK_SECRET",
      "ENUM_SECRET", "987654", "987.654", "LIST_SECRET", "OBJECT_SECRET", "ENUM_DIRECTIVE_SECRET",
      "TRAILING_SECRET").foreach(secret => assert(!reconstructed.contains(secret), secret))
    assert(!reconstructed.contains("#"))
    assert(!reconstructed.contains("true"))
    assert(!reconstructed.contains("null"))
    assert(reconstructed.contains("[REDACTED]"))
    assert(QueryParser.parse(reconstructed).isSuccess)
  }

  test("only the selected operation and transitively reachable fragments are retained") {
    val output = captured(request("""
      query Selected { ...First ...First }
      query Unselected { irrelevant(value: "UNSELECTED_SECRET") }
      fragment First on Query { ...Second }
      fragment Second on Query { health { status } }
      fragment Unreachable on Query { unused(value: "UNREACHABLE_SECRET") }
    """, operation = Some("Selected")))
    val reconstructed = query(output)
    assert(reconstructed.contains("Selected"))
    assert(reconstructed.contains("fragment First"))
    assert(reconstructed.contains("fragment Second"))
    assert(!reconstructed.contains("Unselected"))
    assert(!reconstructed.contains("Unreachable"))
    assertEquals(reconstructed.sliding("fragment First".length).count(_ == "fragment First"), 1)
  }

  test("only selected declared top-level Boolean and Boolean! JSON booleans retain values") {
    val output = captured(request("""
      query Selected($include: Boolean, $required: Boolean!, $text: String, $list: [Boolean], $wrong: Boolean) { health { status } }
      query Other($other: Boolean) { health { status } }
    """, Json.obj("include" -> Json.True, "required" -> Json.False, "text" -> Json.True,
      "list" -> Json.arr(Json.True), "other" -> Json.True, "wrong" -> Json.fromString("true"),
      "unknown" -> Json.True), Some("Selected")))
    val variables = output.hcursor.downField("variables")
    assertEquals(variables.get[Boolean]("include"), Right(true))
    assertEquals(variables.get[Boolean]("required"), Right(false))
    Vector("text", "other", "wrong", "unknown").foreach { key =>
      assertEquals(variables.get[String](key), Right("[REDACTED]"))
    }
    assertEquals(variables.downField("list").as[Vector[String]], Right(Vector("[REDACTED]")))
  }

  test("credential terms override Boolean permission after ASCII key normalization") {
    val keys = Vector("PASSWORD", "pass_wd", "pwD", "accessTOKEN", "client_secret",
      "Authorization", "Cookie", "credential", "api_key", "private_key",
      "connection_string", "mongodb_uri", "session_id")
    keys.foreach { key =>
      val output = captured(request(s"query Selected($$$key: Boolean) { health { status } }",
        Json.obj(key -> Json.True)))
      assertEquals(output.hcursor.downField("variables").get[String](key), Right("[REDACTED]"))
    }
  }

  test("nested credentials and arbitrary-key scalar secrets never survive") {
    val values = Json.obj(
      "ordinary" -> Json.fromString("ARBITRARY_SECRET"),
      "number" -> Json.fromLong(987654321),
      "nil" -> Json.Null,
      "nested" -> Json.obj("include" -> Json.True, "API-KEY" -> Json.obj("ordinary" -> Json.fromString("KEY_SECRET"))),
      "array" -> Json.arr(Json.True, Json.fromString("ARRAY_SECRET"), Json.obj("ordinary" -> Json.fromString("DEEP_SECRET"))))
    val output = captured(request("query Selected($include: Boolean) { health { status } }", values))
    Vector("ARBITRARY_SECRET", "987654321", "KEY_SECRET", "ARRAY_SECRET", "DEEP_SECRET").foreach { secret =>
      assert(!output.noSpaces.contains(secret))
    }
    assertEquals(output.hcursor.downField("variables").downField("nested").get[String]("include"), Right("[REDACTED]"))
    assertEquals(output.hcursor.downField("variables").downField("nested").get[String]("API-KEY"), Right("[REDACTED]"))
    assertEquals(output.hcursor.downField("variables").get[String]("nil"), Right("[REDACTED]"))
  }

  test("Unicode names clip at 64 code points without splitting surrogate pairs") {
    val key = "😀" * 65
    val output = captured(request("{ health { status } }", Json.obj(key -> Json.fromString("VALUE_SECRET"))))
    val keys = output.hcursor.downField("variables").keys.getOrElse(fail("Missing keys")).toVector
    assertEquals(keys, Vector("😀" * 64))
    assertEquals(output.hcursor.get[Boolean]("truncated"), Right(true))
    assert(!output.noSpaces.contains("VALUE_SECRET"))
  }

  test("UTF-8 byte overflow returns a complete omission indicator") {
    val values = Json.fromFields((1 to 12).map(index => ("😀" * 60 + index.toString) -> Json.True))
    val output = captured(request("{ health { status } }", values))
    assertEquals(output, Json.obj("omitted" -> Json.True, "truncated" -> Json.True))
  }

  test("operation, variable, field and alias names are clipped with an explicit indicator") {
    val longName = "Name" + "x" * 70
    val output = captured(request(s"query $longName($$$longName: Boolean) {$longName: $longName}",
      Json.obj(longName -> Json.True)))
    assertEquals(output.hcursor.get[String]("operationName"), Right(longName.take(64)))
    assertEquals(output.hcursor.get[Boolean]("truncated"), Right(true))
    assert(!query(output).contains(longName))
    assertEquals(output.hcursor.downField("variables").get[Boolean](longName.take(64)), Right(true))
  }

  test("JSON key controls stay escaped and arbitrary metadata names are intentionally disclosed") {
    val key = "ordinary\n\"key"
    val output = captured(request("{field}", Json.obj(key -> Json.fromString("VALUE_SECRET"))))
    assertEquals(output.hcursor.downField("variables").get[String](key), Right("[REDACTED]"))
    assert(!output.noSpaces.contains('\n'))
  }

  test("depth, total node and per-container child budgets omit safely") {
    val nested = (1 to 9).foldLeft(Json.fromString("DEPTH_SECRET"))((value, _) => Json.arr(value))
    val broad = Json.fromFields((1 to 33).map(index => s"key$index" -> Json.True))
    val many = Json.obj("first" -> Json.arr(Vector.fill(32)(Json.True)*),
      "second" -> Json.arr(Vector.fill(32)(Json.True)*))
    Vector(Json.obj("nested" -> nested), broad, many).foreach { values =>
      assertEquals(captured(request("{ health { status } }", values)).hcursor.get[Boolean]("omitted"), Right(true))
    }
    val deepQuery = "{" + "field {" * 9 + "field" + "}" * 9 + "}"
    assertEquals(captured(request(deepQuery)).hcursor.get[Boolean]("omitted"), Right(true))
    val wideQuery = "{" + List.fill(33)("field").mkString(" ") + "}"
    assertEquals(captured(request(wideQuery)).hcursor.get[Boolean]("omitted"), Right(true))
  }

  test("fragment cycles terminate and unresolved references never use raw fallback") {
    val cyclic = captured(request("{...Loop} fragment Loop on Query {...Loop field(value: \"CYCLE_SECRET\")}"))
    assert(!cyclic.noSpaces.contains("CYCLE_SECRET"))
    assertEquals(DiagnosticPayload.capture(request("{...Missing}")), None)
    assertEquals(DiagnosticPayload.capture(request("query First {field} query Second {field}")), None)
    assertEquals(DiagnosticPayload.capture(request("query First {field}", operation = Some("Missing"))), None)
    assertEquals(DiagnosticPayload.capture(request("{field}", Json.arr())), None)
  }
}
