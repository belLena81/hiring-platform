package com.example.graphQL.cats.api.graphql

import io.circe.Json
import io.circe.parser.parse as parseJson
import munit.FunSuite
import sangria.parser.QueryParser

/** HOR-01 and HOR-02: GraphQL operations are counted by schema root field and a bounded outcome class. */
final class GraphQLOperationOutcomesSpec extends FunSuite {
  import GraphQLOperationKind.*
  import GraphQLOutcome.*

  private def document(query: String) = QueryParser.parse(query).fold(throw _, identity)

  private def json(text: String): Json = parseJson(text).fold(throw _, identity)

  private def classify(query: String, response: String, operationName: Option[String] = None) =
    GraphQLOperationOutcomes.classify(document(query), operationName, json(response))

  private def error(path: String, code: String) =
    s"""{"message":"x","path":$path,"extensions":{"code":"$code"}}"""

  test("HOR-01 a root field without errors counts as success") {
    assertEquals(
      classify("{ me { id } }", """{"data":{"me":{"id":"1"}}}"""),
      List(GraphQLRootFieldOutcome(Query, "me", Success))
    )
  }

  test("HOR-01 a field error returned with HTTP 200 counts as field_error on its root field") {
    assertEquals(
      classify("{ me { id } }", s"""{"data":{"me":null},"errors":[${error("""["me"]""", "NOT_FOUND")}]}"""),
      List(GraphQLRootFieldOutcome(Query, "me", FieldError))
    )
  }

  test("HOR-01 FORBIDDEN is denied, UNAUTHORIZED is unauthenticated, RATE_LIMITED is rejected") {
    def outcomeOf(code: String) =
      classify("{ me { id } }", s"""{"data":{"me":null},"errors":[${error("""["me"]""", code)}]}""").map(_.outcome)
    assertEquals(outcomeOf("FORBIDDEN"), List(Denied))
    assertEquals(outcomeOf("RECRUITER_REQUIRED"), List(Denied))
    assertEquals(outcomeOf("UNAUTHORIZED"), List(Unauthenticated))
    assertEquals(outcomeOf("INVALID_CREDENTIALS"), List(Unauthenticated))
    assertEquals(outcomeOf("RATE_LIMITED"), List(Rejected))
  }

  test("HOR-01 a dependency failure, an unknown code and a code-less error are internal") {
    def outcomeOf(errorJson: String) =
      classify("{ me { id } }", s"""{"data":{"me":null},"errors":[$errorJson]}""").map(_.outcome)
    assertEquals(outcomeOf(error("""["me"]""", "UNAVAILABLE")), List(Internal))
    assertEquals(outcomeOf(error("""["me"]""", "VECTOR_SEARCH_UNAVAILABLE")), List(Internal))
    assertEquals(outcomeOf(error("""["me"]""", "SOMETHING_NEW")), List(Internal))
    assertEquals(outcomeOf("""{"message":"Execution failed","path":["me"]}"""), List(Internal))
  }

  test("HOR-01 a caller input refusal marked exceptional stays field_error") {
    assertEquals(
      classify("{ me { id } }", s"""{"data":null,"errors":[${error("""["me"]""", "INPUT_TOO_LARGE")}]}""")
        .map(_.outcome),
      List(FieldError)
    )
  }

  test("HOR-02 the label is the schema field name, never the alias") {
    val result = classify(
      """{ secretCandidateAlias: me { id } }""",
      s"""{"data":{"secretCandidateAlias":null},"errors":[${error("""["secretCandidateAlias"]""", "UNAUTHORIZED")}]}"""
    )
    assertEquals(result, List(GraphQLRootFieldOutcome(Query, "me", Unauthenticated)))
  }

  test("HOR-02 a caller-supplied operation name is never part of the result") {
    val result = classify(
      "query SomethingTheCallerChose { me { id } }",
      """{"data":{"me":null}}""",
      Some("SomethingTheCallerChose")
    )
    assertEquals(result, List(GraphQLRootFieldOutcome(Query, "me", Success)))
  }

  test("HOR-01 several root fields are counted once each with their own outcome") {
    val result = classify(
      "{ a: me { id } b: me { id } c: accountDeletionStatus(receiptId: \"r\") { status } }",
      s"""{"data":{"a":{"id":"1"},"b":null,"c":null},"errors":[${error("""["b"]""", "FORBIDDEN")},${error(
          """["c","status"]""",
          "UNAVAILABLE"
        )}]}"""
    )
    assertEquals(
      result,
      List(
        GraphQLRootFieldOutcome(Query, "me", Success),
        GraphQLRootFieldOutcome(Query, "me", Denied),
        GraphQLRootFieldOutcome(Query, "accountDeletionStatus", Internal)
      )
    )
  }

  test("HOR-01 the most severe error of one root field decides its outcome") {
    val result = classify(
      "{ me { id } }",
      s"""{"data":{"me":null},"errors":[${error("""["me"]""", "NOT_FOUND")},${error(
          """["me","id"]""",
          "UNAVAILABLE"
        )}]}"""
    )
    assertEquals(result.map(_.outcome), List(Internal))
  }

  test("HOR-01 an error without a path (a request level refusal) applies to every root field") {
    val result = classify(
      "{ me { id } accountDeletionStatus(receiptId: \"r\") { status } }",
      """{"data":null,"errors":[{"message":"Too many requests","extensions":{"code":"RATE_LIMITED"}}]}"""
    )
    assertEquals(result.map(r => r.rootField -> r.outcome), List("me" -> Rejected, "accountDeletionStatus" -> Rejected))
  }

  test("HOR-01 mutations carry the mutation operation type") {
    val result = classify(
      "mutation { cancelInterview(input: {}) { __typename } }",
      """{"data":{"cancelInterview":{"__typename":"X"}}}"""
    )
    assertEquals(result, List(GraphQLRootFieldOutcome(Mutation, "cancelInterview", Success)))
  }

  test("HOR-02 introspection fields and __typename collapse to _introspection") {
    val result =
      classify("{ __schema { types { name } } __type(name: \"User\") { name } __typename }", """{"data":{}}""")
    assertEquals(result.map(_.rootField), List("_introspection", "_introspection", "_introspection"))
  }

  test("HOR-02 a root field the schema does not define collapses to _unknown") {
    val result = classify(
      "{ definitelyNotAField { id } }",
      s"""{"data":null,"errors":[{"message":"x","path":["definitelyNotAField"]}]}"""
    )
    assertEquals(result, List(GraphQLRootFieldOutcome(Query, GraphQLOperationOutcomes.UnknownField, Internal)))
  }

  test("HOR-02 a mutation field name under a query operation does not leak through") {
    val result = classify("{ cancelInterview { id } }", """{"data":null}""")
    assertEquals(result.map(_.rootField), List(GraphQLOperationOutcomes.UnknownField))
  }

  test("HOR-02 an operation name that selects no operation collapses to the _invalid request count") {
    val result = classify("query A { me { id } }", """{"data":null}""", Some("NoSuchOperationName"))
    assertEquals(result, GraphQLOperationOutcomes.invalidRequest)
  }

  test("HOR-01 operationName picks which operation of the document is counted") {
    val query = "query A { me { id } } mutation B { cancelInterview(input: {}) { __typename } }"
    assertEquals(classify(query, """{"data":{}}""", Some("A")).map(_.rootField), List("me"))
    assertEquals(classify(query, """{"data":{}}""", Some("B")).map(_.rootField), List("cancelInterview"))
  }

  test("HOR-01 root fields reached through fragments are counted and a cyclic spread terminates") {
    val result = classify(
      "{ ...F ... on Query { accountDeletionStatus(receiptId: \"r\") { status } } } fragment F on Query { me { id } ...F }",
      """{"data":{"me":null,"accountDeletionStatus":null}}"""
    )
    assertEquals(result.map(_.rootField), List("me", "accountDeletionStatus"))
  }

  test("HOR-01 a skipped conditional root field is not counted unless it appears in data or an error path") {
    val query = "query($s: Boolean!) { me @skip(if: $s) { id } accountDeletionStatus(receiptId: \"r\") { status } }"
    assertEquals(
      classify(query, """{"data":{"accountDeletionStatus":null}}""").map(_.rootField),
      List("accountDeletionStatus")
    )
    assertEquals(
      classify(query, """{"data":{"me":null,"accountDeletionStatus":null}}""").map(_.rootField),
      List("me", "accountDeletionStatus")
    )
  }

  test("HOR-01 a response key used twice is one execution") {
    val result = classify("{ me { id } me { id } }", """{"data":{"me":null}}""")
    assertEquals(result.map(_.rootField), List("me"))
  }

  test("HOR-01 an exhausted cost budget refuses every root field of the document") {
    val refused = GraphQLOperationOutcomes.refused(
      document("{ me { id } alias: me { id } }"),
      None,
      GraphQLOutcome.Rejected
    )
    assertEquals(refused.map(r => r.rootField -> r.outcome), List("me" -> Rejected, "me" -> Rejected))
  }

  test("HOR-02 every error code the catalog can emit maps to a closed outcome class") {
    val classes = GraphQLFailureCatalog.errorCodes.map((code, _) => GraphQLFailureCatalog.outcomeOf(Some(code)))
    assert(classes.forall(GraphQLOutcome.values.contains))
    assert(!classes.contains(Success))
    // The codes tested above are the catalog's, not invented ones.
    val codes = GraphQLFailureCatalog.errorCodes.map(_._1).toSet
    assert(
      Set("FORBIDDEN", "UNAUTHORIZED", "RATE_LIMITED", "UNAVAILABLE", "NOT_FOUND", "INPUT_TOO_LARGE").subsetOf(codes)
    )
  }

  test("HOR-02 label values come from closed sets") {
    assertEquals(
      GraphQLOutcome.values.map(_.label).toSet,
      Set("success", "field_error", "denied", "unauthenticated", "rejected", "internal")
    )
    assertEquals(GraphQLOperationKind.values.map(_.label).toSet, Set("query", "mutation", "_unknown"))
  }

  test("HOR-02 an acyclic doubling fragment chain is collected in linear time") {
    val depth = 25
    val fragments = (0 until depth)
      .map(level => s"fragment f$level on Query { ...f${level + 1} ...f${level + 1} }")
      .mkString(" ") + s" fragment f$depth on Query { me { id } }"
    val started = System.nanoTime()
    val result = classify(s"{ ...f0 }  $fragments", """{"data":{"me":null}}""")
    val elapsedMillis = (System.nanoTime() - started) / 1000000
    assertEquals(result.map(_.rootField), List("me"))
    assert(elapsedMillis < 2000, clue(elapsedMillis))
  }

  test("HOR-02 a document with more root fields than the cap collects at most the cap") {
    val query = "{ " + (1 to 5000).map(i => s"a$i: me { id }").mkString(" ") + " }"
    assert(classify(query, """{"data":{}}""").size <= 1000)
  }
}
