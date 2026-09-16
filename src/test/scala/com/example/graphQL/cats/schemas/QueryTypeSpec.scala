package com.example.graphQL.cats.schemas

import munit.FunSuite
import sangria.parser.QueryParser
import sangria.validation.QueryValidator

final class QueryTypeSpec extends FunSuite {
  test("existing user queries and role enum remain valid") {
    val operation = QueryParser.parse("""
      query ExistingUserQueries($identifier: String!, $role: Role!) {
        users { id name email role }
        userById(identifier: $identifier) { id name email role }
        usersByRole(role: $role) { id name email role }
        admins: usersByRole(role: ADMIN) { id }
        recruiters: usersByRole(role: RECRUITER) { id }
        candidates: usersByRole(role: CANDIDATE) { id }
      }
    """).fold(error => fail(error.getMessage), identity)

    assertEquals(
      QueryValidator.default.validateQuery(QueryType.schema, operation, Map.empty, None),
      Vector.empty
    )
  }
}
