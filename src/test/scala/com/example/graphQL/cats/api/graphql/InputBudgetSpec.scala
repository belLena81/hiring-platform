package com.example.graphQL.cats.api.graphql

import io.circe.Json
import munit.FunSuite

final class InputBudgetSpec extends FunSuite {
  private def accepted(query: String): Boolean =
    GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces).isDefined

  test("lexical budgets handle strings, escaped quotes, block strings and comments") {
    assert(InputBudget.lexical("{ field(argument: \"{[\\\"]}\") }", graphql = true))
    assert(InputBudget.lexical("{ field(argument: \"\"\" { nested \\\"\"\" } \"\"\") } # [[[", graphql = true))
    assert(!InputBudget.lexical("{ field(argument: \"unterminated) }", graphql = true))
    assert(!InputBudget.lexical("{ ]", graphql = true))
    assert(InputBudget.lexical("[" * 32 + "0" + "]" * 32, graphql = false))
    assert(!InputBudget.lexical("[" * 33 + "0" + "]" * 33, graphql = false))
    assert(InputBudget.lexical(List.fill(4096)("token").mkString(" "), graphql = true))
    assert(!InputBudget.lexical(List.fill(4097)("token").mkString(" "), graphql = true))
  }

  test("compact spreads and adjacent signed numbers count as separate tokens") {
    assert(InputBudget.lexical("...F" * 2048, graphql = true))
    assert(!InputBudget.lexical("...F" * 2049, graphql = true))
    assert(InputBudget.lexical("-1" * 4096, graphql = true))
    assert(!InputBudget.lexical("-1" * 4097, graphql = true))
    val numbers = "-1" * 4097
    assert(!accepted(s"query Check($$values: [Int] = [$numbers]) { health { status } }"))
    assert(!accepted(s"{ health(values: [$numbers]) { status } }"))
    assert(InputBudget.lexical(List.fill(4096)("-1.25e+12").mkString(" "), graphql = true))
    assert(!InputBudget.lexical(List.fill(4097)("-1.25e+12").mkString(" "), graphql = true))
    assert(InputBudget.lexical("[0,-2,3.14,4e-2]", graphql = false))
    assert(InputBudget.lexical("0" * 4096, graphql = true))
    assert(!InputBudget.lexical("0" * 4097, graphql = true))
    val zeros = "0" * 4097
    assert(!accepted(s"query Check($$values: [Int] = [$zeros]) { health { status } }"))
    assert(!accepted(s"{ health(values: [$zeros]) { status } }"))
    assert(InputBudget.lexical("[0,-0,0.125,-0.25e+2,0e-2]", graphql = true))
  }

  test("selected-operation aliases and depth have inclusive boundaries") {
    assert(accepted("{" + (1 to 32).map(index => s"alias$index: health {status}").mkString(" ") + "}"))
    assert(!accepted("{" + (1 to 33).map(index => s"alias$index: health {status}").mkString(" ") + "}"))
    assert(accepted("{ " + "field {" * 15 + "field" + "}" * 15 + "}"))
    assert(!accepted("{ " + "field {" * 16 + "field" + "}" * 16 + "}"))
  }

  test("complexity counts expanded field occurrences, including repeated fragments") {
    val fragment = " fragment Many on Query {" + List.fill(50)("health {status}").mkString(" ") + "}"
    assert(accepted("{" + List.fill(10)("...Many").mkString(" ") + "}" + fragment))
    assert(!accepted("{" + List.fill(11)("...Many").mkString(" ") + "}" + fragment))
    assert(!accepted("{...Loop} fragment Loop on Query {...Loop}"))
    assert(!accepted("{...Missing}"))
  }

  test("whole-document budget bounds unselected fragment expansion before validation") {
    val fragments = (0 to 14).map { index =>
      if (index == 14) "fragment Fragment14 on Query { health { status } }"
      else s"fragment Fragment$index on Query { ...Fragment${index + 1} ...Fragment${index + 1} }"
    }.mkString(" ")
    assert(!accepted("{ health { status } } " + fragments))
  }
}
