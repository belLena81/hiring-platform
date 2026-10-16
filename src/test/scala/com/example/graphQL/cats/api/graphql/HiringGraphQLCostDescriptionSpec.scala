package com.example.graphQL.cats.api.graphql

import munit.FunSuite

/** AIR-09.1: clients can price an expensive discovery query from the schema alone. */
final class HiringGraphQLCostDescriptionSpec extends FunSuite {
  private val expensiveRoots =
    List("semanticJobSearch", "recommendedJobs", "candidateMatches", "jobDiscoveryFacets", "nearbyJobs")

  expensiveRoots.foreach { root =>
    test(s"AIR-09.1 $root describes its cost formula, complexity and depth caps and expensive-root limit") {
      val description =
        HiringGraphQLSchema.schema.query.fields.find(_.name == root).flatMap(_.description).getOrElse("")
      assert(description.contains("Cost: 100 + "), clue(description))
      assert(description.contains("total complexity at most 1000"), clue(description))
      assert(description.contains("nesting depth at most 16"), clue(description))
      assert(description.contains("expensive discovery roots"), clue(description))
      assert(description.contains("extensions.cost"), clue(description))
    }
  }

  test("AIR-09.1 the page-priced roots state the page formula and the facets root states the lexical-query formula") {
    def describe(root: String): String =
      HiringGraphQLSchema.schema.query.fields.find(_.name == root).flatMap(_.description).getOrElse("")
    List("semanticJobSearch", "recommendedJobs", "candidateMatches", "nearbyJobs").foreach(root =>
      assert(describe(root).contains("100 + first * (1 + cost of the selected subfields)"), root)
    )
    assert(describe("jobDiscoveryFacets").contains("100 + 25 * (1 + cost of the selected subfields)"))
  }

  test("AIR-09.1 the SDL carries the cost description") {
    assert(HiringGraphQLSchema.sdl.contains("Cost: 100 + first * (1 + cost of the selected subfields)"))
  }
}
