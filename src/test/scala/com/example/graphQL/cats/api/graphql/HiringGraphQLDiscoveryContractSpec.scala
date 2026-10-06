package com.example.graphQL.cats.api.graphql

import munit.FunSuite
import sangria.parser.QueryParser
import sangria.validation.QueryValidator

final class HiringGraphQLDiscoveryContractSpec extends FunSuite {
  test("geographic hits and exact structured facet operations validate against the active schema") {
    val query = """query {
      nearbyJobs(center: {latitude: 35.1856, longitude: 33.3823}, radiusKm: 20,
        filter: {city: "Nicosia", skills: ["Scala"]}, first: 7) {
        results { job { id location { coordinates { latitude longitude } } } distanceKm cursor }
        hasNextPage
      }
      jobDiscoveryFacets(filter: {skills: ["Scala"]}) {
        skills { value count } countries { value count } cities { value count } remote { value count } truncated
      }
      nearbyFacets: jobDiscoveryFacets(center: {latitude: 35.1856, longitude: 33.3823}, radiusKm: 20) {
        skills { value count } truncated
      }
    }"""
    val document = QueryParser.parse(query).fold(error => fail(error.getMessage), identity)
    assertEquals(
      QueryValidator.default.validateQuery(HiringGraphQLSchema.schema, document, Map.empty, None).toList,
      Nil
    )
  }
}
