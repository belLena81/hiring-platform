package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import munit.CatsEffectSuite

final class GraphQLDocumentCacheSpec extends CatsEffectSuite {
  test("caches only documents explicitly stored after successful analysis") {
    val query = "{ health { status } }"
    GraphQLDocumentCache.resource.use { cache =>
      for {
        firstResult <- cache.document(query)
        first <- IO.fromEither(firstResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
        _ <- cache.store(query, first.document)
        secondResult <- cache.document(query)
        second <- IO.fromEither(secondResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
      } yield {
        assert(!first.cached)
        assert(second.cached)
      }
    }
  }

  test("defers cache reads until the returned IO runs") {
    val query = "{ health { status } }"
    GraphQLDocumentCache.resource.use { cache =>
      val lookup = cache.document(query)
      for {
        firstResult <- lookup
        first <- IO.fromEither(firstResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
        _ <- cache.store(query, first.document)
        secondResult <- lookup
        second <- IO.fromEither(secondResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
      } yield assert(second.cached)
    }
  }

  test("does not parse or cache malformed documents") {
    GraphQLDocumentCache.resource.use { cache =>
      cache.document("{ health").map(result => assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery)))
    }
  }
}
