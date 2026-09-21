package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import munit.CatsEffectSuite

final class GraphQLDocumentCacheSpec extends CatsEffectSuite {
  test("caches only documents explicitly stored after successful analysis") {
    val query = "{ health { status } }"
    GraphQLDocumentCache.resource.use { cache =>
      for {
        first <- IO.fromEither(cache.document(query).left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
        _ <- cache.store(query, first.document)
        second <- IO.fromEither(cache.document(query).left.map(failure => new AssertionError(s"Unexpected cache failure: $failure")))
      } yield {
        assert(!first.cached)
        assert(second.cached)
      }
    }
  }

  test("does not parse or cache malformed documents") {
    GraphQLDocumentCache.resource.use { cache =>
      IO {
        assertEquals(cache.document("{ health"), Left(HiringGraphQLSchema.Failure.InvalidQuery))
      }
    }
  }
}
