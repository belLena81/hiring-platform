package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import munit.CatsEffectSuite

final class GraphQLDocumentCacheSpec extends CatsEffectSuite {
  test("caches only documents explicitly stored after successful analysis") {
    val query = "{ me { id } }"
    GraphQLDocumentCache.resource.use { cache =>
      for {
        firstResult <- cache.document(query)
        first <- IO.fromEither(
          firstResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure"))
        )
        _ <- cache.store(query, first.document)
        secondResult <- cache.document(query)
        second <- IO.fromEither(
          secondResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure"))
        )
      } yield {
        assert(!first.cached)
        assert(second.cached)
        // AC-15.1 the cached parse is reused, not parsed again.
        assert(second.document eq first.document)
      }
    }
  }

  test("defers cache reads until the returned IO runs") {
    val query = "{ me { id } }"
    GraphQLDocumentCache.resource.use { cache =>
      val lookup = cache.document(query)
      for {
        firstResult <- lookup
        first <- IO.fromEither(
          firstResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure"))
        )
        _ <- cache.store(query, first.document)
        secondResult <- lookup
        second <- IO.fromEither(
          secondResult.left.map(failure => new AssertionError(s"Unexpected cache failure: $failure"))
        )
      } yield assert(second.cached)
    }
  }

  test("reacquiring one resource value creates an independent cache") {
    val resource = GraphQLDocumentCache.resource
    val query = "{ me { id } }"
    for {
      _ <- resource.use { cache =>
        cache.document(query).flatMap {
          case Right(document) => cache.store(query, document.document)
          case Left(failure)   => IO.raiseError(new AssertionError(s"Unexpected cache failure: $failure"))
        }
      }
      result <- resource.use(_.document(query))
    } yield assert(result.exists(document => !document.cached))
  }

  test("does not parse or cache malformed documents") {
    GraphQLDocumentCache.resource.use { cache =>
      cache.document("{ me").map(result => assertEquals(result, Left(HiringGraphQLSchema.Failure.InvalidQuery)))
    }
  }
}
