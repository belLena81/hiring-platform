package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.mongodb.ReadConcern
import com.mongodb.reactivestreams.client.MongoClients
import munit.CatsEffectSuite

/** DDR-05.2: `$listSearchIndexes` only runs under read concern `local`, while the application client defaults to
  * `majority`. The client connects lazily, so no server is needed to inspect the handles.
  */
final class MongoAtlasSearchAdminSpec extends CatsEffectSuite {
  test("DDR-05.2 the search index listing handle pins local read concern under a majority client") {
    Resource
      .fromAutoCloseable(IO.delay(MongoClients.create(MongoDatabaseProbe.effectiveSettings("mongodb://127.0.0.1:1"))))
      .use { client =>
        IO {
          val database = client.getDatabase("search_admin_spec")
          assertEquals(database.getReadConcern, ReadConcern.MAJORITY)
          val handle = MongoAtlasSearchAdmin.searchMetadataCollection(database, "jobs")
          assertEquals(handle.getReadConcern, ReadConcern.LOCAL)
          // The pin is local to the handle: the database and other collections keep majority.
          assertEquals(database.getCollection("jobs").getReadConcern, ReadConcern.MAJORITY)
        }
      }
  }

  test("DDR-05.2 the lexical job matcher runs its $search on a local read concern handle") {
    MongoDatabaseProbe.clientResource("mongodb://127.0.0.1:1").use { client =>
      for {
        database <- client.getDatabase("search_admin_spec")
        policy <- DiscoveryQueryPolicy.create(scala.concurrent.duration.DurationInt(1).second, 1)
        matcher = new MongoLexicalJobMatcher(
          database,
          "jobs_lexical",
          com.example.graphQL.cats.service.Diagnostics.noop[IO],
          policy
        )
        handle <- matcher.jobsSearchHandle
      } yield assertEquals(handle.underlying.getReadConcern, ReadConcern.LOCAL)
    }
  }

  test("DDR-05.2 the search aggregation handle is local while the same collection elsewhere stays majority") {
    MongoDatabaseProbe.clientResource("mongodb://127.0.0.1:1").use { client =>
      for {
        database <- client.getDatabase("search_admin_spec")
        search <- MongoAtlasSearchAdmin.searchCollection(database, "jobs")
        business <- Mongo4catsCollections.documents(database, "jobs")
      } yield {
        assertEquals(search.underlying.getReadConcern, ReadConcern.LOCAL)
        assertEquals(business.underlying.getReadConcern, ReadConcern.MAJORITY)
      }
    }
  }
}
