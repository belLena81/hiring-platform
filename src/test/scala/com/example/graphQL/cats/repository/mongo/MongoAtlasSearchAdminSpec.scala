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
}
