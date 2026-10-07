package com.example.graphQL.cats.repository.mongo

import cats.effect.Resource
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import org.bson.Document

final class MongoJobDiscoveryTimeoutIntegrationSpec extends MongoJobDiscoveryFixture {
  override protected def dedicatedMongo: Boolean = true
  test("real Mongo maxTimeMS exhaustion returns typed unavailable for nearby jobs and exact facets") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      for {
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Jobs,
          MongoHiringCodecs.job(job(1))
        )
        admin <- fixture.client.getDatabase("admin")
        _ <- Resource
          .make(
            MongoAccessEvaluationSupport
              .command(admin, new Document("configureFailPoint", "maxTimeAlwaysTimeOut").append("mode", "alwaysOn"))
              .void
          )(_ =>
            MongoAccessEvaluationSupport
              .command(admin, new Document("configureFailPoint", "maxTimeAlwaysTimeOut").append("mode", "off"))
              .void
          )
          .use { _ =>
            for {
              nearby <- repository.nearbyJobs(scope, NearbyJobsQuery(center, 10d, filter), 2).value
              facets <- repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, None)).value
              _ = assertEquals(nearby, Left(RepositoryError.Unavailable))
              _ = assertEquals(facets, Left(RepositoryError.Unavailable))
            } yield ()
          }
        recovered <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, 10d, filter), 2))
        _ = assertEquals(recovered.map(_.job.id), List(job(1).id))
      } yield ()
    }
  }

}
