package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.mongodb.client.model.{Filters, Updates}
import java.util.UUID
import org.bson.Document
import scala.jdk.CollectionConverters.*

final class MongoJobDiscoveryIntegrationSpec extends MongoJobDiscoveryFixture {

  test(
    "authoritative discovery distinguishes empty jobs from a revoked cached actor, including cursors beyond radius"
  ) {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      val base = NearbyJobsQuery(center, 10d, filter)
      val beyond = base.copy(after = Some(NearbyJobCursor(11d, JobId(new UUID(0L, 1L)), base.fingerprint)))
      for {
        empty <- repository.nearbyJobs(scope, base, 2).value
        beyondEmpty <- repository.nearbyJobs(scope, beyond, 2).value
        facets <- repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, None)).value
        _ = assertEquals(empty, Right(Nil))
        _ = assertEquals(beyondEmpty, Right(Nil))
        _ = assertEquals(facets, Right(JobDiscoveryFacets(Nil, Nil, Nil, Nil, truncated = false)))
        users <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
        _ <- users.deleteOne(Filters.eq(MongoFields.Id, scope.userId.value.toString))
        revoked <- repository.nearbyJobs(scope, base, 2).value
        revokedBeyond <- repository.nearbyJobs(scope, beyond, 2).value
        revokedFacets <- repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, None)).value
        _ = assertEquals(revoked, Left(RepositoryError.AuthorityRevoked))
        _ = assertEquals(revokedBeyond, Left(RepositoryError.AuthorityRevoked))
        _ = assertEquals(revokedFacets, Left(RepositoryError.AuthorityRevoked))
      } yield ()
    }
  }

  test("radius query excludes remote, missing, closed and deleted jobs; full precision tie pagination is complete") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      val query = NearbyJobsQuery(center, 10d, filter)
      val values = (1 to 24).toList
        .map(index => job(index, Some(GeoPoint(center.latitude + 0.001d, center.longitude)))) ++ List(
        job(25, remote = true),
        job(26, None),
        job(27).copy(status = JobStatus.Closed)
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- values.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        )
        first <- success(repository.nearbyJobs(scope, query, 7))
        _ = assertEquals(first.map(_.job.id), values.take(7).map(_.id))
        cursor = NearbyJobCursor(first.last.distanceKm, first.last.job.id, query.fingerprint)
        rest <- success(repository.nearbyJobs(scope, query.copy(after = Some(cursor)), 30))
        _ = assertEquals((first ++ rest).map(_.job.id), values.take(24).map(_.id))
        _ = assert((first ++ rest).forall(value => value.distanceKm > 0d && value.distanceKm < 1d))
        coll <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
        _ <- (
          coll.deleteOne(Filters.eq("_id", values.head.id.value.toString)),
          success(repository.nearbyJobs(scope, query, 30))
        ).parTupled
        deleted <- success(repository.nearbyJobs(scope, query, 30))
        _ = assertEquals(deleted.size, 23)
        _ <- (
          coll.updateOne(
            Filters.eq("_id", values(1).id.value.toString),
            Updates.set("status", JobStatus.Closed.toString)
          ),
          success(repository.nearbyJobs(scope, query, 30))
        ).parTupled
        closed <- success(repository.nearbyJobs(scope, query, 30))
        _ = assertEquals(closed.size, 22)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }

  test("facets count the complete filtered corpus and bound buckets; radius facets exclude remote and absent points") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      val values = (1 to 24).toList
        .map(job(_)) ++ List(job(25, remote = true), job(26, None), job(27).copy(status = JobStatus.Closed))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- values.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        )
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
        _ <- collection.updateOne(
          Filters.eq("_id", job(1).id.value.toString),
          Updates.set("skills", List("Scala", "Scala", "Skill1").asJava)
        )
        all <- success(repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, None)))
        _ = assertEquals(all.skills.head, JobFacetBucket("Scala", 26L))
        _ = assertEquals(all.skills.size, 20)
        _ = assert(all.truncated)
        nearby <- success(repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, Some(NearbyRadius(center, 10d)))))
        _ = assertEquals(nearby.skills.head, JobFacetBucket("Scala", 24L))
        _ = assertEquals(nearby.remote, List(JobFacetBucket("false", 24L)))
        filtered <- success(
          repository.jobDiscoveryFacets(scope, JobFacetQuery(filter.copy(skills = Set("Scala", "Skill1")), None))
        )
        _ = assertEquals(filtered.cities, List(JobFacetBucket("Nicosia", 1L)))
        dateFilter = filter.copy(city = Some("Nicosia"), skills = Set("Scala"), createdAfter = Some(now.plusSeconds(1)))
        dated <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, 10d, dateFilter), 30))
        _ = assertEquals(dated, Nil)
        datedFacets <- success(repository.jobDiscoveryFacets(scope, JobFacetQuery(dateFilter, None)))
        _ = assertEquals(datedFacets.cities, Nil)
      } yield ()
    }
  }
  test("Mongo maximum distance observes its measured boundary within one metre and excludes two metres outside") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      val value = job(1, Some(GeoPoint(center.latitude + 0.01d, center.longitude)))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        measured <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, 10d, filter), 2))
        distance = measured.head.distanceKm
        boundary <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, distance, filter), 2))
        included <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, distance + 0.001d, filter), 2))
        excluded <- success(repository.nearbyJobs(scope, NearbyJobsQuery(center, distance - 0.002d, filter), 2))
        _ = assertEquals(included.map(_.job.id), List(value.id))
        _ = assert(boundary.isEmpty || boundary.map(_.job.id) == List(value.id))
        _ = assert(boundary.forall(hit => math.abs(hit.distanceKm - distance) <= 0.001d))
        _ = assertEquals(excluded, Nil)
        _ <- IO.println(s"Geographic boundary observation: distanceKm=$distance exactBoundaryMatches=${boundary.size}")
      } yield ()
    }
  }

  List(
    ("antimeridian", GeoPoint(0d, 179.9d), GeoPoint(0d, -179.9d), GeoPoint(0d, -179d), 30d),
    ("north pole", GeoPoint(90d, 0d), GeoPoint(89.9d, 180d), GeoPoint(89d, 0d), 20d),
    ("negative longitude", GeoPoint(51.5d, -0.12d), GeoPoint(51.51d, -0.12d), GeoPoint(51.7d, -0.12d), 2d)
  ).foreach { case (name, origin, inside, outside, radius) =>
    test(s"nearby jobs and exact facets agree across $name") {
      discoveryResource.use { case (fixture, policy, scope) =>
        val repository = MongoJobRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop,
          Some(policy)
        )
        val values = List(
          job(1, Some(origin)),
          job(2, Some(inside)),
          job(3, Some(outside)),
          job(4, Some(inside), remote = true),
          job(5, None),
          job(6, Some(inside)).copy(status = JobStatus.Closed)
        )
        for {
          _ <- values.traverse_(value =>
            MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
          )
          hits <- success(repository.nearbyJobs(scope, NearbyJobsQuery(origin, radius, filter), 10))
          facets <- success(
            repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, Some(NearbyRadius(origin, radius))))
          )
          _ = assertEquals(hits.map(_.job.id), values.take(2).map(_.id))
          _ = assertEquals(facets.skills.find(_.value == "Scala").map(_.count), Some(hits.size.toLong))
          _ = assertEquals(
            facets.skills.toSet,
            Set(JobFacetBucket("Scala", 2L), JobFacetBucket("Skill1", 1L), JobFacetBucket("Skill2", 1L))
          )
          _ = assertEquals(facets.remote, List(JobFacetBucket("false", 2L)))
          _ = assertEquals(facets.cities, List(JobFacetBucket("Nicosia", 2L)))
          _ = assert(hits.forall(hit => hit.distanceKm >= 0d && hit.distanceKm <= radius))
        } yield ()
      }
    }
  }

  test("zero-distance ties survive encoded cursor pagination to exhaustion without gaps or duplicates") {
    discoveryResource.use { case (fixture, policy, scope) =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop,
        Some(policy)
      )
      val query = NearbyJobsQuery(center, 10d, filter)
      val values = (1 to 24).toList.map(job(_))
      def pages(after: Option[NearbyJobCursor], remaining: Int): IO[List[NearbyJob]] =
        if (remaining == 0) IO.raiseError(new AssertionError("Nearby cursor pagination did not terminate"))
        else
          success(repository.nearbyJobs(scope, query.copy(after = after), 7)).flatMap { page =>
            page.lastOption.fold(IO.pure(List.empty[NearbyJob])) { last =>
              val encoded = NearbyJobCursorCodec.encode(last.distanceKm, last.job.id, query)
              IO.fromEither(
                NearbyJobCursorCodec.decode(encoded, query).leftMap(error => new AssertionError(error.message))
              ).flatMap(cursor => pages(Some(cursor), remaining - 1).map(page ++ _))
            }
          }
      for {
        _ <- values.traverse_(value =>
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        )
        hits <- pages(None, 6)
        facets <- success(repository.jobDiscoveryFacets(scope, JobFacetQuery(filter, Some(NearbyRadius(center, 10d)))))
        _ = assertEquals(hits.map(_.job.id), values.map(_.id))
        _ = assert(hits.forall(_.distanceKm == 0d))
        _ = assertEquals(facets.skills.find(_.value == "Scala").map(_.count), Some(24L))
      } yield ()
    }
  }

  test(
    "geo cutover fails on incompatible legacy coordinates, rejects concurrent invalid writes and resumes after repair"
  ) {
    mongoResource.use { fixture =>
      val existing = MongoHiringCodecs.job(job(1))
      val invalid = new Document("type", "Point").append("coordinates", List(181d, 0d).asJava)
      val _ = existing.get("location", classOf[Document]).put("point", invalid)
      for {
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, existing)
        failure <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        _ = assert(failure.isLeft)
        ledger <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", "006_job_geo_points")
        )
        _ = assertEquals(ledger.map(_.getString("state")), Some("Running"))
        coll <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
        invalidWrite <- coll.insertOne(new Document(existing).append("_id", job(2).id.value.toString)).attempt
        _ = assert(invalidWrite.isLeft)
        _ <- coll.updateOne(
          Filters.eq("_id", job(1).id.value.toString),
          Updates.set(
            "location.point",
            new Document("type", "Point").append("coordinates", List(center.longitude, center.latitude).asJava)
          )
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        completed <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", "006_job_geo_points")
        )
        _ = assertEquals(completed.map(_.getString("state")), Some("Complete"))
        indexes <- coll.listIndexes[Document]
        _ = assert(indexes.exists(_.getString("name") == MongoHiringSetup.JobsLocationPointIndex))
      } yield ()
    }
  }

}
