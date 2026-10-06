package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.mongodb.client.model.{Filters, Updates}
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import org.bson.Document
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

final class MongoJobDiscoveryIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val center = GeoPoint(35.1856d, 33.3823d)
  private val filter = JobSearchFilter(None, Set.empty, None)
  private def job(index: Int, point: Option[GeoPoint] = Some(center), remote: Boolean = false): Job =
    Job(
      JobId(new UUID(0L, index.toLong)),
      UserId(new UUID(1L, 1L)),
      "Engineer",
      "Build hiring",
      List("Scala"),
      Set("Scala", s"Skill$index"),
      Location("Cyprus", "Nicosia", remote, point),
      JobStatus.Open,
      now,
      now
    )
  private def success[A](effect: RepositoryIO[A]): IO[A] =
    effect.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"Repository failed: $error")), IO.pure))

  test("radius query excludes remote, missing, closed and deleted jobs; full precision tie pagination is complete") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
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
        first <- success(repository.nearbyJobs(query, 7))
        _ = assertEquals(first.map(_.job.id), values.take(7).map(_.id))
        cursor = NearbyJobCursor(first.last.distanceKm, first.last.job.id.value.toString, query.fingerprint)
        rest <- success(repository.nearbyJobs(query.copy(after = Some(cursor)), 30))
        _ = assertEquals((first ++ rest).map(_.job.id), values.take(24).map(_.id))
        _ = assert((first ++ rest).forall(value => value.distanceKm > 0d && value.distanceKm < 1d))
        coll <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Jobs)
        _ <- (
          coll.deleteOne(Filters.eq("_id", values.head.id.value.toString)),
          success(repository.nearbyJobs(query, 30))
        ).parTupled
        deleted <- success(repository.nearbyJobs(query, 30))
        _ = assertEquals(deleted.size, 23)
        _ <- (
          coll.updateOne(
            Filters.eq("_id", values(1).id.value.toString),
            Updates.set("status", JobStatus.Closed.toString)
          ),
          success(repository.nearbyJobs(query, 30))
        ).parTupled
        closed <- success(repository.nearbyJobs(query, 30))
        _ = assertEquals(closed.size, 22)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }

  test("facets count the complete filtered corpus and bound buckets; radius facets exclude remote and absent points") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
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
        all <- success(repository.jobDiscoveryFacets(JobFacetQuery(filter, None)))
        _ = assertEquals(all.skills.head, JobFacetBucket("Scala", 26L))
        _ = assertEquals(all.skills.size, 20)
        _ = assert(all.truncated)
        nearby <- success(repository.jobDiscoveryFacets(JobFacetQuery(filter, Some(NearbyRadius(center, 10d)))))
        _ = assertEquals(nearby.skills.head, JobFacetBucket("Scala", 24L))
        _ = assertEquals(nearby.remote, List(JobFacetBucket("false", 24L)))
        filtered <- success(
          repository.jobDiscoveryFacets(JobFacetQuery(filter.copy(skills = Set("Scala", "Skill1")), None))
        )
        _ = assertEquals(filtered.cities, List(JobFacetBucket("Nicosia", 1L)))
        dateFilter = filter.copy(city = Some("Nicosia"), skills = Set("Scala"), createdAfter = Some(now.plusSeconds(1)))
        dated <- success(repository.nearbyJobs(NearbyJobsQuery(center, 10d, dateFilter), 30))
        _ = assertEquals(dated, Nil)
        datedFacets <- success(repository.jobDiscoveryFacets(JobFacetQuery(dateFilter, None)))
        _ = assertEquals(datedFacets.cities, Nil)
      } yield ()
    }
  }
  test("Mongo maximum distance includes its computed radius boundary and excludes a point two metres outside") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val repository = MongoJobRepository.transactional(
        fixture.database,
        fixture.client,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop
      )
      val value = job(1, Some(GeoPoint(center.latitude + 0.01d, center.longitude)))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
        measured <- success(repository.nearbyJobs(NearbyJobsQuery(center, 10d, filter), 2))
        distance = measured.head.distanceKm
        included <- success(repository.nearbyJobs(NearbyJobsQuery(center, distance + 0.001d, filter), 2))
        excluded <- success(repository.nearbyJobs(NearbyJobsQuery(center, distance - 0.002d, filter), 2))
        _ = assertEquals(included.map(_.job.id), List(value.id))
        _ = assertEquals(excluded, Nil)
      } yield ()
    }
  }

  test(
    "geo cutover fails on incompatible legacy coordinates, rejects concurrent invalid writes and resumes after repair"
  ) {
    MongoAccessEvaluationSupport.resource.use { fixture =>
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
