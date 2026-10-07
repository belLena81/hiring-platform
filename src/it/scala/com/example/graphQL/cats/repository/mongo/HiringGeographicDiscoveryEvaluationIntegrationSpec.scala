package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.search.*
import fs2.Stream
import io.circe.Json
import org.bson.Document
import com.mongodb.client.model.{Filters, Updates}
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Bounded service/driver workload. Results are measurements, never an SLO assertion. */
final class HiringGeographicDiscoveryEvaluationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val support = MongoAccessEvaluationSupport
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val center = GeoPoint(35.1856d, 33.3823d)
  private val candidates = (0 until 32).toList.map(index =>
    User(
      UserId(support.deterministicId(s"discovery-candidate:$index")),
      None,
      s"Synthetic candidate $index",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now
    )
  )
  private val recruiter = User(
    UserId(support.deterministicId("discovery-recruiter")),
    None,
    "Synthetic recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val jobs = (0 until 128).toList.map(index =>
    Job(
      JobId(support.deterministicId(s"discovery-job:$index")),
      UserId(support.deterministicId("discovery-recruiter")),
      s"Synthetic job $index",
      "Build hiring systems",
      List("Scala"),
      Set("Scala", s"Skill${index % 24}"),
      Location(
        "Cyprus",
        if (index % 2 == 0) "Nicosia" else "Limassol",
        remote = index % 11 == 0,
        coordinates = Option.when(index % 13 != 0)(GeoPoint(center.latitude + index * 0.0001d, center.longitude))
      ),
      if (index % 7 == 0) JobStatus.Closed else JobStatus.Open,
      now.minusSeconds(index.toLong),
      now
    )
  )
  private val queries = (0 until 20).toList.map(index =>
    NearbyJobsQuery(
      center,
      1d + index,
      JobSearchFilter(
        Option.when(index % 3 == 0)("Nicosia"),
        Option.when(index % 4 == 0)(Set("Scala")).getOrElse(Set.empty),
        None
      )
    )
  )
  private def percentile(values: List[Double], fraction: Double): Double =
    values.sorted.apply(math.ceil(values.size * fraction).toInt.max(1) - 1)

  test("measure 128 jobs and 32 authenticated candidates over 20 fixed geographic queries at concurrency 1 and 8") {
    mongoResource
      .evalMap(fixture => DiscoveryQueryPolicy.create(2.seconds, 4).map(policy => (fixture, policy)))
      .use { case (fixture, policy) =>
        val repository = MongoJobRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop,
          Some(policy)
        )
        val users = MongoUserRepository.transactional(
          fixture.database,
          fixture.client,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop
        )
        val receipts = MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        val service = JobService.live(
          users,
          repository,
          new EmbeddingWorkPublisher { def wake: IO[Unit] = IO.unit },
          Idempotent(receipts),
          Diagnostics.noop
        )
        def storage = for {
          admin <- fixture.client.getDatabase("admin")
          _ <- support.command(admin, new Document("fsync", 1))
          value <- support.command(fixture.database, new Document("collStats", MongoCollections.Jobs))
        } yield Json.obj(
          "storageBytes" -> Json.fromLong(value.get("storageSize", classOf[Number]).longValue()),
          "indexBytes" -> Json.fromLong(value.get("totalIndexSize", classOf[Number]).longValue()),
          "indexSizes" -> io.circe.parser.parse(value.get("indexSizes", classOf[Document]).toJson).getOrElse(Json.Null),
          "sampleBoundary" -> Json.fromString("After fsync flush on the disposable MongoDB")
        )
        for {
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
          mongoBuild <- support.command(fixture.database, new Document("buildInfo", 1))
          before <- storage
          _ <- MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.Users,
            MongoHiringCodecs.user(recruiter)
          )
          _ <- candidates.traverse_(value =>
            MongoRepositoryTestSupport
              .insertOne(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(value))
          )
          _ <- jobs.traverse_(value =>
            MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.Jobs, MongoHiringCodecs.job(value))
          )
          after <- storage
          scope <- IO.fromEither(
            com.example.graphQL.cats.service.read.HiringReadScope
              .validated(
                ActorContext(candidates.head.id, candidates.head.role),
                candidates.head,
                com.example.graphQL.cats.service.auth.ActorAuthorization(users)
              )
              .leftMap(error => new AssertionError(error.toString))
          )
          explain <- support.command(
            fixture.database,
            new Document(
              "explain",
              new Document("aggregate", MongoCollections.Users)
                .append(
                  "pipeline",
                  MongoJobRepository
                    .authorizedDiscoveryPipeline(
                      scope,
                      List(
                        new Document(
                          "$geoNear",
                          new Document(
                            "near",
                            new Document("type", "Point").append(
                              "coordinates",
                              List(center.longitude, center.latitude).asJava
                            )
                          )
                            .append("key", "location.point")
                            .append("distanceField", "_distanceKm")
                            .append("distanceMultiplier", 0.001d)
                            .append("spherical", true)
                            .append("maxDistance", 1000d)
                            .append(
                              "query",
                              MongoJobRepository.discoveryFilter(queries.head.filter).append("location.remote", false)
                            )
                        ),
                        new Document("$sort", new Document("_distanceKm", 1).append("_id", 1)),
                        new Document("$limit", 21),
                        new Document(
                          "$project",
                          MongoSearchEligibilityCodecs.projection(
                            MongoSearchEligibilityCodecs.jobFields
                              .filterNot(_ == MongoFields.EmbeddingMeta) :+ "_distanceKm"
                          )
                        )
                      )
                    )
                    .asJava
                )
                .append("cursor", new Document())
            )
              .append("verbosity", "executionStats")
          )
          denied <- service.nearbyJobs(ActorContext(recruiter.id, recruiter.role), queries.head, 20).value
          _ = assert(denied.isLeft)
          deniedFacets <- service
            .jobDiscoveryFacets(
              ActorContext(recruiter.id, recruiter.role),
              JobFacetQuery(queries.head.filter, None)
            )
            .value
          _ = assert(deniedFacets.isLeft)
          userCollection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
          _ <- userCollection.updateOne(
            Filters.eq("_id", candidates.head.id.value.toString),
            Updates.set("accountStatus", AccountStatus.Deleted.toString)
          )
          deletedDenied <- service
            .nearbyJobs(ActorContext(candidates.head.id, candidates.head.role), queries.head, 20)
            .value
          _ = assert(deletedDenied.isLeft)
          _ <- userCollection.updateOne(
            Filters.eq("_id", candidates.head.id.value.toString),
            Updates.set("accountStatus", AccountStatus.Active.toString)
          )
          rows <- List(1, 8).traverse { concurrency =>
            for {
              _ <- fixture.commands.clear
              resourcesBefore <- fixture.sampleResources
              started <- IO.monotonic
              samples <- Stream
                .emits(candidates.zipWithIndex.flatMap { case (candidate, index) =>
                  queries.zipWithIndex.map { case (query, queryIndex) => (candidate, index, query, queryIndex) }
                })
                .covary[IO]
                .parEvalMap(concurrency) { case (candidate, _, query, _) =>
                  for {
                    start <- IO.monotonic
                    result <- service.nearbyJobs(ActorContext(candidate.id, candidate.role), query, 20).value
                    end <- IO.monotonic
                  } yield ((end - start).toNanos.toDouble / 1000000d, result.isLeft)
                }
                .compile
                .toList
              finished <- IO.monotonic
              resourcesAfter <- fixture.sampleResources
              commands <- fixture.commands.snapshot
              _ = assertEquals(samples.count(_._2), 0)
              elapsed = (finished - started).toNanos.toDouble / 1000000000d
            } yield Json.obj(
              "concurrency" -> Json.fromInt(concurrency),
              "requests" -> Json.fromInt(samples.size),
              "errors" -> Json.fromInt(samples.count(_._2)),
              "requestsPerSecond" -> Json.fromDoubleOrNull(samples.size / elapsed),
              "p50Millis" -> Json.fromDoubleOrNull(percentile(samples.map(_._1), 0.5)),
              "p95Millis" -> Json.fromDoubleOrNull(percentile(samples.map(_._1), 0.95)),
              "p99Millis" -> Json.fromDoubleOrNull(percentile(samples.map(_._1), 0.99)),
              "mongoCommands" -> Json.fromInt(commands.size),
              "resourcesBefore" -> resourcesBefore,
              "resourcesAfter" -> resourcesAfter
            )
          }
          ended <- IO.realTimeInstant
          _ <- IO.blocking {
            val directory = Paths.get(".local/data/discovery-evaluation")
            val _ = Files.createDirectories(directory)
            val path = directory.resolve(s"geographic-${ended.toEpochMilli}.json")
            val _ = Files.writeString(
              path,
              Json
                .obj(
                  "jobs" -> Json.fromInt(128),
                  "queryBoundary" -> Json.fromString("Current Users actor gate with geoNear first inside jobs lookup"),
                  "candidates" -> Json.fromInt(32),
                  "fixedQueries" -> Json.fromInt(20),
                  "warmupRequests" -> Json.fromInt(0),
                  "samplesPerQuery" -> Json.fromInt(32),
                  "measurementBoundary" -> Json.fromString(
                    "Job service authorization through real Mongo driver; excludes HTTP and GraphQL"
                  ),
                  "mongoVersion" -> Json.fromString(mongoBuild.getString("version")),
                  "javaVersion" -> Json.fromString(System.getProperty("java.version")),
                  "processors" -> Json.fromInt(Runtime.getRuntime.availableProcessors()),
                  "explain" -> io.circe.parser.parse(explain.toJson).getOrElse(Json.Null),
                  "storageBefore" -> before,
                  "storageAfter" -> after,
                  "measurements" -> Json.fromValues(rows)
                )
                .spaces2
            )
            println(s"Geographic discovery evidence: $path")
          }
        } yield ()
      }
  }
}
