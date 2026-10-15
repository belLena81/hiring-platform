package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, SearchError, UseCaseError}
import com.example.graphQL.cats.service.job.JobService
import com.example.graphQL.cats.service.search.*
import com.example.hiring.testing.DisposableAtlas
import io.circe.Json
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Atlas Local gate for lexical-query facets (HDQ-12.L). An absent URI is a skipped external gate, never acceptance.
  * Environment label for every record: `local-container`.
  */
final class AtlasLexicalQueryFacetsIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val now = Instant.parse("2026-10-05T00:00:00Z")
  private val environment = "local-container"
  private val nicosia = GeoPoint(35.1856d, 33.3823d)
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val noFilter = JobSearchFilter(None, Set.empty, None)
  private val recruiterId = UserId(
    UUID.nameUUIDFromBytes("hdq12-recruiter".getBytes(java.nio.charset.StandardCharsets.UTF_8))
  )
  private val candidate = User(
    UserId(UUID.nameUUIDFromBytes("hdq12-candidate".getBytes(java.nio.charset.StandardCharsets.UTF_8))),
    None,
    "Facet candidate",
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("scala"), None, None))),
    now
  )
  private val recruiter = User(
    recruiterId,
    None,
    "Facet recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val actor = ActorContext(candidate.id, candidate.role)

  private def database(uri: String): Resource[IO, MongoDatabase[IO]] =
    MongoDatabaseProbe.clientResource(uri).flatMap { client =>
      Resource.make(client.getDatabase(s"hdq12_facets_${UUID.randomUUID().toString.replace("-", "")}"))(db =>
        MongoAccessEvaluationSupport.command(db, new org.bson.Document("dropDatabase", 1)).void
      )
    }

  private def live(maxTime: FiniteDuration = 5.seconds)(
      body: (MongoDatabase[IO], DiscoveryQueryPolicy) => IO[Unit]
  ): IO[Unit] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_)))
      .flatMap(database(_).use(db => DiscoveryQueryPolicy.create(maxTime, 4).flatMap(policy => body(db, policy))))
  }

  private def jobId(n: Int): JobId = JobId(new UUID(7L, n.toLong))

  /** Open jobs: titles carry "scala" for `scala` ones; every third matching job is remote; Nicosia ones carry a point.
    */
  private def job(
      n: Int,
      title: String,
      skills: Set[String],
      city: String,
      remote: Boolean,
      status: JobStatus = JobStatus.Open
  ): Job =
    Job(
      jobId(n),
      recruiterId,
      title,
      s"Synthetic hiring description $n",
      List("requirement"),
      skills,
      Location("Cyprus", city, remote, Option.when(city == "Nicosia" && !remote)(nicosia)),
      status,
      now,
      now,
      closedAt = Option.when(status == JobStatus.Closed)(now)
    )

  private def corpus(scalaJobs: Int, otherJobs: Int): List[Job] =
    (1 to scalaJobs).toList.map(n =>
      job(
        n,
        s"Scala engineer $n",
        Set("scala", s"extra${n % 4}"),
        if (n % 2 == 0) "Limassol" else "Nicosia",
        n % 3 == 0
      )
    ) ++ (1 to otherJobs).toList.map(n => job(10000 + n, s"Java engineer $n", Set("java"), "Nicosia", remote = false))

  private def seed(db: MongoDatabase[IO], jobs: List[Job]): IO[Unit] =
    List(candidate, recruiter).traverse_(user =>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(user))
    ) *> jobs.traverse_(value =>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(value))
    )

  private def matcher(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy) =
    new MongoLexicalJobMatcher(db, indexes.jobLexicalIndex, Diagnostics.noop[IO], policy)

  private def service(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy, limit: Int): JobService[IO] = {
    val jobs = new MongoJobRepository(
      db,
      MongoRepositoryTestSupport.noTransaction,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO],
      Some(policy)
    )
    val users = new MongoUserRepository(
      db,
      MongoRepositoryTestSupport.noTransaction,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )
    new JobService(
      users,
      jobs,
      com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher.noop,
      com.example.graphQL.cats.service.mutation.Idempotent(
        new MongoMutationReceiptRepository(db, MongoRepositoryTestSupport.noTransaction, Diagnostics.noop[IO])
      ),
      Diagnostics.noop[IO],
      cats.effect.Clock[IO],
      cats.effect.std.UUIDGen[IO],
      lexicalMatcher = Some(matcher(db, policy)),
      lexicalFacetMatchLimit = LexicalMatchLimit.from(limit).get,
      lexicalFacetDeadline = Some(policy.maxTime)
    )
  }

  /** Polls the real index until it reports exactly `expected` matches for "scala"; index ingestion is asynchronous. */
  private def awaitIndexed(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy, expected: Int): IO[Unit] = {
    def poll(remaining: Int): IO[Unit] =
      matcher(db, policy).matches("scala", noFilter, 5000).value.flatMap {
        case Right(found) if found.ids.size == expected => IO.unit
        case _ if remaining > 0                         => IO.sleep(500.millis) *> poll(remaining - 1)
        case other                                      =>
          IO.raiseError(new AssertionError(s"Atlas Local index did not reach $expected matches: $other"))
      }
    poll(240)
  }

  private def initialized(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy, jobs: List[Job]): IO[Unit] =
    MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO]) *> seed(db, jobs) *>
      awaitIndexed(db, policy, jobs.count(_.title.startsWith("Scala")))

  private def facets(svc: JobService[IO], query: JobFacetQuery) =
    svc.jobDiscoveryFacets(actor, query).value

  private def writeRecord(name: String, record: Json): IO[Unit] = IO.blocking {
    val directory = java.nio.file.Path.of(".local/data/search-evaluation")
    val _ = java.nio.file.Files.createDirectories(directory)
    val labelled = record.mapObject(_.add("environment", Json.fromString(environment)))
    val _ = java.nio.file.Files.writeString(directory.resolve(s"$name.json"), labelled.spaces2)
  }

  test("HDQ-12.L HDQ-12.1 HDQ-12.2 facets for a lexical query equal the counts of the matched Open jobs") {
    live() { (db, policy) =>
      val jobs = corpus(scalaJobs = 12, otherJobs = 3)
      val matching = jobs.filter(_.title.startsWith("Scala"))
      for {
        _ <- initialized(db, policy, jobs)
        svc = service(db, policy, LexicalMatchLimit.Default.value)
        typical <- facets(svc, JobFacetQuery(noFilter, None, Some("scala")))
        conjunction <- facets(
          svc,
          JobFacetQuery(JobSearchFilter(Some("Nicosia"), Set("scala"), Some(now)), None, Some("scala"))
        )
        radius <- facets(svc, JobFacetQuery(noFilter, Some(NearbyRadius(nicosia, 20d)), Some("scala")))
        structured <- facets(svc, JobFacetQuery(noFilter, None))
      } yield {
        assertEquals(typical, Right(JobDiscoveryFacets.fromJobs(matching)))
        assertEquals(
          conjunction,
          Right(JobDiscoveryFacets.fromJobs(matching.filter(_.location.city == "Nicosia")))
        )
        assertEquals(
          radius,
          Right(
            JobDiscoveryFacets.fromJobs(
              matching.filter(job => !job.location.remote && job.location.coordinates.nonEmpty)
            )
          )
        )
        // No-query behaviour is unchanged: it counts all Open jobs, including those the text did not match.
        assertEquals(structured.map(_.coverage), Right(FacetCoverage.AllMatches))
        assertEquals(structured.map(_.remote.map(_.count).sum), Right(jobs.size.toLong))
      }
    }
  }

  test("HDQ-12.L HDQ-12.3 the cap is configuration: N-1, N and N+1 matches are labelled ALL_MATCHES or TOP_MATCHES") {
    live() { (db, policy) =>
      val jobs = corpus(scalaJobs = 12, otherJobs = 0)
      def total(result: Either[UseCaseError, JobDiscoveryFacets]): Option[Long] =
        result.toOption.map(_.remote.map(_.count).sum)
      for {
        _ <- initialized(db, policy, jobs)
        below <- facets(service(db, policy, 13), JobFacetQuery(noFilter, None, Some("scala")))
        exact <- facets(service(db, policy, 12), JobFacetQuery(noFilter, None, Some("scala")))
        above <- facets(service(db, policy, 11), JobFacetQuery(noFilter, None, Some("scala")))
        small <- facets(service(db, policy, 5), JobFacetQuery(noFilter, None, Some("scala")))
      } yield {
        assertEquals(below.map(_.coverage), Right(FacetCoverage.AllMatches))
        assertEquals(total(below), Some(12L))
        assertEquals(exact.map(_.coverage), Right(FacetCoverage.AllMatches))
        assertEquals(total(exact), Some(12L))
        assertEquals(above.map(_.coverage), Right(FacetCoverage.TopMatches(11)))
        assertEquals(total(above), Some(11L))
        assertEquals(small.map(_.coverage), Right(FacetCoverage.TopMatches(5)))
        assertEquals(total(small), Some(5L))
      }
    }
  }

  test("HDQ-12.L HDQ-12.7.L closed jobs vanish at once and index lag for a newly opened job is recorded") {
    live() { (db, policy) =>
      val jobs = corpus(scalaJobs = 6, otherJobs = 0)
      val fresh = job(500, "Scala engineer fresh", Set("scala"), "Nicosia", remote = false)
      for {
        _ <- initialized(db, policy, jobs)
        svc = service(db, policy, LexicalMatchLimit.Default.value)
        _ <- MongoAccessEvaluationSupport.command(
          db,
          new org.bson.Document("update", MongoCollections.Jobs).append(
            "updates",
            java.util.List.of(
              new org.bson.Document("q", new org.bson.Document("_id", jobs.head.id.value.toString))
                .append("u", new org.bson.Document("$set", new org.bson.Document("status", "Closed")))
            )
          )
        )
        afterClose <- facets(svc, JobFacetQuery(noFilter, None, Some("scala")))
        _ <- MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(fresh))
        started <- IO.monotonic
        immediate <- facets(svc, JobFacetQuery(noFilter, None, Some("scala")))
        counted = immediate.toOption.exists(_.remote.map(_.count).sum == 6L)
        // Bounded observation of the lag: the new job should appear once mongot indexes it.
        appeared <- (0 until 60).toList.foldLeftM(counted) { (seen, _) =>
          if (seen) IO.pure(true)
          else
            IO.sleep(500.millis) *> facets(svc, JobFacetQuery(noFilter, None, Some("scala")))
              .map(_.toOption.exists(_.remote.map(_.count).sum == 6L))
        }
        ended <- IO.monotonic
        _ <- writeRecord(
          "lexical-facets-index-lag",
          Json.obj(
            "criterion" -> Json.fromString("HDQ-12.7.L"),
            "newlyOpenedJobCountedImmediately" -> Json.fromBoolean(counted),
            "newlyOpenedJobEventuallyCounted" -> Json.fromBoolean(appeared),
            "lagObserved" -> Json.fromBoolean(!counted),
            "elapsedMillis" -> Json.fromLong((ended - started).toMillis)
          )
        )
      } yield {
        // The closed job is excluded immediately although the index may still list it (authoritative recheck).
        assertEquals(afterClose.map(_.remote.map(_.count).sum), Right(5L))
        assert(appeared, "newly opened job was never indexed within the bounded observation window")
      }
    }
  }

  test("HDQ-12.L HDQ-12.8 an expired deadline gives sanitized FACETS_UNAVAILABLE with no structured fallback") {
    live(maxTime = 1.millis) { (db, policy) =>
      val jobs = corpus(scalaJobs = 4, otherJobs = 0)
      DiscoveryQueryPolicy.create(5.seconds, 4).flatMap { setupPolicy =>
        initialized(db, setupPolicy, jobs) *> {
          val svc = service(db, policy, LexicalMatchLimit.Default.value)
          val attempts = (0 until 20).toList.traverse(_ => facets(svc, JobFacetQuery(noFilter, None, Some("scala"))))
          attempts.flatMap { results =>
            val unavailable = results.collect { case Left(error) => error }
            writeRecord(
              "lexical-facets-timeout",
              Json.obj(
                "criterion" -> Json.fromString("HDQ-12.8"),
                "maxTimeMillis" -> Json.fromLong(1L),
                "attempts" -> Json.fromInt(results.size),
                "unavailable" -> Json.fromInt(unavailable.size),
                "error" -> Json.fromString(unavailable.headOption.fold("none")(_.toString))
              )
            ) *> IO {
              assert(unavailable.nonEmpty, "BLOCKED: a 1 ms deadline never expired on this deployment")
              assert(unavailable.forall(_ == UseCaseError.Search(SearchError.FacetsUnavailable)), unavailable.toString)
            }
          }
        }
      }
    }
  }

  private def firstValue(json: Json, key: String): Option[Json] =
    json.asObject
      .flatMap(_.apply(key))
      .orElse(json.asObject.flatMap(_.values.toList.collectFirstSome(firstValue(_, key))))
      .orElse(json.asArray.flatMap(_.toList.collectFirstSome(firstValue(_, key))))

  private def stagesOf(json: Json): List[String] =
    json.asObject.toList.flatMap(object_ =>
      object_("stage").flatMap(_.asString).toList ++ object_.values.toList.flatMap(stagesOf)
    ) ++ json.asArray.toList.flatMap(_.toList.flatMap(stagesOf))

  private def indexNamesOf(json: Json): List[String] =
    json.asObject.toList.flatMap(object_ =>
      object_("indexName").flatMap(_.asString).toList ++ object_.values.toList.flatMap(indexNamesOf)
    ) ++ json.asArray.toList.flatMap(_.toList.flatMap(indexNamesOf))

  test("HDQ-12.L D5 step 1 and step 2 plan evidence for the lexical-query facet pipeline") {
    live() { (db, policy) =>
      val jobs = corpus(scalaJobs = 1100, otherJobs = 100)
      val matching = jobs.filter(_.title.startsWith("Scala"))
      def explain(name: String, query: JobFacetQuery, ids: Set[JobId]): IO[(Int, Option[Long], Boolean)] = {
        val inner = MongoJobRepository.facetPipeline(query, Some(ids))
        val command = new org.bson.Document(
          "explain",
          new org.bson.Document("aggregate", MongoCollections.Jobs)
            .append("pipeline", inner.asJava)
            .append("cursor", new org.bson.Document())
        ).append("verbosity", "executionStats")
        MongoAccessEvaluationSupport.command(db, command).flatMap { result =>
          val json = io.circe.parser.parse(result.toJson).fold(throw _, identity)
          val docs = firstValue(json, "totalDocsExamined").flatMap(_.asNumber).flatMap(_.toLong)
          val keys = firstValue(json, "totalKeysExamined").flatMap(_.asNumber).flatMap(_.toLong)
          val returned = firstValue(json, "nReturned").flatMap(_.asNumber).flatMap(_.toLong)
          val millis = firstValue(json, "executionTimeMillis").flatMap(_.asNumber).flatMap(_.toLong)
          writeRecord(
            s"lexical-facets-explain-$name",
            Json.obj(
              "criterion" -> Json.fromString("HDQ-12.L D5"),
              "scenario" -> Json.fromString(name),
              "idCount" -> Json.fromInt(ids.size),
              "corpusJobs" -> Json.fromInt(jobs.size),
              "openJobs" -> Json.fromInt(jobs.count(_.status == JobStatus.Open)),
              "stages" -> Json.fromValues(stagesOf(json).distinct.map(Json.fromString)),
              "indexNames" -> Json.fromValues(indexNamesOf(json).distinct.map(Json.fromString)),
              "totalKeysExamined" -> keys.fold(Json.Null)(Json.fromLong),
              "totalDocsExamined" -> docs.fold(Json.Null)(Json.fromLong),
              "nReturned" -> returned.fold(Json.Null)(Json.fromLong),
              "executionTimeMillis" -> millis.fold(Json.Null)(Json.fromLong),
              "explain" -> json
            )
          ).as((ids.size, docs, indexNamesOf(json).exists(name => name != "_id_" && name.nonEmpty)))
        }
      }
      for {
        _ <- initialized(db, policy, jobs)
        step1Started <- IO.monotonic
        capped <- matcher(db, policy).matches("scala", noFilter, 1000).value
        step1Elapsed <- IO.monotonic.map(_ - step1Started)
        wide = capped.toOption.map(_.ids).getOrElse(fail(s"step 1 unavailable: $capped"))
        small = wide.take(5)
        radius = Some(NearbyRadius(nicosia, NearbyJobsQuery.MaxRadiusKm))
        nonRadiusWide <- explain("non-radius-N", JobFacetQuery(noFilter, None, Some("scala")), wide)
        nonRadiusSmall <- explain("non-radius-small", JobFacetQuery(noFilter, None, Some("scala")), small)
        radiusWide <- explain("radius-N", JobFacetQuery(noFilter, radius, Some("scala")), wide)
        radiusSmall <- explain("radius-small", JobFacetQuery(noFilter, radius, Some("scala")), small)
        combinedStarted <- IO.monotonic
        combined <- facets(service(db, policy, 1000), JobFacetQuery(noFilter, None, Some("scala")))
        combinedElapsed <- IO.monotonic.map(_ - combinedStarted)
        _ <- writeRecord(
          "lexical-facets-step1-and-combined",
          Json.obj(
            "criterion" -> Json.fromString("HDQ-12.L D5"),
            "step1ReturnedIds" -> Json.fromInt(wide.size),
            "step1Coverage" -> Json.fromString(capped.toOption.fold("unavailable")(_.coverage.toString)),
            "step1ElapsedMillis" -> Json.fromLong(step1Elapsed.toMillis),
            "combinedElapsedMillis" -> Json.fromLong(combinedElapsed.toMillis),
            "maxTimeMillis" -> Json.fromLong(5000L),
            "corpusMatching" -> Json.fromInt(matching.size)
          )
        )
      } yield {
        assertEquals(capped.map(_.coverage), Right(FacetCoverage.TopMatches(1000)))
        assertEquals(wide.size, 1000)
        assert(combined.isRight)
        // Acceptance: the non-radius plan examines no more documents than IDs and does not drive from other indexes.
        List(nonRadiusWide, nonRadiusSmall).foreach { case (ids, docs, usesOtherIndex) =>
          assert(docs.forall(_ <= ids.toLong), s"examined $docs documents for $ids IDs")
          assert(!usesOtherIndex, "planner chose an index other than _id_ for the ID-restricted match")
        }
        // Radius examination is recorded in the JSON evidence; it is not asserted against N (D4).
        assert(radiusWide._2.nonEmpty && radiusSmall._2.nonEmpty)
      }
    }
  }
}
