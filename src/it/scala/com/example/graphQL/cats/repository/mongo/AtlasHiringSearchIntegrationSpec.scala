package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Explicit live gate. An absent URI is a skipped external gate, never Atlas acceptance. */
final class AtlasHiringSearchIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val model = "synthetic-hiring-evaluation"
  private val vector = List.tabulate(32)(index => if (index == 0) 1.0f else 0.01f)
  private val now = Instant.parse("2026-10-05T00:00:00Z")
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val page =
    PageSize.fromInt(7).toEither.fold(errors => throw new IllegalArgumentException(errors.toString), identity)
  private val filters = CandidateMatchFilters(List("scala"), Some("canada"), Some("toronto"), Some("AVAILABLE_NOW"))

  private def database(uri: String): Resource[IO, MongoDatabase[IO]] =
    MongoDatabaseProbe.clientResource(uri).flatMap { client =>
      Resource.make(client.getDatabase(s"search_evaluation_hiring_${UUID.randomUUID().toString.replace("-", "")}"))(
        db => MongoAccessEvaluationSupport.command(db, new org.bson.Document("dropDatabase", 1)).void
      )
    }

  private def repository(db: MongoDatabase[IO], strategy: SearchFusionStrategy, rerank: Boolean = false) =
    new MongoSemanticSearchRepository(
      db,
      indexes.jobVectorIndex,
      indexes.candidateVectorIndex,
      indexes.jobLexicalIndex,
      indexes.candidateLexicalIndex,
      100,
      100,
      strategy,
      rerank,
      diagnostics = Diagnostics.noop
    )

  private def fixture(db: MongoDatabase[IO]): IO[(Job, Set[UserId])] = {
    val recruiter = UserId(UUID.nameUUIDFromBytes("recruiter".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
    val job = Job(
      JobId(UUID.randomUUID()),
      recruiter,
      "Scala developer",
      "Scala hiring evaluation",
      List("scala"),
      Set("scala"),
      Location("Canada", "Toronto", false),
      JobStatus.Open,
      now,
      now
    )
    val embeddedJob = job.copy(embedding =
      Some(EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(job)), now)))
    )
    val cases =
      List("true-match", "false", "absent", "true-mismatch", "true-missing", "inactive", "wrong-model", "wrong-skills")
    val users = cases.map { name =>
      val profile = CandidateProfile(
        if (name == "wrong-skills") Set("java") else Set("scala"),
        Some("Scala hiring evaluation"),
        None,
        if (name == "true-missing") None
        else Some(CandidateResidence(if (name == "true-match") "Canada" else "Elsewhere", Some("Toronto"))),
        Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
        name.startsWith("true")
      )
      User(
        UserId(UUID.randomUUID()),
        None,
        name,
        UserRole.Candidate,
        Some(UserProfile.Candidate(profile)),
        now,
        embedding = Some(
          EntityEmbedding(
            vector,
            EmbeddingMeta(
              if (name == "wrong-model") "other" else model,
              SourceHash.sha256(SearchableText.candidate(profile)),
              now
            )
          )
        )
      )
    }
    val accepted = users.filter(user => Set("true-match", "false", "absent").contains(user.name)).map(_.id).toSet
    val owner = User(
      recruiter,
      None,
      "Synthetic recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
      now
    )
    MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(owner)) *>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(embeddedJob)) *>
      users
        .traverse_ { user =>
          val document = MongoHiringCodecs.user(user)
          if (user.name == "absent") {
            val _ = document.get("profile", classOf[org.bson.Document]).remove("recruiterSearchOptIn")
          }
          if (user.name == "inactive") {
            val _ = document.put("accountStatus", "Deleted")
            val _ = document.remove("profile")
            val _ = document.put("deletedAt", java.util.Date.from(now))
          }
          MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, document)
        }
        .as((embeddedJob, accepted))
  }

  private def query(job: Job, lexical: Boolean) = VectorSearchQuery(
    job.embedding.fold(vector)(_.values),
    Option.when(lexical)("scala"),
    JobSearchFilter(None, Set.empty, None),
    page,
    if (lexical) SearchMode.HYBRID else SearchMode.VECTOR,
    model,
    UUID.randomUUID(),
    Option.when(lexical)(vector),
    ValidatedCandidateMatchFilters.from(filters).fold(errors => fail(errors.toString), identity)
  )

  private def live(body: MongoDatabase[IO] => IO[Unit]): IO[Unit] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    database(sys.env("ATLAS_TEST_URI")).use(body)
  }

  private def observeClosureLag(repo: MongoSemanticSearchRepository, job: Job): IO[Unit] = {
    def poll(remaining: Int, observed: Boolean): IO[(Boolean, Boolean)] =
      repo.searchJobs(query(job, false)).value.flatMap {
        case Left(_) => IO.raiseError(new AssertionError("Atlas lag observation retrieval unavailable"))
        case Right(hits) if !hits.exists(_.job.id == job.id) => IO.pure((observed, true))
        case Right(_) if remaining > 0                       => IO.sleep(500.millis) *> poll(remaining - 1, true)
        case Right(_)                                        => IO.pure((true, false))
      }
    for {
      started <- IO.monotonic
      result <- poll(60, false)
      ended <- IO.monotonic
      _ <- IO.blocking {
        val directory = java.nio.file.Path.of(".local/data/search-evaluation")
        val _ = java.nio.file.Files.createDirectories(directory)
        val record = io.circe.Json.obj(
          "scenario" -> io.circe.Json.fromString("Closed job after retrieval; separate from authoritative validation"),
          "staleIndexedHitObserved" -> io.circe.Json.fromBoolean(result._1),
          "indexRemovalObserved" -> io.circe.Json.fromBoolean(result._2),
          "elapsedMillis" -> io.circe.Json.fromLong((ended - started).toMillis),
          "acceptance" -> io.circe.Json.fromString(
            if (result._1 && result._2) "Observed bounded lag scenario"
            else "Open: lag not observed or removal exceeded observation window"
          )
        )
        val _ = java.nio.file.Files.writeString(directory.resolve(s"closure-lag-${job.id.value}.json"), record.spaces2)
      }
    } yield ()
  }

  private def awaitIngestion(db: MongoDatabase[IO], job: Job, expected: Set[UserId]): IO[Unit] = {
    val repo = repository(db, SearchFusionStrategy.ApplicationRrf)
    def poll(remaining: Int): IO[Unit] =
      (repo.candidateMatches(query(job, false)).value, repo.searchJobs(query(job, false)).value).tupled.flatMap {
        case (Right(candidates), Right(jobs))
            if candidates.map(_.candidate.id).toSet == expected && jobs.map(_.job.id) == List(job.id) =>
          IO.unit
        case _ if remaining > 0 => IO.sleep(500.millis) *> poll(remaining - 1)
        case _                  =>
          IO.raiseError(new AssertionError("Atlas initial fixture ingestion did not complete within the bounded wait"))
      }
    poll(120)
  }

  test("production vector and application RRF branches enforce the consent truth table and current eligibility") {
    live { db =>
      for {
        seeded <- fixture(db)
        (job, expected) = seeded
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop)
        repo = repository(db, SearchFusionStrategy.ApplicationRrf)
        // Poll only initial index ingestion. Mutation checks below do not wait for Atlas indexing.
        _ <- awaitIngestion(db, seeded._1, seeded._2)
        _ <- List(false, true).traverse_ { lexical =>
          def await(attempts: Int): IO[Unit] = repo.candidateMatches(query(job, lexical)).value.flatMap { result =>
            result match {
              case Right(hits) if hits.map(_.candidate.id).toSet == expected =>
                repo.candidateEligibility(hits.map(_.candidate.id)).value.map { current =>
                  assertEquals(current.map(_.map(_.id).toSet), Right(expected))
                  assert(current.toOption.toList.flatten.forall(value => value.profile.forall(_.resumeRef.isEmpty)))
                }
              case _ if attempts > 0 => IO.sleep(500.millis) *> await(attempts - 1)
              case _                 => IO(assertEquals(result.map(_.map(_.candidate.id).toSet), Right(expected)))
            }
          }
          await(120)
        }
        service = new SemanticSearchService(
          new MongoUserRepository(
            db,
            MongoRepositoryTestSupport.noTransaction,
            MongoEmbeddingWorkEnqueuer.disabled,
            Diagnostics.noop
          ),
          new MongoJobRepository(
            db,
            MongoRepositoryTestSupport.noTransaction,
            MongoEmbeddingWorkEnqueuer.disabled,
            Diagnostics.noop
          ),
          new EmbeddingService {
            def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
              IO.raiseError(new IllegalStateException("Synthetic vector gate must not call an embedding provider"))
          },
          repo,
          model
        )
        validated <- service
          .candidateMatches(
            ActorContext(job.recruiterId, UserRole.Recruiter),
            job.id,
            None,
            filters,
            page,
            UUID.randomUUID()
          )
          .value
        _ = assertEquals(validated.map(_.map(_.candidate.id).toSet), Right(expected))
        hits <- repo.searchJobs(query(job, false)).value
        _ = assertEquals(hits.map(_.map(_.job.id)), Right(List(job.id)))
        _ <- MongoAccessEvaluationSupport.command(
          db,
          new org.bson.Document("update", MongoCollections.Jobs).append(
            "updates",
            java.util.List.of(
              new org.bson.Document("q", new org.bson.Document("_id", job.id.value.toString))
                .append(
                  "u",
                  new org.bson.Document(
                    "$set",
                    new org.bson.Document("status", "Closed").append("closedAt", java.util.Date.from(now))
                  )
                )
            )
          )
        )
        current <- repo.jobEligibility(List(job.id)).value
        _ = assertEquals(current.map(_.map(_.job.id)), Right(List(job.id)))
        _ = assert(
          current.toOption.toList.flatten.forall(value =>
            !SearchEligibilityPolicy.job(
              value,
              job.embedding.fold(throw new IllegalStateException("fixture embedding missing"))(_.meta),
              model,
              query(job, false).filter
            )
          )
        )
        _ <- observeClosureLag(repo, job)
      } yield ()
    }
  }

  List(SearchFusionStrategy.MongoRankFusion, SearchFusionStrategy.MongoScoreFusion).foreach { strategy =>
    test(s"explicit production $strategy capability gate") {
      live { db =>
        for {
          seeded <- fixture(db)
          _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop)
          _ <- awaitIngestion(db, seeded._1, seeded._2)
          result <- repository(db, strategy).candidateMatches(query(seeded._1, true)).value
          _ = assert(result.isRight, s"BLOCKED: $strategy unavailable on this deployment")
          _ = assertEquals(result.map(_.map(_.candidate.id).toSet), Right(seeded._2))
        } yield ()
      }
    }
  }
  test("explicit production native rerank capability gate") {
    live { db =>
      for {
        seeded <- fixture(db)
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop)
        _ <- awaitIngestion(db, seeded._1, seeded._2)
        result <- repository(db, SearchFusionStrategy.MongoRankFusion, rerank = true)
          .candidateMatches(query(seeded._1, true))
          .value
        _ = assert(result.isRight, "BLOCKED: native reranking unavailable on this deployment")
        _ = assertEquals(result.map(_.map(_.candidate.id).toSet), Right(seeded._2))
      } yield ()
    }
  }

}
