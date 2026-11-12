package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, SearchError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.hiring.testing.DisposableAtlas
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** GDF-03.L: jobs similar to a job's stored embedding, against the real `$vectorSearch` index on Atlas Local. The real
  * `SemanticSearchService` runs over the real `MongoSemanticSearchRepository`; the embedding provider throws if called,
  * so the test also shows that the root makes no provider call. An absent `ATLAS_TEST_URI` skips the suite, and a skip
  * is never acceptance: this suite is run by the owner on Atlas Local and has not been run by the author.
  */
final class AtlasSimilarJobsIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val now = Instant.parse("2026-11-12T12:00:00Z")
  private val model = "synthetic-hiring-evaluation"
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val bound = PageSize.fromInt(PageSize.Max).toOption.get

  /** Unit-length-ish directions: `similarTo` leans towards dimension 0, `orthogonal` towards dimension 1. */
  private val sourceVector = List.tabulate(32)(index => if (index == 0) 1.0f else 0.01f)
  private val nearVector = List.tabulate(32)(index => if (index == 0) 1.0f else if (index == 1) 0.2f else 0.01f)
  private val farVector = List.tabulate(32)(index => if (index == 1) 1.0f else 0.01f)

  private def userId(name: String) = UserId(UUID.nameUUIDFromBytes(s"gdf03-atlas-$name".getBytes("UTF-8")))
  private val owner = User(
    userId("owner"),
    None,
    "Similar jobs recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val viewer = User(
    userId("viewer"),
    None,
    "Similar jobs candidate",
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("scala"), None, None))),
    now
  )
  private def ctx(user: User) = ActorContext(user.id, user.role)

  private def jobAt(
      n: Int,
      jobVector: Option[List[Float]],
      status: JobStatus = JobStatus.Open,
      staleSource: Boolean = false
  ): Job = {
    val base = Job(
      JobId(new UUID(11L, n.toLong)),
      owner.id,
      s"Scala engineer $n",
      s"Synthetic similar jobs description $n",
      List("requirement"),
      Set("scala"),
      Location("Cyprus", Some("Nicosia"), false, None),
      JobState.requested(status, now),
      now,
      now
    )
    jobVector.fold(base) { values =>
      val source = if (staleSource) "a source the job no longer has" else SearchableText.job(base)
      base.copy(embedding = Some(EntityEmbedding(values, EmbeddingMeta(model, SourceHash.sha256(source), now))))
    }
  }

  private def withAtlas[A](body: (MongoDatabase[IO], DiscoveryQueryPolicy) => IO[A]): IO[A] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_))).flatMap { uri =>
      AtlasLocalSupport
        .client(uri, None)
        .flatMap(client => AtlasLocalSupport.nonceDatabase(client, AtlasLocalSupport.nonceName("similar_jobs_")))
        .use(db => DiscoveryQueryPolicy.create(5.seconds, 4).flatMap(policy => body(db, policy)))
    }
  }

  private def seed(db: MongoDatabase[IO], jobs: List[Job]): IO[Unit] =
    MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO]) *>
      List(owner, viewer).traverse_(user =>
        MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(user))
      ) *>
      jobs.traverse_(job => MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(job)))

  private def eventually[A](attempts: Int)(read: IO[Either[?, A]])(done: A => Boolean): IO[A] =
    read.flatMap {
      case Right(value) if done(value) => IO.pure(value)
      case _ if attempts > 0           => IO.sleep(500.millis) *> eventually(attempts - 1)(read)(done)
      case other => IO.raiseError(new AssertionError(s"Atlas index did not reach the expected state: $other"))
    }

  private def repositoryOf(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy) =
    new MongoSemanticSearchRepository(
      db,
      indexes.jobVectorIndex,
      indexes.candidateVectorIndex,
      indexes.jobLexicalIndex,
      indexes.candidateLexicalIndex,
      100,
      100,
      diagnostics = Diagnostics.noop[IO],
      discoveryPolicy = Some(policy)
    )

  private def serviceOf(db: MongoDatabase[IO], repo: SemanticSearchRepository[IO]) =
    new SemanticSearchService[IO](
      new MongoUserRepository(
        db,
        MongoRepositoryTestSupport.noTransaction,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      new MongoJobRepository(
        db,
        MongoRepositoryTestSupport.noTransaction,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      new EmbeddingService[IO] {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          IO.raiseError(new IllegalStateException("similarJobs must not call an embedding provider"))
      },
      repo,
      model
    )

  private def close(db: MongoDatabase[IO], job: Job): IO[Unit] =
    MongoRepositoryTestSupport.collection(db, MongoCollections.Jobs).flatMap { collection =>
      collection
        .updateOne(
          Filters.eq(MongoFields.Id, job.id.value.toString),
          Updates.combine(
            Updates.set(MongoFields.Status, "Closed"),
            Updates.set(MongoFields.ClosedAt, java.util.Date.from(now)),
            Updates.inc(MongoFields.Version, 1L)
          )
        )
        .void
    }

  test(
    "GDF-03.1.L similar jobs are nearest first, exclude the source, closed and stale-source jobs, without a provider"
  ) {
    withAtlas { (db, policy) =>
      val source = jobAt(1, Some(sourceVector))
      val near = jobAt(2, Some(nearVector))
      val far = jobAt(3, Some(farVector))
      val closedNeighbour = jobAt(4, Some(nearVector))
      val staleNeighbour = jobAt(5, Some(nearVector), staleSource = true)
      val fixtures = List(source, near, far, closedNeighbour, staleNeighbour)
      val repo = repositoryOf(db, policy)
      val probe = VectorSearchQuery(
        sourceVector,
        None,
        JobSearchFilter(None, Set.empty, None),
        bound,
        SearchMode.VECTOR,
        model,
        UUID.randomUUID()
      )
      for {
        _ <- seed(db, fixtures)
        _ <- eventually(240)(repo.similarJobs(probe).value)(hits =>
          Set(source.id, near.id, far.id, closedNeighbour.id).subsetOf(hits.map(_.id).toSet)
        )
        _ <- close(db, closedNeighbour)
        result <- serviceOf(db, repo).similarJobs(ctx(viewer), source.id, bound, UUID.randomUUID()).value
      } yield {
        val ids = result.fold(error => fail(s"similarJobs failed: $error"), _.map(_.job.id))
        assertEquals(ids, List(near.id, far.id))
      }
    }
  }

  test("GDF-03.2.L a closed source job and an unknown job give the same outcome, a missing embedding is unavailable") {
    withAtlas { (db, policy) =>
      val closedSource = jobAt(6, Some(sourceVector), status = JobStatus.Closed)
      val bare = jobAt(7, None)
      val service = serviceOf(db, repositoryOf(db, policy))
      for {
        _ <- seed(db, List(closedSource, bare))
        hidden <- service.similarJobs(ctx(viewer), closedSource.id, bound, UUID.randomUUID()).value
        unknown <- service.similarJobs(ctx(viewer), JobId(new UUID(11L, 999L)), bound, UUID.randomUUID()).value
        unembedded <- service.similarJobs(ctx(viewer), bare.id, bound, UUID.randomUUID()).value
      } yield {
        assertEquals(hidden, Left(UseCaseError.Domain(DomainError.NotFound("job"))))
        assertEquals(unknown, hidden)
        assertEquals(unembedded, Left(UseCaseError.Search(SearchError.MissingEmbedding("job"))))
      }
    }
  }
}
