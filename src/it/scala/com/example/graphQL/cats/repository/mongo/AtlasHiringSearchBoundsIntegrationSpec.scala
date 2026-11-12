package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.hiring.testing.DisposableAtlas
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Atlas Local gates for retrieval bounds, native fusion and authorization order (MVR-06.L, HSE-04.L, SEB-04.L). An
  * absent URI is a skipped external gate, never acceptance. Synthetic vectors; no embedding provider is called.
  */
final class AtlasHiringSearchBoundsIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val model = "synthetic-hiring-evaluation"
  private val vector = List.tabulate(32)(index => if (index == 0) 1.0f else 0.01f)
  private val now = Instant.parse("2026-10-05T00:00:00Z")
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val BranchLimit = 3
  private val torontoCandidates = 5
  private def pageOf(size: Int) =
    PageSize.fromInt(size).toEither.fold(errors => throw new IllegalArgumentException(errors.toString), identity)
  private val first = pageOf(2)
  private val broad = CandidateMatchFilters(List("scala"), Some("canada"), None, None)
  private val selective = CandidateMatchFilters(List("scala"), Some("canada"), Some("ottawa"), None)
  private val empty = CandidateMatchFilters(List("scala"), Some("canada"), Some("nowhere"), None)

  private def live(
      body: (MongoDatabase[IO], DiscoveryQueryPolicy, AtlasLocalSupport.RecordedCommands) => IO[Unit]
  ): IO[Unit] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    val commands = new AtlasLocalSupport.RecordedCommands
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_))).flatMap { uri =>
      AtlasLocalSupport
        .client(uri, Some(commands))
        .flatMap(client =>
          AtlasLocalSupport.nonceDatabase(client, AtlasLocalSupport.nonceName("search_evaluation_hiring_"))
        )
        .use(db => DiscoveryQueryPolicy.create(2.seconds, 4).flatMap(policy => body(db, policy, commands)))
    }
  }

  private def repository(
      db: MongoDatabase[IO],
      policy: DiscoveryQueryPolicy,
      strategy: SearchFusionStrategy = SearchFusionStrategy.ApplicationRrf,
      rerank: Boolean = false
  ) =
    new MongoSemanticSearchRepository(
      db,
      indexes.jobVectorIndex,
      indexes.candidateVectorIndex,
      indexes.jobLexicalIndex,
      indexes.candidateLexicalIndex,
      100,
      BranchLimit,
      strategy,
      rerank,
      diagnostics = Diagnostics.noop[IO],
      discoveryPolicy = Some(policy)
    )

  private def recruiter(name: String): User =
    User(
      UserId(UUID.randomUUID()),
      None,
      name,
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
      now
    )

  private def candidate(name: String, city: String): User = {
    val profile = CandidateProfile(
      Set("scala"),
      Some("Scala hiring evaluation"),
      None,
      Some(CandidateResidence("Canada", Some(city))),
      Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      true
    )
    User(
      UserId(UUID.randomUUID()),
      None,
      name,
      UserRole.Candidate,
      Some(UserProfile.Candidate(profile)),
      now,
      embedding =
        Some(EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(SearchableText.candidate(profile)), now)))
    )
  }

  private final case class Seeded(job: Job, owner: User, other: User, candidates: List[User])

  /** One Open job, its owner, a second recruiter, and more opted-in matching candidates than the branch limit. */
  private def seed(db: MongoDatabase[IO]): IO[Seeded] = {
    val owner = recruiter("Synthetic owner")
    val job = Job(
      JobId(UUID.randomUUID()),
      owner.id,
      "Scala developer",
      "Scala hiring evaluation",
      List("scala"),
      Set("scala"),
      Location("Canada", Some("Toronto"), false),
      JobState.Open,
      now,
      now
    )
    val embedded = job.copy(embedding =
      Some(EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(job)), now)))
    )
    val other = recruiter("Other recruiter")
    val candidates =
      (1 to torontoCandidates).toList.map(n => candidate(s"toronto-$n", "Toronto")) :+ candidate("ottawa", "Ottawa")
    (List(owner, other) ++ candidates).traverse_(user =>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(user))
    ) *> MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(embedded)) *>
      IO.pure(Seeded(embedded, owner, other, candidates))
  }

  private def candidateQuery(filters: CandidateMatchFilters) = VectorSearchQuery(
    vector,
    None,
    JobSearchFilter(None, Set.empty, None),
    first,
    SearchMode.VECTOR,
    model,
    UUID.randomUUID(),
    None,
    ValidatedCandidateMatchFilters.from(filters).fold(errors => fail(errors.toString), identity)
  )

  private def hybridJobQuery = VectorSearchQuery(
    vector,
    Some("scala"),
    JobSearchFilter(None, Set.empty, None),
    pageOf(7),
    SearchMode.HYBRID,
    model,
    UUID.randomUUID()
  )

  private def eventually[A](attempts: Int)(read: IO[Either[?, A]])(done: A => Boolean): IO[A] =
    read.flatMap {
      case Right(value) if done(value) => IO.pure(value)
      case _ if attempts > 0           => IO.sleep(500.millis) *> eventually(attempts - 1)(read)(done)
      case other => IO.raiseError(new AssertionError(s"Atlas index did not reach the expected state: $other"))
    }

  private final case class Counts(
      retrievals: Int = 0,
      retrieved: List[Int] = Nil,
      eligibilityCalls: Int = 0,
      eligibilityIds: List[Int] = Nil
  )

  /** Delegates to the production repository and counts retrieval rounds, retrieved hits and validation candidates. */
  private final class CountingRepository(inner: SemanticSearchRepository[IO], counts: Ref[IO, Counts])
      extends SemanticSearchRepository[IO] {
    private def eligibility(size: Int): RepositoryF[IO, Unit] =
      RepositoryF.lift(
        counts.update(value =>
          value.copy(eligibilityCalls = value.eligibilityCalls + 1, eligibilityIds = size :: value.eligibilityIds)
        )
      )
    def authorizedJobEligibility(
        scope: HiringReadScope,
        ids: List[JobId],
        queryCandidate: Option[CandidateSearchEligibility]
    ) =
      eligibility(ids.size).flatMap(_ => inner.authorizedJobEligibility(scope, ids, queryCandidate))
    def authorizedCandidateEligibility(scope: HiringReadScope, queryJob: JobSearchEligibility, ids: List[UserId]) =
      eligibility(ids.size).flatMap(_ => inner.authorizedCandidateEligibility(scope, queryJob, ids))
    def searchJobs(query: VectorSearchQuery) = inner.searchJobs(query)
    def similarJobs(query: VectorSearchQuery) = inner.similarJobs(query)
    def recommendedJobs(query: VectorSearchQuery) = inner.recommendedJobs(query)
    def candidateMatches(query: VectorSearchQuery) =
      inner
        .candidateMatches(query)
        .semiflatTap(hits =>
          counts
            .update(value => value.copy(retrievals = value.retrievals + 1, retrieved = hits.size :: value.retrieved))
        )
    def lexicalJobs(query: LexicalSearchQuery) = inner.lexicalJobs(query)
    def lexicalCandidates(query: LexicalSearchQuery) = inner.lexicalCandidates(query)
  }

  private def service(db: MongoDatabase[IO], search: SemanticSearchRepository[IO]) =
    new SemanticSearchService(
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
          IO.raiseError(new IllegalStateException("Synthetic vector gate must not call an embedding provider"))
      },
      search,
      model
    )

  test(
    "MVR-06.L validation candidates stay within the branch limit and results within first for broad, selective and empty searches"
  ) {
    live { (db, policy, commands) =>
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        seeded <- seed(db)
        plain = repository(db, policy)
        // Wait for initial ingestion only; the bounded searches below do not wait for any later index change.
        _ <- eventually(120)(plain.candidateMatches(candidateQuery(broad)).value)(_.size == BranchLimit)
        _ <- eventually(120)(plain.candidateMatches(candidateQuery(selective)).value)(_.size == 1)
        counts <- Ref.of[IO, Counts](Counts())
        subject = service(db, new CountingRepository(plain, counts))
        actor = ActorContext(seeded.owner.id, UserRole.Recruiter)
        // (name, filters, hits the branch retrieves, results the page returns)
        cases = List(("broad", broad, BranchLimit, first.value), ("selective", selective, 1, 1), ("empty", empty, 0, 0))
        dbName = db.underlying.getName
        findCounts <- cases.traverse { case (name, filters, retrieved, returned) =>
          for {
            _ <- counts.set(Counts())
            _ = commands.clear()
            result <- subject.candidateMatches(actor, seeded.job.id, None, filters, first, UUID.randomUUID()).value
            observed <- counts.get
          } yield {
            val hits = result.fold(error => fail(s"$name search failed: $error"), _.hits.size)
            assertEquals(hits, returned, clue(name))
            assert(hits <= first.value, clue(name))
            assertEquals(observed.retrievals, 1, clue(s"$name: one retrieval round, no refill"))
            assertEquals(observed.retrieved, List(retrieved), clue(name))
            assert(observed.retrieved.forall(_ <= BranchLimit), clue(name))
            assert(observed.eligibilityCalls <= 1, clue(s"$name: no per-hit fetch"))
            assert(observed.eligibilityIds.forall(_ <= BranchLimit), clue(s"$name: validation candidates"))
            assertEquals(
              observed.eligibilityIds.sum,
              retrieved,
              clue(s"$name: validation candidates equal retrieved hits")
            )
            // Driver view: exactly one retrieval-rooted aggregate on users, whatever the number of hits.
            assertEquals(commands.retrievals(dbName, MongoCollections.Users).size, 1, clue(s"$name: users retrievals"))
            name -> commands.findCount(dbName, MongoCollections.Users)
          }
        }
        // No per-hit fetch: the number of user `find` commands does not grow with the hits (the empty case is the floor).
        floor = findCounts.collectFirst { case ("empty", count) => count }.getOrElse(0)
        _ = findCounts.foreach { case (name, count) => assertEquals(count, floor, clue(s"$name: user finds")) }
      } yield ()
    }
  }

  test("HSE-02 SEB-04.L a recruiter who does not own the job and a candidate are denied before any retrieval") {
    live { (db, policy, _) =>
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        seeded <- seed(db)
        counts <- Ref.of[IO, Counts](Counts())
        subject = service(db, new CountingRepository(repository(db, policy), counts))
        actors = List(
          (
            "non-owner recruiter",
            ActorContext(seeded.other.id, UserRole.Recruiter),
            UseCaseError.Domain(DomainError.NotFound("job"))
          ),
          (
            "candidate",
            ActorContext(seeded.candidates.head.id, UserRole.Candidate),
            UseCaseError.Domain(DomainError.RecruiterRequired)
          )
        )
        _ <- actors.traverse_ { case (name, actor, expected) =>
          subject.candidateMatches(actor, seeded.job.id, None, broad, first, UUID.randomUUID()).value.map { result =>
            assertEquals(result.map(_ => ()), Left(expected), clue(s"$name must be denied"))
          }
        }
        observed <- counts.get
      } yield {
        assertEquals(observed.retrievals, 0)
        assertEquals(observed.eligibilityCalls, 0)
      }
    }
  }

  test("HSE-04.L default application RRF runs separate vector and lexical branches and sends no native fusion stage") {
    live { (db, policy, commands) =>
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        seeded <- seed(db)
        repo = repository(db, policy)
        _ <- eventually(120)(repo.searchJobs(hybridJobQuery).value)(_.exists(_.id == seeded.job.id))
        _ = commands.clear()
        hits <- repo.searchJobs(hybridJobQuery).value
        roots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
      } yield {
        assertEquals(hits.map(_.map(_.id)), Right(List(seeded.job.id)))
        assertEquals(roots.sorted, List("$search", "$vectorSearch"))
      }
    }
  }

  List(SearchFusionStrategy.MongoRankFusion, SearchFusionStrategy.MongoScoreFusion).foreach { strategy =>
    val stage = if (strategy == SearchFusionStrategy.MongoRankFusion) "$rankFusion" else "$scoreFusion"
    test(s"HSE-04.L native $strategy sends one aggregation and maps finite scores") {
      live { (db, policy, commands) =>
        for {
          _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
          seeded <- seed(db)
          repo = repository(db, policy, strategy)
          _ <- eventually(120)(repo.searchJobs(hybridJobQuery).value)(_.exists(_.id == seeded.job.id))
          _ = commands.clear()
          result <- repo.searchJobs(hybridJobQuery).value
          roots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
        } yield {
          // A failure here is BLOCKED: the deployment does not provide this stage.
          val hits = result.fold(error => fail(s"BLOCKED: $strategy unavailable on this deployment: $error"), identity)
          assertEquals(hits.map(_.id), List(seeded.job.id))
          assert(hits.forall(hit => hit.score.isFinite && hit.score > 0d), clue(hits.map(_.score)))
          assertEquals(roots, List(stage))
        }
      }
    }

    test(s"HSE-04.L native $strategy candidate search sends one aggregation and maps finite scores") {
      live { (db, policy, commands) =>
        val query = candidateQuery(broad).copy(lexicalQuery = Some("scala"), candidateQueryVector = Some(vector))
        for {
          _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
          seeded <- seed(db)
          _ <- eventually(120)(repository(db, policy).candidateMatches(candidateQuery(broad)).value)(
            _.size == BranchLimit
          )
          _ = commands.clear()
          result <- repository(db, policy, strategy).candidateMatches(query).value
          roots = commands.retrievals(db.underlying.getName, MongoCollections.Users).flatMap(_.stages.headOption)
        } yield {
          // A failure here is BLOCKED: the deployment does not provide this stage.
          val hits = result.fold(error => fail(s"BLOCKED: $strategy unavailable on this deployment: $error"), identity)
          assert(hits.nonEmpty && hits.size <= BranchLimit, clue(hits.size))
          assert(hits.forall(hit => seeded.candidates.exists(_.id == hit.id)), clue(hits.map(_.id)))
          assert(hits.forall(hit => hit.score.isFinite && hit.score > 0d), clue(hits.map(_.score)))
          assertEquals(roots, List(stage))
        }
      }
    }
  }

  test("HSE-04.L HDQ-04.L an unsupported native stage gives sanitized unavailability and does not switch strategy") {
    live { (db, policy, commands) =>
      assume(
        sys.env.get("ATLAS_TEST_URI").flatMap(DisposableAtlas.nativeRerankSkipReason).isDefined,
        "Unsupported-stage outcome is only forced on an explicit local-container deployment (no native $rerank)"
      )
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        seeded <- seed(db)
        _ <- eventually(120)(
          repository(db, policy).searchJobs(hybridJobQuery).value
        )(_.exists(_.id == seeded.job.id))
        _ = commands.clear()
        result <- repository(db, policy, SearchFusionStrategy.MongoRankFusion, rerank = true)
          .searchJobs(hybridJobQuery)
          .value
        roots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
      } yield {
        assert(result.isLeft, clue(result))
        assert(
          result.left.exists(error =>
            error == RepositoryError.Unavailable || error == RepositoryError.VectorSearchUnavailable
          ),
          clue(result)
        )
        // One native attempt; no application RRF branches ($search/$vectorSearch roots) were run as a substitute.
        assertEquals(roots, List("$rankFusion"))
      }
    }
  }
}
