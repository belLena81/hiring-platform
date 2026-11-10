package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.RankingBasis
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.{ActorContext, AuthenticationError, Diagnostics, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.service.search.SkillEvidenceRankedComposition.RecordingEvidenceRanker
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.hiring.testing.DisposableAtlas
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** Atlas Local gates for the Atlas parts of the skill-evidence ranker criteria (HDQ-07.8, HDQ-07.9). The real
  * `SemanticSearchService` runs over the real `MongoSemanticSearchRepository` and the Atlas Local indexes; the ranker
  * is the test-source composition with a recording ranker, so each test sees exactly what the ranker would receive.
  * Nothing in production calls the ranker. An absent `ATLAS_TEST_URI` skips the suite and is never acceptance.
  *
  * Candidate matching uses the lexical candidate branch (the vector branch is made unavailable, so the service answers
  * from the degraded lexical path). Recommendations are vector-only in the service and use the deterministic synthetic
  * vector fixture, so no embedding provider is called. Other seeded users (the shared viewer) are legitimate lexical
  * hits, so each test compares only the fixtures it creates (`fixtureIds`).
  *
  * Not covered here: the Admin clause of the criteria (no Admin is seeded on Atlas) and the SEB-04.L fixtures; this
  * spec builds its own deterministic fixtures. The vector candidate branch is covered by the last HDQ-07.9 test, which
  * passes no query text so no embedding provider is called.
  */
final class AtlasSkillEvidenceRankingIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val model = "synthetic-hiring-evaluation"
  private val vector = List.tabulate(32)(index => if (index == 0) 1.0f else 0.01f)
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val noFilter = JobSearchFilter(None, Set.empty, None)
  private val bound = pageOf(PageSize.Max)
  private val first = pageOf(7)

  private def pageOf(size: Int) =
    PageSize.fromInt(size).toEither.fold(errors => throw new IllegalArgumentException(errors.toString), identity)

  private def userId(name: String) = UserId(UUID.nameUUIDFromBytes(s"hdq07-atlas-$name".getBytes("UTF-8")))
  private def recruiterNamed(name: String) = User(
    userId(name),
    None,
    s"Ranking recruiter $name",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val owner = recruiterNamed("owner")
  private val otherRecruiter = recruiterNamed("other")
  private val ghostRecruiter = recruiterNamed("ghost")
  private val viewer = User(
    userId("viewer"),
    None,
    "Ranking candidate",
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("scala"), None, None))),
    now
  )
  private def ctx(user: User) = ActorContext(user.id, user.role)

  private def jobAt(
      n: Int,
      skills: Set[String] = Set("scala"),
      status: JobStatus = JobStatus.Open,
      embedded: Boolean = true,
      staleSource: Boolean = false,
      ownedBy: User = owner,
      jobVector: List[Float] = vector
  ): Job = {
    val base = Job(
      JobId(new UUID(7L, n.toLong)),
      ownedBy.id,
      s"Scala engineer $n",
      s"Synthetic ranking description $n",
      List("requirement"),
      skills,
      Location("Cyprus", Some("Nicosia"), false, None),
      JobState.requested(status, now),
      now,
      now
    )
    if (!embedded) base
    else {
      val source = if (staleSource) "a source the job no longer has" else SearchableText.job(base)
      base.copy(embedding = Some(EntityEmbedding(jobVector, EmbeddingMeta(model, SourceHash.sha256(source), now))))
    }
  }

  private def candidateAt(
      tag: String,
      skills: Set[String],
      residence: Option[CandidateResidence],
      availability: Option[CandidateAvailabilityStatus],
      optIn: Boolean,
      embedded: Boolean = false,
      summary: String = "Scala hiring evaluation",
      staleSource: Boolean = false
  ): User = {
    val profile = CandidateProfile(skills, Some(summary), None, residence, availability, optIn)
    val source = if (staleSource) "a source the candidate no longer has" else SearchableText.candidate(profile)
    User(
      userId(s"candidate-$tag"),
      None,
      s"Name $tag",
      UserRole.Candidate,
      Some(UserProfile.Candidate(profile)),
      now,
      embedding = Option.when(embedded)(
        EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(source), now))
      )
    )
  }

  /** Mirrors the shared-Atlas gating of `AtlasDiscoveryIntegrationSpec`, returning the body's value. */
  private def withAtlas[A](
      body: (MongoDatabase[IO], DiscoveryQueryPolicy, AtlasLocalSupport.RecordedCommands) => IO[A]
  ): IO[A] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    val commands = new AtlasLocalSupport.RecordedCommands
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_))).flatMap { uri =>
      AtlasLocalSupport
        .client(uri, Some(commands))
        .flatMap(client =>
          AtlasLocalSupport.nonceDatabase(client, AtlasLocalSupport.nonceName("search_evaluation_ranking_"))
        )
        .use(db => DiscoveryQueryPolicy.create(5.seconds, 4).flatMap(policy => body(db, policy, commands)))
    }
  }

  private def users(db: MongoDatabase[IO]) =
    new MongoUserRepository(
      db,
      MongoRepositoryTestSupport.noTransaction,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )

  private def seed(db: MongoDatabase[IO], people: List[User], jobs: List[Job]): IO[Unit] =
    MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO]) *>
      people.traverse_(user =>
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

  /** Candidate matching answers from the lexical branch unless `vectorCandidates`; recommendations use the real vector
    * branch.
    */
  private def serviceOf(
      db: MongoDatabase[IO],
      repo: MongoSemanticSearchRepository,
      vectorCandidates: Boolean = false
  ): SemanticSearchService[IO] =
    new SemanticSearchService(
      users(db),
      new MongoJobRepository(
        db,
        MongoRepositoryTestSupport.noTransaction,
        MongoEmbeddingWorkEnqueuer.disabled,
        Diagnostics.noop[IO]
      ),
      new EmbeddingService[IO] {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          IO.raiseError(new IllegalStateException("Synthetic ranking gate must not call an embedding provider"))
      },
      new SemanticSearchRepository[IO] {
        export repo.{
          authorizedJobEligibility,
          authorizedCandidateEligibility,
          searchJobs,
          recommendedJobs,
          lexicalJobs,
          lexicalCandidates
        }
        def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] =
          if (vectorCandidates) repo.candidateMatches(query)
          else RepositoryF.at[IO].raise(RepositoryError.VectorSearchUnavailable)
      },
      model
    )

  private def validated(filters: CandidateMatchFilters) =
    ValidatedCandidateMatchFilters.from(filters).fold(e => fail(e.toString), identity)

  /** Probe without any private filter: every active fixture with the skill must be indexed before assertions run. */
  private def awaitCandidatesIndexed(repo: MongoSemanticSearchRepository, fixtureIds: Set[UserId]): IO[Unit] =
    eventually(240)(
      repo
        .lexicalCandidates(
          LexicalSearchQuery("scala", noFilter, first, UUID.randomUUID(), validated(CandidateMatchFilters.empty))
        )
        .value
    )(hits => fixtureIds.subsetOf(hits.map(_.id).toSet)).void

  /** Deletes the account exactly as production does: the tombstone drops the profile and embedding. */
  private def tombstone(db: MongoDatabase[IO], user: User): IO[Unit] =
    MongoRepositoryTestSupport.collection(db, MongoCollections.Users).flatMap { collection =>
      collection
        .updateOne(Filters.eq(MongoFields.Id, user.id.value.toString), AtlasLocalSupport.tombstoneUpdate(now))
        .void
    }

  /** Closes a job as production does: status, close time and revision advance together. */
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

  private def orFail[E, A](label: String)(result: Either[E, A]): A =
    result.fold(error => fail(s"$label failed: $error"), identity)

  // ---------------------------------------------------------------- HDQ-07.9

  test(
    "HDQ-07.9 deleted and opted-in non-matching candidates never reach the ranker on the Atlas Local lexical matching path, opted-out candidates may"
  ) {
    withAtlas { (db, policy, _) =>
      val matchJob = jobAt(1)
      val cyprus = Some(CandidateResidence("Cyprus", Some("Nicosia")))
      val elsewhere = Some(CandidateResidence("Elsewhere", Some("Nicosia")))
      val available = Some(CandidateAvailabilityStatus.AVAILABLE_NOW)
      val eligible =
        candidateAt("eligible", Set("scala"), cyprus, available, optIn = true, summary = "Scala hiring one")
      val optedOut =
        candidateAt("opted-out", Set("scala"), elsewhere, available, optIn = false, summary = "Scala hiring two words")
      val nonMatching =
        candidateAt(
          "non-matching",
          Set("scala"),
          elsewhere,
          available,
          optIn = true,
          summary = "Scala hiring three more words"
        )
      val deleted =
        candidateAt(
          "deleted",
          Set("scala"),
          cyprus,
          available,
          optIn = true,
          summary = "Scala hiring four more words here"
        )
      val all = List(eligible, optedOut, nonMatching, deleted)
      val fixtureIds = all.map(_.id).toSet
      val filters = CandidateMatchFilters(Nil, Some("cyprus"), Some("nicosia"), None)
      val repo = repositoryOf(db, policy)
      val service = serviceOf(db, repo)
      for {
        _ <- seed(db, List(owner, otherRecruiter, viewer) ++ all, List(matchJob))
        // All fixtures are indexed while still active; only then is one deleted, so the live `$match` between `$search`
        // and `$limit` (which removes Deleted and opted-in non-matching candidates before the limit counts them) and the
        // service's authoritative recheck, not index lag or absence from the index, remove them.
        _ <- awaitCandidatesIndexed(repo, fixtureIds)
        _ <- tombstone(db, deleted)
        plain <- service.candidateMatches(ctx(owner), matchJob.id, None, filters, bound, UUID.randomUUID()).value
        ranker <- RecordingEvidenceRanker.create()
        ranked <- SkillEvidenceRankedComposition
          .candidateMatches(
            service,
            ranker,
            ctx(owner),
            matchJob.id,
            matchJob.skills,
            bound,
            UUID.randomUUID(),
            filters
          )
          .value
        seen <- ranker.invocations
      } yield {
        val plainHits = orFail("plain matching")(plain).hits
        val reached = plainHits.map(_.candidate.id).filter(fixtureIds).toSet
        // Premises from the real service over the real index: consent model, deletion and the private filter.
        assertEquals(reached, Set(eligible.id, optedOut.id))
        assertEquals(seen.size, 1)
        val received = seen.head.map(_.id).filter(id => fixtureIds.map(_.value.toString)(id))
        assertEquals(received.toSet, reached.map(_.value.toString))
        // Ties between equally scored hits may order differently across calls, so inputs are compared as sets.
        assertEquals(seen.head.map(_.id).toSet, plainHits.map(_.candidate.id.value.toString).toSet)
        val excluded = Set(nonMatching.id, deleted.id).map(_.value.toString)
        assert(seen.head.forall(hit => !excluded(hit.id)), clue(seen.head.map(_.id)))
        val output = orFail("ranked matching")(ranked)
        assertEquals(output.basis, Some(RankingBasis.SkillEvidence))
        assert(output.items.forall(item => !excluded(item.candidate.id.value.toString)), clue(output.items))
        assertEquals(output.items.map(_.candidate.id).toSet, plainHits.map(_.candidate.id).toSet)
      }
    }
  }

  test(
    "HDQ-07.9 closed, deleted and stale-source jobs never reach the ranker on Atlas Local recommendations"
  ) {
    withAtlas { (db, policy, _) =>
      def distinct(n: Int) = vector.updated(1, 0.01f + 0.05f * n.toFloat)
      val caller = candidateAt("caller", Set("scala"), None, None, optIn = false, embedded = true)
      val openA = jobAt(1, Set("scala"), jobVector = distinct(1))
      val openB = jobAt(2, Set("scala", "kafka"), jobVector = distinct(2))
      val closedLater = jobAt(3, jobVector = distinct(3))
      val deletedLater = jobAt(4, jobVector = distinct(4))
      val stale = jobAt(5, staleSource = true, jobVector = distinct(5))
      val all = List(openA, openB, closedLater, deletedLater, stale)
      val fixtureIds = all.map(_.id).toSet
      val repo = repositoryOf(db, policy)
      val service = serviceOf(db, repo)
      val probe = VectorSearchQuery(vector, None, noFilter, first, SearchMode.VECTOR, model, UUID.randomUUID())
      for {
        _ <- seed(db, List(owner, otherRecruiter, viewer, caller), all)
        _ <- eventually(240)(repo.recommendedJobs(probe).value)(hits => fixtureIds.subsetOf(hits.map(_.id).toSet))
        jobsCollection <- MongoRepositoryTestSupport.collection(db, MongoCollections.Jobs)
        _ <- close(db, closedLater)
        _ <- jobsCollection.deleteOne(Filters.eq(MongoFields.Id, deletedLater.id.value.toString))
        plain <- service.recommendedJobs(ctx(caller), bound, UUID.randomUUID()).value
        ranker <- RecordingEvidenceRanker.create()
        ranked <- SkillEvidenceRankedComposition
          .recommendedJobs(service, ranker, ctx(caller), Set("scala"), bound, UUID.randomUUID())
          .value
        seen <- ranker.invocations
      } yield {
        val plainJobs = orFail("plain recommendations")(plain)
        assertEquals(plainJobs.map(_.job.id).filter(fixtureIds).toSet, Set(openA.id, openB.id))
        assertEquals(seen.size, 1)
        // The fixtures have distinct vectors, so their scores and order are stable across the two calls.
        assertEquals(seen.head.map(_.id), plainJobs.map(_.job.id.value.toString))
        val excluded = Set(closedLater.id, deletedLater.id, stale.id).map(_.value.toString)
        assert(seen.head.forall(hit => !excluded(hit.id)), clue(seen.head.map(_.id)))
        val output = orFail("ranked recommendations")(ranked)
        assertEquals(output.basis, Some(RankingBasis.SkillEvidence))
        assert(output.items.forall(item => !excluded(item.job.id.value.toString)), clue(output.items))
        assertEquals(output.items.map(_.job.id).toSet, plainJobs.map(_.job.id).toSet)
      }
    }
  }

  test(
    "HDQ-07.9 stale-source and deleted candidates never reach the ranker on the Atlas Local vector candidate branch, opted-out candidates may"
  ) {
    withAtlas { (db, policy, _) =>
      val matchJob = jobAt(1)
      val cyprus = Some(CandidateResidence("Cyprus", Some("Nicosia")))
      val available = Some(CandidateAvailabilityStatus.AVAILABLE_NOW)
      val eligible = candidateAt("v-eligible", Set("scala"), cyprus, available, optIn = true, embedded = true)
      val optedOut = candidateAt("v-opted-out", Set("scala"), None, None, optIn = false, embedded = true)
      val stale =
        candidateAt("v-stale", Set("scala"), cyprus, available, optIn = true, embedded = true, staleSource = true)
      val deleted = candidateAt("v-deleted", Set("scala"), cyprus, available, optIn = true, embedded = true)
      val all = List(eligible, optedOut, stale, deleted)
      val fixtureIds = all.map(_.id).toSet
      val repo = repositoryOf(db, policy)
      val service = serviceOf(db, repo, vectorCandidates = true)
      val probe = VectorSearchQuery(vector, None, noFilter, first, SearchMode.VECTOR, model, UUID.randomUUID())
      for {
        _ <- seed(db, List(owner, otherRecruiter, viewer) ++ all, List(matchJob))
        // Every embedded fixture is in the vector index before one is deleted, so the recheck removes it.
        _ <- eventually(240)(repo.candidateMatches(probe).value)(hits => fixtureIds.subsetOf(hits.map(_.id).toSet))
        _ <- tombstone(db, deleted)
        plain <- service
          .candidateMatches(ctx(owner), matchJob.id, None, CandidateMatchFilters.empty, bound, UUID.randomUUID())
          .value
        ranker <- RecordingEvidenceRanker.create()
        ranked <- SkillEvidenceRankedComposition
          .candidateMatches(service, ranker, ctx(owner), matchJob.id, matchJob.skills, bound, UUID.randomUUID())
          .value
        seen <- ranker.invocations
      } yield {
        val results = orFail("plain vector matching")(plain)
        assertEquals(results.degraded, None, clue("the vector branch answered, not the lexical fallback"))
        val reached = results.hits.map(_.candidate.id).filter(fixtureIds).toSet
        assertEquals(reached, Set(eligible.id, optedOut.id))
        assertEquals(seen.size, 1)
        assertEquals(seen.head.map(_.id).toSet, results.hits.map(_.candidate.id.value.toString).toSet)
        val excluded = Set(stale.id, deleted.id).map(_.value.toString)
        assert(seen.head.forall(hit => !excluded(hit.id)), clue(seen.head.map(_.id)))
        val output = orFail("ranked vector matching")(ranked)
        assertEquals(output.basis, Some(RankingBasis.SkillEvidence))
        assert(output.items.forall(item => !excluded(item.candidate.id.value.toString)), clue(output.items))
        assertEquals(output.items.map(_.candidate.id).toSet, results.hits.map(_.candidate.id).toSet)
      }
    }
  }

  test(
    "HDQ-07.9 forbidden and ineligible callers get the plain service's typed error with zero ranker invocations and zero retrievals on Atlas Local"
  ) {
    withAtlas { (db, policy, commands) =>
      val matchJob = jobAt(1)
      val closedOwned = jobAt(2, status = JobStatus.Closed)
      val ghostJob = jobAt(3, ownedBy = ghostRecruiter)
      val goodCandidate = candidateAt("visible", Set("scala"), None, None, optIn = false)
      val controlCaller = candidateAt("control-caller", Set("scala"), None, None, optIn = false, embedded = true)
      val deletedCaller = candidateAt("deleted-caller", Set("scala"), None, None, optIn = false, embedded = true)
      val repo = repositoryOf(db, policy)
      val service = serviceOf(db, repo)
      val filters = CandidateMatchFilters.empty
      val recommendationProbe =
        VectorSearchQuery(vector, None, noFilter, first, SearchMode.VECTOR, model, UUID.randomUUID())
      def matching(label: String, actor: ActorContext, id: JobId, expected: UseCaseError) =
        for {
          plain <- service.candidateMatches(actor, id, None, filters, bound, UUID.randomUUID()).value
          ranker <- RecordingEvidenceRanker.create()
          ranked <- SkillEvidenceRankedComposition
            .candidateMatches(service, ranker, actor, id, Set("scala"), first, UUID.randomUUID(), filters)
            .value
          seen <- ranker.invocations
        } yield {
          assertEquals(plain.map(_ => ()), Left(expected), clue(label))
          assertEquals(ranked.map(_ => ()), Left(expected), clue(label))
          assertEquals(seen, Vector.empty, clue(label))
        }
      def recommending(label: String, actor: ActorContext, expected: UseCaseError) =
        for {
          plain <- service.recommendedJobs(actor, bound, UUID.randomUUID()).value
          ranker <- RecordingEvidenceRanker.create()
          ranked <- SkillEvidenceRankedComposition
            .recommendedJobs(service, ranker, actor, Set("scala"), first, UUID.randomUUID())
            .value
          seen <- ranker.invocations
        } yield {
          assertEquals(plain.map(_ => ()), Left(expected), clue(label))
          assertEquals(ranked.map(_ => ()), Left(expected), clue(label))
          assertEquals(seen, Vector.empty, clue(label))
        }
      for {
        _ <- seed(
          db,
          List(owner, otherRecruiter, ghostRecruiter, viewer, goodCandidate, controlCaller, deletedCaller),
          List(matchJob, closedOwned, ghostJob)
        )
        _ <- awaitCandidatesIndexed(repo, Set(goodCandidate.id))
        _ <- eventually(240)(repo.recommendedJobs(recommendationProbe).value)(_.exists(_.id == matchJob.id))
        _ <- tombstone(db, ghostRecruiter)
        _ <- tombstone(db, deletedCaller)
        // Positive control: authorized calls do issue retrievals that the recorder sees, so the zero counts below
        // cannot hold merely because nothing was recorded.
        _ <- IO.delay(commands.clear())
        authorizedMatches <- service
          .candidateMatches(ctx(owner), matchJob.id, None, filters, bound, UUID.randomUUID())
          .value
        authorizedRecommendations <- service.recommendedJobs(ctx(controlCaller), bound, UUID.randomUUID()).value
        controlUsers <- IO.delay(commands.retrievals(db.underlying.getName, MongoCollections.Users))
        controlJobs <- IO.delay(commands.retrievals(db.underlying.getName, MongoCollections.Jobs))
        _ <- IO.delay(commands.clear())
        _ <- matching(
          "a candidate calling candidateMatches",
          ctx(viewer),
          matchJob.id,
          UseCaseError.Domain(DomainError.RecruiterRequired)
        )
        _ <- matching(
          "a non-owner recruiter",
          ctx(otherRecruiter),
          matchJob.id,
          UseCaseError.Domain(DomainError.NotFound("job"))
        )
        _ <- matching("a closed owned job", ctx(owner), closedOwned.id, UseCaseError.Domain(DomainError.JobMustBeOpen))
        _ <- matching(
          "a deleted recruiter",
          ctx(ghostRecruiter),
          ghostJob.id,
          UseCaseError.Authentication(AuthenticationError.Unauthorized)
        )
        _ <- recommending(
          "a recruiter calling recommendedJobs",
          ctx(owner),
          UseCaseError.Domain(DomainError.CandidateRequired)
        )
        _ <- recommending(
          "a deleted candidate",
          ctx(deletedCaller),
          UseCaseError.Authentication(AuthenticationError.Unauthorized)
        )
        users <- IO.delay(commands.retrievals(db.underlying.getName, MongoCollections.Users))
        jobs <- IO.delay(commands.retrievals(db.underlying.getName, MongoCollections.Jobs))
      } yield {
        assert(authorizedMatches.isRight, clue(authorizedMatches))
        assert(authorizedRecommendations.isRight, clue(authorizedRecommendations))
        assert(controlUsers.nonEmpty, clue("the authorized candidate matching issued no users retrieval"))
        assert(controlJobs.nonEmpty, clue("the authorized recommendations issued no jobs retrieval"))
        assertEquals(users, Nil)
        assertEquals(jobs, Nil)
      }
    }
  }

  // ---------------------------------------------------------------- HDQ-07.8

  private final case class World(
      inputShape: List[(Int, Option[com.example.graphQL.cats.domain.search.SkillEvidence])],
      fixturePositions: List[Int],
      outputPositions: List[Int],
      basis: Option[RankingBasis]
  )

  private val skillSets = List(Set("scala", "cats", "kafka"), Set("scala", "cats"), Set("scala"))
  private val evidenceJobSkills = Set("scala", "cats", "kafka")

  /** One world: the same user ids, skills and summaries in a fresh database; only `privateAttributes` changes consent,
    * residence, availability and name.
    */
  private def world(privateAttributes: Boolean, filters: CandidateMatchFilters): IO[World] =
    withAtlas { (db, policy, _) =>
      val cyprus = Some(CandidateResidence("Cyprus", Some("Nicosia")))
      val matchJob = jobAt(1, evidenceJobSkills)
      val fixtures = skillSets.zipWithIndex.map { case (skills, index) =>
        if (privateAttributes)
          candidateAt(s"rank-$index", skills, cyprus, Some(CandidateAvailabilityStatus.AVAILABLE_NOW), optIn = true)
            .copy(name = s"Zed private $index")
        else candidateAt(s"rank-$index", skills, None, None, optIn = false).copy(name = s"Alice public $index")
      }
      val fixtureIds = fixtures.map(_.id).toSet
      val repo = repositoryOf(db, policy)
      val service = serviceOf(db, repo)
      for {
        _ <- seed(db, List(owner, otherRecruiter, viewer) ++ fixtures, List(matchJob))
        _ <- awaitCandidatesIndexed(repo, fixtureIds)
        ranker <- RecordingEvidenceRanker.create()
        ranked <- SkillEvidenceRankedComposition
          .candidateMatches(
            service,
            ranker,
            ctx(owner),
            matchJob.id,
            evidenceJobSkills,
            bound,
            UUID.randomUUID(),
            filters
          )
          .value
        seen <- ranker.invocations
      } yield {
        val selection = orFail("ranked matching")(ranked)
        assertEquals(seen.size, 1)
        val indexOf = fixtures.map(_.id.value.toString).zipWithIndex.toMap
        val input = seen.head
        // The fixtures' input order, as fixture indexes, and what the ranker received for them (evidence only).
        val inputOrder = input.filter(hit => indexOf.contains(hit.id)).map(hit => indexOf(hit.id))
        assertEquals(inputOrder.toSet, Set(0, 1, 2), clue("all three fixtures reach the ranker"))
        World(
          input.map(hit => (hit.retrievalRank, hit.evidence)),
          inputOrder,
          selection.items.flatMap(item => indexOf.get(item.candidate.id.value.toString)),
          selection.basis
        )
      }
    }

  private def assertWorldsEqual(withPrivate: World, withoutPrivate: World): Unit = {
    // The shared viewer is a hit in both worlds, so the full input shape (ranks and evidence) is comparable.
    assertEquals(withPrivate.inputShape.size, withoutPrivate.inputShape.size)
    assertEquals(withPrivate.fixturePositions, withoutPrivate.fixturePositions)
    assertEquals(withPrivate.inputShape, withoutPrivate.inputShape)
    assertEquals(withPrivate.outputPositions, withoutPrivate.outputPositions)
    assertEquals(withPrivate.basis, withoutPrivate.basis)
    // Sanity check only: BM25 already retrieves the fixtures in coverage order, so this does not tell the ranker from
    // retrieval order; the property under test is the equality of the two worlds above.
    assertEquals(withPrivate.outputPositions.take(3), List(0, 1, 2))
  }

  test(
    "HDQ-07.8 changing only candidates' consent, residence, availability and name leaves ranker input and positions unchanged through the Atlas Local index"
  ) {
    // Counterfactual with no private filter at all, as the criterion states: opted-in and opted-out fixtures are both
    // retrieved and only the private attributes and names differ between the worlds. The second pair filters on Cyprus:
    // an opted-in Cyprus resident and an opted-out one (whose private fields are ignored) both pass, so the worlds
    // again select the same candidates.
    val cyprusFilter = CandidateMatchFilters(Nil, Some("cyprus"), None, None)
    for {
      unfilteredWith <- world(privateAttributes = true, CandidateMatchFilters.empty)
      unfilteredWithout <- world(privateAttributes = false, CandidateMatchFilters.empty)
      filteredWith <- world(privateAttributes = true, cyprusFilter)
      filteredWithout <- world(privateAttributes = false, cyprusFilter)
    } yield {
      assertWorldsEqual(unfilteredWith, unfilteredWithout)
      assertWorldsEqual(filteredWith, filteredWithout)
    }
  }
}
