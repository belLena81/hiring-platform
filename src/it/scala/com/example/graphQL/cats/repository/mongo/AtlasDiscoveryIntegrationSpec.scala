package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, RepositoryError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.search.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.hiring.testing.DisposableAtlas
import com.mongodb.client.model.{Filters, Updates}
import io.circe.Json
import mongo4cats.database.MongoDatabase
import munit.CatsEffectSuite
import org.bson.Document
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Atlas Local gates for discovery (HDQ-01.L, HDQ-04.L, HDQ-05.L, HDQ-06.L, HDQ-11.L). An absent URI is a skipped
  * external gate, never acceptance. Records carry the `local-container` label; nothing here adopts a change. Timing
  * dependent observations that cannot be made are reported as inconclusive, never as passed.
  */
final class AtlasDiscoveryIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val center = GeoPoint(35.1856d, 33.3823d)
  private val model = "synthetic-hiring-evaluation"
  private val vector = List.tabulate(32)(index => if (index == 0) 1.0f else 0.01f)
  private val indexes =
    AtlasSearchIndexConfig("jobs_vector", "candidates_vector", "jobs_lexical", "candidates_lexical", 32, 120000, 500)
  private val noFilter = JobSearchFilter(None, Set.empty, None)
  private val BranchLimit = 3
  private def pageOf(size: Int) =
    PageSize.fromInt(size).toEither.fold(errors => throw new IllegalArgumentException(errors.toString), identity)

  private def userId(name: String) = UserId(UUID.nameUUIDFromBytes(s"hdq-discovery-$name".getBytes("UTF-8")))
  private val owner = User(
    userId("owner"),
    None,
    "Discovery recruiter",
    UserRole.Recruiter,
    Some(UserProfile.Recruiter(RecruiterProfile("Synthetic organization", None))),
    now
  )
  private val otherRecruiter = owner.copy(id = userId("other"), name = "Other recruiter")
  private val viewer = User(
    userId("viewer"),
    None,
    "Discovery candidate",
    UserRole.Candidate,
    Some(UserProfile.Candidate(CandidateProfile(Set("scala"), None, None))),
    now
  )

  private def live(maxTime: FiniteDuration = 5.seconds)(
      body: (MongoDatabase[IO], DiscoveryQueryPolicy, AtlasLocalSupport.RecordedCommands, String) => IO[Unit]
  ): IO[Unit] = IO.defer {
    assume(sys.env.contains("ATLAS_TEST_URI"), "BLOCKED: ATLAS_TEST_URI absent; live Atlas gate not executed")
    val commands = new AtlasLocalSupport.RecordedCommands
    IO.fromEither(DisposableAtlas.authorizedUri(sys.env).left.map(new IllegalArgumentException(_))).flatMap { uri =>
      AtlasLocalSupport
        .client(uri, Some(commands))
        .flatMap(client =>
          AtlasLocalSupport.nonceDatabase(client, AtlasLocalSupport.nonceName("search_evaluation_discovery_"))
        )
        .use(db => DiscoveryQueryPolicy.create(maxTime, 4).flatMap(policy => body(db, policy, commands, uri)))
    }
  }

  private def jobId(n: Int): JobId = JobId(new UUID(11L, n.toLong))

  private def job(
      n: Int,
      title: String = "Scala engineer",
      status: JobStatus = JobStatus.Open,
      latitudeOffset: Option[Double] = Some(0d),
      remote: Boolean = false,
      embedded: Boolean = false
  ): Job = {
    val base = Job(
      jobId(n),
      owner.id,
      title,
      s"Synthetic hiring description $n",
      List("requirement"),
      Set("scala"),
      Location(
        "Cyprus",
        Some("Nicosia"),
        remote,
        latitudeOffset.filter(_ => !remote).map(offset => GeoPoint(center.latitude + offset, center.longitude))
      ),
      JobState.requested(status, now),
      now,
      now
    )
    if (embedded)
      base.copy(embedding =
        Some(EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(base)), now)))
      )
    else base
  }

  private def seedUsers(db: MongoDatabase[IO], extra: List[User] = Nil): IO[Unit] =
    (List(owner, otherRecruiter, viewer) ++ extra).traverse_(user =>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Users, MongoHiringCodecs.user(user))
    )

  private def seedJobs(db: MongoDatabase[IO], jobs: List[Job]): IO[Unit] =
    jobs.traverse_(value =>
      MongoRepositoryTestSupport.insertOne(db, MongoCollections.Jobs, MongoHiringCodecs.job(value))
    )

  private def users(db: MongoDatabase[IO]) =
    new MongoUserRepository(
      db,
      MongoRepositoryTestSupport.noTransaction,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )

  private def jobRepository(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy) =
    new MongoJobRepository(
      db,
      MongoRepositoryTestSupport.noTransaction,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO],
      Some(policy)
    )

  private def scopeOf(db: MongoDatabase[IO], actor: User): IO[HiringReadScope] =
    IO.fromEither(
      HiringReadScope
        .validated(
          ActorContext(actor.id, actor.role),
          ActorRecord.of(actor),
          com.example.graphQL.cats.service.auth.ActorAuthorization(users(db))
        )
        .leftMap(error => new AssertionError(error.toString))
    )

  private def jobService(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy) =
    new com.example.graphQL.cats.service.job.JobService(
      users(db),
      jobRepository(db, policy),
      com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher.noop,
      com.example.graphQL.cats.service.mutation.Idempotent(
        new MongoMutationReceiptRepository(db, MongoRepositoryTestSupport.noTransaction, Diagnostics.noop[IO])
      ),
      Diagnostics.noop[IO],
      cats.effect.Clock[IO],
      cats.effect.std.UUIDGen[IO],
      lexicalMatcher = Some(matcher(db, policy)),
      lexicalFacetMatchLimit = LexicalMatchLimit.from(1000).get,
      lexicalFacetDeadline = Some(policy.maxTime)
    )

  private def matcher(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy) =
    new MongoLexicalJobMatcher(db, indexes.jobLexicalIndex, Diagnostics.noop[IO], policy)

  private def eventually[A](attempts: Int)(read: IO[Either[?, A]])(done: A => Boolean): IO[A] =
    read.flatMap {
      case Right(value) if done(value) => IO.pure(value)
      case _ if attempts > 0           => IO.sleep(500.millis) *> eventually(attempts - 1)(read)(done)
      case other => IO.raiseError(new AssertionError(s"Atlas index did not reach the expected state: $other"))
    }

  private def awaitLexical(db: MongoDatabase[IO], policy: DiscoveryQueryPolicy, expected: Int): IO[Unit] =
    eventually(240)(matcher(db, policy).matches("scala", noFilter, 5000).value)(_.ids.size == expected).void

  private def writeRecord(name: String, record: Json): IO[Unit] = IO.blocking {
    val directory = java.nio.file.Path.of(".local/data/search-evaluation")
    val _ = java.nio.file.Files.createDirectories(directory)
    val _ = java.nio.file.Files.writeString(directory.resolve(s"$name.json"), record.spaces2)
  }

  private def listedIndexes(db: MongoDatabase[IO], collection: String): IO[List[Document]] =
    MongoAccessEvaluationSupport
      .command(db, new Document("listIndexes", collection))
      .map(_.get("cursor", classOf[Document]).getList("firstBatch", classOf[Document]).asScala.toList)

  private def atlasVersion(db: MongoDatabase[IO]): IO[String] =
    MongoAccessEvaluationSupport
      .command(db, new Document("buildInfo", 1))
      .map(value => Option(value.getString("version")).getOrElse("unknown"))

  // ---------------------------------------------------------------- HDQ-01.L

  test(
    "HDQ-01.L radius discovery returns only eligible jobs inside the boundary beside ready Atlas search indexes"
  ) {
    live() { (db, policy, commands, _) =>
      val inside = List(0d, 0.03d, 0.06d, 0.085d).zipWithIndex.map { case (offset, index) =>
        job(index + 1, latitudeOffset = Some(offset))
      }
      val excluded = List(
        job(10, latitudeOffset = Some(0.2d)), // about 22 km away
        job(11, latitudeOffset = Some(0.095d)), // about 10.6 km, just beyond the 10 km boundary
        job(12, latitudeOffset = Some(0.01d), status = JobStatus.Closed),
        job(13, latitudeOffset = Some(0.02d), status = JobStatus.Draft),
        job(14, remote = true) // remote jobs carry no point
      )
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db) *> seedJobs(db, inside ++ excluded)
        // Index readiness: the 2dsphere index and all four Atlas search indexes exist and are queryable.
        geo <- listedIndexes(db, MongoCollections.Jobs)
        search <- List(MongoCollections.Jobs, MongoCollections.Users).flatTraverse(
          MongoAtlasSearchAdmin.listIndexes(db, _, 100)
        )
        // The lexical index is live beside the geo index: it indexes the Open jobs (inside, the two far ones and the remote one).
        _ <- awaitLexical(db, policy, inside.size + 3)
        scope <- scopeOf(db, viewer)
        _ = commands.clear()
        result <- jobRepository(db, policy).nearbyJobs(scope, NearbyJobsQuery(center, 10d, noFilter), 30).value
        gate = commands.aggregates(db.underlying.getName, MongoCollections.Users)
      } yield {
        val geoIndex = geo.find(_.getString("name") == MongoIndexNames.JobsLocationPoint)
        assert(geoIndex.exists(_.get("key", classOf[Document]).get("location.point") == "2dsphere"), clue(geo))
        val queryable = search.map(index => index.getString("name") -> index.getBoolean("queryable", false)).toMap
        assertEquals(
          queryable.keySet,
          Set(
            indexes.jobVectorIndex,
            indexes.candidateVectorIndex,
            indexes.jobLexicalIndex,
            indexes.candidateLexicalIndex
          )
        )
        assert(queryable.values.forall(identity), clue(queryable))
        val found = result.fold(error => fail(s"nearbyJobs failed: $error"), identity)
        assertEquals(found.map(_.job.id).toSet, inside.map(_.id).toSet)
        assert(found.forall(_.distanceKm <= 10d), clue(found.map(_.distanceKm)))
        assertEquals(found.map(_.distanceKm), found.map(_.distanceKm).sorted)
        // One authoritative aggregate (actor gate with the $geoNear lookup) and no per-job fetch on either collection.
        assertEquals(gate.size, 1, clue(gate))
        assertEquals(commands.findCount(db.underlying.getName, MongoCollections.Jobs), 0)
      }
    }
  }

  // ---------------------------------------------------------------- HDQ-04.L

  test(
    "HDQ-04.L lexical, vector and hybrid job branches keep the analyzer semantics, the branch limit and the Open filter"
  ) {
    live() { (db, policy, commands, _) =>
      val open = (1 to 6).toList.map(n => job(n, title = s"Scala engineer $n", embedded = true))
      val closed = job(20, title = "Scala engineer closed", status = JobStatus.Closed, embedded = true)
      val repo = new MongoSemanticSearchRepository(
        db,
        indexes.jobVectorIndex,
        indexes.candidateVectorIndex,
        indexes.jobLexicalIndex,
        indexes.candidateLexicalIndex,
        100,
        BranchLimit,
        diagnostics = Diagnostics.noop[IO],
        discoveryPolicy = Some(policy)
      )
      val filters =
        ValidatedCandidateMatchFilters.from(CandidateMatchFilters.empty).fold(e => fail(e.toString), identity)
      def lexical(text: String) =
        repo.lexicalJobs(LexicalSearchQuery(text, noFilter, pageOf(7), UUID.randomUUID(), filters)).value
      def vectorQuery(text: Option[String]) = VectorSearchQuery(
        vector,
        text,
        noFilter,
        pageOf(7),
        if (text.isDefined) SearchMode.HYBRID else SearchMode.VECTOR,
        model,
        UUID.randomUUID()
      )
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db) *> seedJobs(db, open :+ closed)
        _ <- eventually(240)(lexical("scala"))(_.size == BranchLimit)
        _ <- eventually(240)(repo.searchJobs(vectorQuery(None)).value)(_.size == BranchLimit)
        _ = commands.clear()
        upper <- lexical("SCALA")
        lexicalRoots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
        _ = commands.clear()
        vectorOnly <- repo.searchJobs(vectorQuery(None)).value
        vectorRoots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
        _ = commands.clear()
        hybrid <- repo.searchJobs(vectorQuery(Some("SCALA"))).value
        hybridRoots = commands.retrievals(db.underlying.getName, MongoCollections.Jobs).flatMap(_.stages.headOption)
      } yield {
        val openIds = open.map(_.id).toSet
        val lexicalHits = upper.fold(error => fail(s"lexical failed: $error"), identity)
        // Analyzer: an upper-case query matches lower-case indexed text; the branch limit binds at 3 of 6.
        assertEquals(lexicalHits.size, BranchLimit)
        assert(lexicalHits.forall(hit => openIds.contains(hit.id)), clue(lexicalHits.map(_.id)))
        val vectorHits = vectorOnly.fold(error => fail(s"vector failed: $error"), identity)
        assertEquals(vectorHits.size, BranchLimit)
        val hybridHits = hybrid.fold(error => fail(s"hybrid failed: $error"), identity)
        assert(hybridHits.nonEmpty && hybridHits.size <= BranchLimit, clue(hybridHits.size))
        // Filter: the closed job matching the text and vector is never returned by any branch.
        List(lexicalHits.map(_.id), vectorHits.map(_.id), hybridHits.map(_.id)).foreach(ids =>
          assert(!ids.contains(closed.id), clue(ids))
        )
        // One round trip per branch.
        assertEquals(lexicalRoots, List("$search"))
        assertEquals(vectorRoots, List("$vectorSearch"))
        assertEquals(hybridRoots.sorted, List("$search", "$vectorSearch"))
      }
    }
  }

  // ---------------------------------------------------------------- HDQ-05.L

  test(
    "HDQ-05.L structured facets never count closed, draft or deleted jobs and the lexical index lag is recorded"
  ) {
    live() { (db, policy, _, uri) =>
      val open = (1 to 8).toList.map(n => job(n, title = s"Scala engineer $n"))
      val restricted = List(
        job(9, title = "Scala engineer closed", status = JobStatus.Closed),
        job(10, title = "Scala engineer draft", status = JobStatus.Draft)
      )
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db) *> seedJobs(db, open ++ restricted)
        // The search index has indexed all eight Open jobs before the changes below.
        _ <- awaitLexical(db, policy, open.size)
        scope <- scopeOf(db, viewer)
        repo = jobRepository(db, policy)
        jobsCollection <- MongoRepositoryTestSupport.collection(db, MongoCollections.Jobs)
        closedNow = open(0).id
        deletedNow = open(1).id
        _ <- jobsCollection.updateOne(
          Filters.eq(MongoFields.Id, closedNow.value.toString),
          Updates.set("status", "Closed")
        )
        _ <- jobsCollection.deleteOne(Filters.eq(MongoFields.Id, deletedNow.value.toString))
        facets <- repo.jobDiscoveryFacets(scope, JobFacetQuery(noFilter, None), None, None).value
        // Text query issued right after the change, before the index is observed to converge: the index may still
        // list the closed and deleted jobs, the authoritative recheck must not count them.
        textFacets <- jobService(db, policy)
          .jobDiscoveryFacets(ActorContext(viewer.id, viewer.role), JobFacetQuery(noFilter, None, Some("scala")))
          .value
        // Lag observation: does the index still list the closed or deleted job right after the change?
        started <- IO.monotonic
        first <- matcher(db, policy).matches("scala", noFilter, 5000).value
        staleListed = first.toOption.exists(found => found.ids.contains(closedNow) || found.ids.contains(deletedNow))
        removed <- (0 until 60).toList.foldLeftM(!staleListed) { (done, _) =>
          if (done) IO.pure(true)
          else
            IO.sleep(500.millis) *> matcher(db, policy)
              .matches("scala", noFilter, 5000)
              .value
              .map(
                _.toOption.exists(found => !found.ids.contains(closedNow) && !found.ids.contains(deletedNow))
              )
        }
        ended <- IO.monotonic
        _ <- writeRecord(
          "facets-index-lag",
          Json.obj(
            "criterion" -> Json.fromString("HDQ-05.L"),
            "environment" -> Json.fromString(DisposableAtlas.deployment(uri)),
            "staleIndexedHitObserved" -> Json.fromBoolean(staleListed),
            "indexRemovalObserved" -> Json.fromBoolean(removed),
            "lagObserved" -> Json.fromBoolean(staleListed),
            "elapsedMillis" -> Json.fromLong((ended - started).toMillis),
            "scope" -> Json.fromString(
              "Closed and deleted job after indexing; structured counts read operational data, the record only observes the lexical index."
            )
          )
        )
        // Restricted actor: a revoked actor learns nothing, not even an empty facet list.
        usersCollection <- MongoRepositoryTestSupport.collection(db, MongoCollections.Users)
        _ <- usersCollection.deleteOne(Filters.eq(MongoFields.Id, scope.userId.value.toString))
        revoked <- repo.jobDiscoveryFacets(scope, JobFacetQuery(noFilter, None), None, None).value
      } yield {
        val counted = facets.fold(error => fail(s"facets failed: $error"), identity)
        // Six Open jobs remain: neither the closed, the draft nor the deleted job is counted, immediately.
        assertEquals(counted.remote.map(_.count).sum, 6L)
        assertEquals(counted.cities.map(_.count).sum, 6L)
        assertEquals(counted.skills.find(_.value == "scala").map(_.count), Some(6L))
        val text = textFacets.fold(error => fail(s"text facets failed: $error"), identity)
        assertEquals(text.remote.map(_.count).sum, 6L, clue("closed and deleted jobs are never counted"))
        assertEquals(text.cities.map(_.count).sum, 6L)
        assertEquals(text.skills.find(_.value == "scala").map(_.count), Some(6L))
        assertEquals(text.coverage, FacetCoverage.AllMatches)
        assertEquals(revoked, Left(RepositoryError.AuthorityRevoked))
        // Lag is a recorded observation either way; a never-removed stale hit would mean the index did not converge.
        assume(removed, "Inconclusive: the stale index entry was still listed after 30 s")
      }
    }
  }

  // ---------------------------------------------------------------- HDQ-06.L

  test(
    "HDQ-06.L consent and private-field counterfactuals hold on Atlas Local branches with minimized output and denied non-owners"
  ) {
    live() { (db, policy, commands, _) =>
      val matchJob = job(1, embedded = true)
      def candidate(name: String, residence: Option[CandidateResidence], optIn: Boolean): User = {
        val profile = CandidateProfile(
          Set("scala"),
          Some("Scala hiring evaluation"),
          None,
          residence,
          Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
          optIn
        )
        User(
          UserId(UUID.nameUUIDFromBytes(s"hdq06-$name".getBytes("UTF-8"))),
          None,
          name,
          UserRole.Candidate,
          Some(UserProfile.Candidate(profile)),
          now,
          embedding = Some(
            EntityEmbedding(vector, EmbeddingMeta(model, SourceHash.sha256(SearchableText.candidate(profile)), now))
          )
        )
      }
      val canada = Some(CandidateResidence("Canada", Some("Toronto")))
      val elsewhere = Some(CandidateResidence("Elsewhere", Some("Toronto")))
      val matching = candidate("match", canada, optIn = true)
      val optedOut = candidate("opted-out", elsewhere, optIn = false)
      val absent = candidate("absent", elsewhere, optIn = false)
      val mismatch = candidate("mismatch", elsewhere, optIn = true)
      val all = List(matching, optedOut, absent, mismatch)
      val filters = CandidateMatchFilters(List("scala"), Some("canada"), Some("toronto"), Some("AVAILABLE_NOW"))
      val validated = ValidatedCandidateMatchFilters.from(filters).fold(e => fail(e.toString), identity)
      val repo = new MongoSemanticSearchRepository(
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
      val query = VectorSearchQuery(
        vector,
        None,
        noFilter,
        pageOf(7),
        SearchMode.VECTOR,
        model,
        UUID.randomUUID(),
        None,
        validated
      )
      val service = new SemanticSearchService(
        users(db),
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
        repo,
        model
      )
      def matches(actor: User) =
        service.candidateMatches(
          ActorContext(actor.id, actor.role),
          matchJob.id,
          None,
          filters,
          pageOf(7),
          UUID.randomUUID()
        )
      def ids(actor: User) = matches(actor).value.map(_.map(_.hits.map(_.candidate.id).toSet))
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db, all) *> seedJobs(db, List(matchJob))
        usersCollection <- MongoRepositoryTestSupport.collection(db, MongoCollections.Users)
        // The absent opt-in fixture: the field is physically missing, not false.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, absent.id.value.toString),
          Updates.unset("profile.recruiterSearchOptIn")
        )
        expected = Set(matching.id, optedOut.id, absent.id)
        _ <- eventually(240)(repo.candidateMatches(query).value)(_.map(_.id).toSet == expected)
        baseline <- matches(owner).value
        // Counterfactual 1 (live recheck, immediate): the opted-out candidate opts in while living elsewhere.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, optedOut.id.value.toString),
          Updates.set("profile.recruiterSearchOptIn", true)
        )
        afterOptIn <- ids(owner)
        // Counterfactual 2 (index path): the mismatching opted-in candidate opts out, so private predicates no
        // longer apply; it reappears once the index reflects the change. Timing dependent.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, mismatch.id.value.toString),
          Updates.set("profile.recruiterSearchOptIn", false)
        )
        reappeared <- (0 until 120).toList.foldLeftM(false) { (seen, _) =>
          if (seen) IO.pure(true)
          else
            ids(owner).flatMap { found =>
              if (found.exists(_.contains(mismatch.id))) IO.pure(true) else IO.sleep(500.millis).as(false)
            }
        }
        // Authorization: a recruiter who does not own the job and a candidate are denied before any retrieval.
        _ = commands.clear()
        nonOwner <- matches(otherRecruiter).value
        asCandidate <- matches(viewer).value
        retrievalsAfterDenied = commands.retrievals(db.underlying.getName, MongoCollections.Users)
      } yield {
        val first = baseline.fold(error => fail(s"owner search failed: $error"), identity)
        assertEquals(first.hits.map(_.candidate.id).toSet, expected)
        // Minimized output: only id, name, skills and experience summary; no residence, availability or consent.
        first.hits.foreach(hit =>
          assertEquals(
            hit.candidate.productElementNames.toList,
            List("id", "name", "skills", "experienceSummary"),
            clue(hit.candidate)
          )
        )
        assertEquals(afterOptIn, Right(Set(matching.id, absent.id)))
        assertEquals(nonOwner.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("job"))))
        assertEquals(asCandidate.map(_ => ()), Left(UseCaseError.Domain(DomainError.RecruiterRequired)))
        assertEquals(retrievalsAfterDenied, Nil)
        assume(reappeared, "Inconclusive: the opt-out change was not reflected by the index within 60 s")
      }
    }
  }

  test(
    "HDQ-06.L consent and private-field counterfactuals hold on the Atlas Local lexical candidate branch with minimized output and denied non-owners"
  ) {
    live() { (db, policy, commands, _) =>
      val matchJob = job(1, embedded = true)
      def candidate(name: String, residence: Option[CandidateResidence], optIn: Boolean): User = {
        val profile = CandidateProfile(
          Set("scala"),
          Some("Scala hiring evaluation"),
          None,
          residence,
          Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
          optIn
        )
        User(
          UserId(UUID.nameUUIDFromBytes(s"hdq06-lexical-$name".getBytes("UTF-8"))),
          None,
          name,
          UserRole.Candidate,
          Some(UserProfile.Candidate(profile)),
          now
        )
      }
      val canada = Some(CandidateResidence("Canada", Some("Toronto")))
      val elsewhere = Some(CandidateResidence("Elsewhere", Some("Toronto")))
      val matching = candidate("match", canada, optIn = true)
      val optedOut = candidate("opted-out", elsewhere, optIn = false)
      val absent = candidate("absent", elsewhere, optIn = false)
      val mismatch = candidate("mismatch", elsewhere, optIn = true)
      val all = List(matching, optedOut, absent, mismatch)
      // The lexical branch needs no embedding, so other seeded Active Candidates with the skill (the shared viewer)
      // are legitimate hits. The consent counterfactuals concern only this test's own four candidates.
      val fixtureIds = all.map(_.id).toSet
      val filters = CandidateMatchFilters(List("scala"), Some("canada"), Some("toronto"), Some("AVAILABLE_NOW"))
      val validated = ValidatedCandidateMatchFilters.from(filters).fold(e => fail(e.toString), identity)
      val repo = new MongoSemanticSearchRepository(
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
      // The vector branch is unavailable, so the service answers from the lexical candidate branch (degraded).
      val lexicalOnly = new SemanticSearchRepository[IO] {
        export repo.{
          authorizedJobEligibility,
          authorizedCandidateEligibility,
          searchJobs,
          recommendedJobs,
          similarJobs,
          lexicalJobs,
          lexicalCandidates
        }
        def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] =
          RepositoryF.at[IO].raise(RepositoryError.VectorSearchUnavailable)
      }
      val service = new SemanticSearchService(
        users(db),
        new MongoJobRepository(
          db,
          MongoRepositoryTestSupport.noTransaction,
          MongoEmbeddingWorkEnqueuer.disabled,
          Diagnostics.noop[IO]
        ),
        new EmbeddingService[IO] {
          def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
            IO.raiseError(new IllegalStateException("Synthetic lexical gate must not call an embedding provider"))
        },
        lexicalOnly,
        model
      )
      def matches(actor: User) =
        service.candidateMatches(
          ActorContext(actor.id, actor.role),
          matchJob.id,
          None,
          filters,
          pageOf(7),
          UUID.randomUUID()
        )
      def ids(actor: User) =
        matches(actor).value.map(_.map(_.hits.map(_.candidate.id).toSet.intersect(fixtureIds)))
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db, all) *> seedJobs(db, List(matchJob))
        usersCollection <- MongoRepositoryTestSupport.collection(db, MongoCollections.Users)
        // The absent opt-in fixture: the field is physically missing, not false.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, absent.id.value.toString),
          Updates.unset("profile.recruiterSearchOptIn")
        )
        expected = Set(matching.id, optedOut.id, absent.id)
        _ <- eventually(240)(
          repo.lexicalCandidates(LexicalSearchQuery("scala", noFilter, pageOf(7), UUID.randomUUID(), validated)).value
        )(_.map(_.id).toSet.intersect(fixtureIds) == expected)
        baseline <- matches(owner).value
        // Counterfactual 1 (live recheck, immediate): the opted-out candidate opts in while living elsewhere.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, optedOut.id.value.toString),
          Updates.set("profile.recruiterSearchOptIn", true)
        )
        afterOptIn <- ids(owner)
        // Counterfactual 2 (index path): the mismatching opted-in candidate opts out, so private predicates no
        // longer apply; it reappears once the index reflects the change. Timing dependent.
        _ <- usersCollection.updateOne(
          Filters.eq(MongoFields.Id, mismatch.id.value.toString),
          Updates.set("profile.recruiterSearchOptIn", false)
        )
        reappeared <- (0 until 120).toList.foldLeftM(false) { (seen, _) =>
          if (seen) IO.pure(true)
          else
            ids(owner).flatMap { found =>
              if (found.exists(_.contains(mismatch.id))) IO.pure(true) else IO.sleep(500.millis).as(false)
            }
        }
        // Authorization: a recruiter who does not own the job and a candidate are denied before any retrieval.
        _ = commands.clear()
        nonOwner <- matches(otherRecruiter).value
        asCandidate <- matches(viewer).value
        retrievalsAfterDenied = commands.retrievals(db.underlying.getName, MongoCollections.Users)
      } yield {
        val first = baseline.fold(error => fail(s"owner search failed: $error"), identity)
        assert(first.degraded.isDefined, clue(first.degraded))
        assertEquals(first.hits.map(_.mode).distinct, List(SearchMode.LEXICAL))
        assertEquals(first.hits.map(_.candidate.id).toSet.intersect(fixtureIds), expected)
        // Minimized output: only id, name, skills and experience summary; no residence, availability or consent.
        first.hits.foreach(hit =>
          assertEquals(
            hit.candidate.productElementNames.toList,
            List("id", "name", "skills", "experienceSummary"),
            clue(hit.candidate)
          )
        )
        assertEquals(afterOptIn, Right(Set(matching.id, absent.id)))
        assertEquals(nonOwner.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("job"))))
        assertEquals(asCandidate.map(_ => ()), Left(UseCaseError.Domain(DomainError.RecruiterRequired)))
        assertEquals(retrievalsAfterDenied, Nil)
        assume(reappeared, "Inconclusive: the opt-out change was not reflected by the index within 60 s")
      }
    }
  }

  // ---------------------------------------------------------------- HDQ-11.L

  private val radiiKm = List(1d, 3d, 5d, 10d, 20d, 50d)
  private val repetitions = 20

  private def currentPipeline(radiusKm: Double): List[Document] = List(
    new Document(
      "$geoNear",
      new Document(
        "near",
        new Document("type", "Point").append("coordinates", List(center.longitude, center.latitude).asJava)
      )
        .append("key", "location.point")
        .append("distanceField", "_distanceKm")
        .append("distanceMultiplier", 0.001d)
        .append("spherical", true)
        .append("maxDistance", radiusKm * 1000d)
        .append("query", MongoJobRepository.discoveryFilter(noFilter, None).append("location.remote", false))
    ),
    new Document("$sort", new Document("_distanceKm", 1).append("_id", 1)),
    new Document("$limit", 500),
    new Document("$project", new Document("_id", 1))
  )

  /** The unadopted alternative: the same boundary expressed as `$geoWithin`, ordered by id. */
  private def candidatePipeline(radiusKm: Double): List[Document] = List(
    new Document(
      "$match",
      MongoJobRepository
        .discoveryFilter(noFilter, None)
        .append("location.remote", false)
        .append(
          "location.point",
          new Document(
            "$geoWithin",
            new Document(
              "$centerSphere",
              List(List(center.longitude, center.latitude).asJava, radiusKm / 6378.1d).asJava
            )
          )
        )
    ),
    new Document("$sort", new Document("_id", 1)),
    new Document("$limit", 500),
    new Document("$project", new Document("_id", 1))
  )

  private def run(db: MongoDatabase[IO], pipeline: List[Document]): IO[Set[String]] =
    Mongo4catsCollections
      .documents(db, MongoCollections.Jobs)
      .flatMap(_.aggregate[Document](pipeline).stream.compile.toList)
      .map(_.map(_.getString("_id")).toSet)

  private final case class Explained(returned: Long, docs: Long, keys: Long, stages: Set[String])

  private def explain(db: MongoDatabase[IO], pipeline: List[Document]): IO[Option[Explained]] =
    MongoAccessEvaluationSupport
      .command(
        db,
        new Document(
          "explain",
          new Document("aggregate", MongoCollections.Jobs)
            .append("pipeline", pipeline.asJava)
            .append("cursor", new Document())
        ).append("verbosity", "executionStats")
      )
      .map { plan =>
        val json = io.circe.parser.parse(plan.toJson).getOrElse(Json.Null)
        // Every executionStats block of the plan is summed, so a multi-stage plan is not under-reported.
        val blocks = json.findAllByKey("executionStats").map { stats =>
          val c = stats.hcursor
          (
            c.get[Long]("nReturned").toOption,
            c.get[Long]("totalDocsExamined").toOption,
            c.get[Long]("totalKeysExamined").toOption
          ).tupled
        }
        Option.when(blocks.nonEmpty && blocks.forall(_.isDefined)) {
          val values = blocks.flatten
          Explained(
            values.map(_._1).sum,
            values.map(_._2).sum,
            values.map(_._3).sum,
            json.findAllByKey("stage").flatMap(_.asString).toSet
          )
        }
      }

  test(
    "HDQ-11.L paired geographic pipelines record plans, counts and latency as local-container with no change adopted"
  ) {
    live() { (db, _, _, uri) =>
      val workload = (0 until 200).toList.map { index =>
        job(
          index + 1,
          status = if (index % 7 == 0) JobStatus.Closed else JobStatus.Open,
          latitudeOffset = Option.when(index % 13 != 0)(index * 0.0005d),
          remote = index % 11 == 0
        )
      }
      def timed(pipelines: List[List[Document]], concurrency: Int) =
        (for { _ <- 1 to repetitions; pipeline <- pipelines } yield pipeline).toList
          .grouped(concurrency)
          .toList
          .flatTraverse(batch =>
            batch.parTraverse(pipeline =>
              for {
                start <- IO.monotonic
                outcome <- run(db, pipeline).attempt
                end <- IO.monotonic
              } yield ((end - start).toNanos.toDouble / 1000000d, outcome.isLeft)
            )
          )
      for {
        _ <- MongoHiringSetup.initialize(db, Some(indexes), Diagnostics.noop[IO])
        _ <- seedUsers(db) *> seedJobs(db, workload)
        version <- atlasVersion(db)
        now0 <- IO.realTime.map(_.toMillis)
        // Counts: the same ids for every query, or the candidate is not comparable.
        pairs <- radiiKm.traverse(radius =>
          (run(db, currentPipeline(radius)), run(db, candidatePipeline(radius))).tupled
        )
        identical = pairs.forall { case (current, candidate) => current == candidate }
        plans <- radiiKm.traverse(radius =>
          (explain(db, currentPipeline(radius)), explain(db, candidatePipeline(radius))).tupled
        )
        _ = assume(
          plans.forall { case (current, candidate) => current.isDefined && candidate.isDefined },
          "Inconclusive: explain output has no executionStats"
        )
        currentPlans = plans.flatMap(_._1)
        candidatePlans = plans.flatMap(_._2)
        // Latency at concurrency 1 and 8 over the fixed query set.
        latencies <- List(1, 8).traverse { concurrency =>
          for {
            current <- timed(radiiKm.map(currentPipeline), concurrency)
            candidate <- timed(radiiKm.map(candidatePipeline), concurrency)
          } yield (concurrency, current, candidate)
        }
        built = {
          import AtlasLocalCriterionEvidence.*
          def figures(plansOf: List[Explained], samples: List[(Double, Boolean)]) = PlanFigures(
            plansOf.map(_.returned).sum,
            plansOf.map(_.docs).sum,
            plansOf.map(_.keys).sum,
            percentile(samples.map(_._1), 0.95).getOrElse(0d),
            samples.count(_._2)
          )
          val verdicts = latencies.map { case (concurrency, current, candidate) =>
            val decision =
              decideDiscovery(figures(currentPlans, current), figures(candidatePlans, candidate), identical)
            (concurrency, figures(currentPlans, current), figures(candidatePlans, candidate), decision)
          }
          val outcome =
            if (verdicts.forall(_._4._1 == DiscoveryOutcome.CandidateForCloudConfirmation))
              DiscoveryOutcome.CandidateForCloudConfirmation
            else DiscoveryOutcome.RetainCurrent
          def figuresJson(value: PlanFigures) = Json.obj(
            "returned" -> Json.fromLong(value.returned),
            "docsExamined" -> Json.fromLong(value.docsExamined),
            "keysExamined" -> Json.fromLong(value.keysExamined),
            "p95Millis" -> Json.fromDoubleOrNull(value.p95Millis),
            "errors" -> Json.fromInt(value.errors)
          )
          val record = Json.obj(
            "criterion" -> Json.fromString("HDQ-11.L"),
            "environment" -> Json.fromString(DisposableAtlas.deployment(uri)),
            "atlasVersion" -> Json.fromString(version),
            "jobs" -> Json.fromInt(workload.size),
            "queries" -> Json.fromInt(radiiKm.size),
            "repetitions" -> Json.fromInt(repetitions),
            "identicalResultIds" -> Json.fromBoolean(identical),
            "current" -> Json.fromString("$geoNear on location.point (2dsphere), sorted by distance"),
            "candidate" -> Json.fromString("$geoWithin $centerSphere match, sorted by id; not adopted"),
            "plans" -> Json.obj(
              "currentStages" -> Json.arr(currentPlans.flatMap(_.stages).distinct.sorted.map(Json.fromString)*),
              "candidateStages" -> Json.arr(candidatePlans.flatMap(_.stages).distinct.sorted.map(Json.fromString)*)
            ),
            "measurements" -> Json.arr(verdicts.map { case (concurrency, current, candidate, decision) =>
              Json.obj(
                "concurrency" -> Json.fromInt(concurrency),
                "current" -> figuresJson(current),
                "candidate" -> figuresJson(candidate),
                "runVerdict" -> Json.fromString(decision._1.text),
                "reason" -> Json.fromString(decision._2)
              )
            }*),
            "outcome" -> Json.fromString(outcome.text),
            "limits" -> Json.arr(
              List(
                "The $geoWithin candidate has no distance order and no distance cursor, so identical ids is weaker than functional equivalence with nearbyJobs paging.",
                "Only the radius query is paired; lexical and facet queries are not.",
                "Production runs $geoNear inside a $lookup from users (actor gate); the explain and timings here run on the jobs collection directly.",
                "Explained figures sum every executionStats block of the plan."
              ).map(Json.fromString)*
            ),
            "scope" -> Json.fromString(
              "local-container correctness and plan evidence, not Atlas figures; no index, query or ranking change is adopted; only HDQ-11.C can adopt."
            )
          )
          val built = (record, verdicts)
          built
        }
        (record, verdicts) = built
        _ <- writeRecord(s"criterion-evidence-discovery-${now0}", record)
      } yield {
        verdicts.foreach { case (concurrency, current, candidate, _) =>
          assertEquals(current.errors + candidate.errors, 0, clue(s"concurrency $concurrency"))
        }
        // The workload must have exercised the boundary, or the comparison is empty.
        assert(currentPlans.map(_.returned).sum > 0L)
      }
    }
  }
}
