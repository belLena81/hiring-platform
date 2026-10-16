package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.events.{
  OperationalEventPayload,
  OperationalEvents,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionResult
}
import com.example.graphQL.cats.shared.crypto.SourceHash
import java.util.UUID
import munit.CatsEffectSuite

/** Lexical fallback when the query embedding provider or `$vectorSearch` is unavailable (AIR-05, AIR-06). */
final class SemanticSearchLexicalFallbackSpec extends CatsEffectSuite {
  private val model = "voyage-4-lite"
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val pageSize = PageSize.fromInt(10).toOption.get
  private val noFilter = JobSearchFilter(None, Set.empty, None)
  private val candidateActor = ActorContext(candidateId, UserRole.Candidate)
  private val recruiterActor = ActorContext(recruiterId, UserRole.Recruiter)
  private val providerDown = Left(EmbeddingError.ProviderUnavailable)
  private val providerUp = Right(EmbeddingVector(List(0.1f, 0.2f), model, 2))

  private def jobWithEmbedding(job: Job): Job =
    job.copy(embedding =
      Some(EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(job)), now)))
    )
  private def jobNumber(n: Int): JobId = JobId(new UUID(0L, 0x1000L + n))
  private def userNumber(n: Int): UserId = UserId(new UUID(0L, 0x2000L + n))
  private def jobIn(n: Int, city: String, status: JobStatus = JobStatus.Open): Job =
    openJob.copy(id = jobNumber(n), location = Location("Ukraine", Some(city), remote = false), status = status)

  private def candidateUser(
      n: Int,
      profile: CandidateProfile = CandidateProfile(Set("Scala"), Some("Backend engineer"), None),
      role: UserRole = UserRole.Candidate,
      status: AccountStatus = AccountStatus.Active
  ): User =
    candidate.copy(
      id = userNumber(n),
      name = s"Candidate $n",
      role = role,
      accountStatus = status,
      profile = Some(UserProfile.Candidate(profile))
    )

  /** In-memory search store: vector retrieval and lexical retrieval fail or answer as configured. */
  private final class FakeStore(
      users: Ref[IO, Map[UserId, User]],
      jobs: Ref[IO, Map[JobId, Job]],
      vector: Either[RepositoryError, Unit],
      lexical: Either[RepositoryError, Unit],
      lexicalJobIds: List[JobId],
      lexicalCandidateIds: List[UserId],
      afterLexicalRetrieval: IO[Unit],
      lexicalQueries: Ref[IO, Vector[LexicalSearchQuery]]
  ) extends SemanticSearchRepository[IO] {
    private def active(scope: HiringReadScope): IO[Option[User]] =
      users.get.map(_.get(scope.userId).filter(u => u.role == scope.role && u.accountStatus == AccountStatus.Active))

    def authorizedJobEligibility(
        scope: HiringReadScope,
        ids: List[JobId],
        queryCandidate: Option[CandidateSearchEligibility]
    ): RepositoryF[IO, List[JobSearchEligibility]] =
      RepositoryF.lift(
        for {
          actor <- active(scope)
          current <- jobs.get
        } yield
          if (actor.exists(_.role == UserRole.Candidate))
            ids.flatMap(current.get).map(JobSearchEligibility.fromJob)
          else Nil
      )

    def authorizedCandidateEligibility(
        scope: HiringReadScope,
        queryJob: JobSearchEligibility,
        ids: List[UserId]
    ): RepositoryF[IO, List[CandidateSearchEligibility]] =
      RepositoryF.lift(for {
        actor <- active(scope)
        currentJobs <- jobs.get
        currentUsers <- users.get
      } yield {
        val manages =
          actor.exists(a => a.role == UserRole.Admin || currentJobs.get(queryJob.job.id).exists(_.recruiterId == a.id))
        val open = currentJobs.get(queryJob.job.id).exists(_.status == JobStatus.Open)
        if (manages && open) ids.flatMap(currentUsers.get).map(CandidateSearchEligibility.fromUser) else Nil
      })

    def searchJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
      RepositoryF.fromEither[IO, List[JobRetrievalHit]](vector.as(Nil))
    def recommendedJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
      RepositoryF.at[IO].pure(Nil)
    def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] =
      RepositoryF.fromEither[IO, List[CandidateRetrievalHit]](vector.as(Nil))

    def lexicalJobs(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalJobHit]] =
      RepositoryF.fromFEither(
        lexicalQueries.update(_ :+ query) *> afterLexicalRetrieval.as(
          lexical.as(lexicalJobIds.zipWithIndex.map((id, i) => LexicalRetrievalHit(id, 10d - i, query.searchId)))
        )
      )
    def lexicalCandidates(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalCandidateHit]] =
      RepositoryF.fromFEither(
        lexicalQueries.update(_ :+ query) *> afterLexicalRetrieval.as(
          lexical.as(lexicalCandidateIds.zipWithIndex.map((id, i) => LexicalRetrievalHit(id, 10d - i, query.searchId)))
        )
      )
  }

  private final class Harness(
      val service: SemanticSearchService[IO],
      val embedCalls: Ref[IO, Int],
      val lexicalQueries: Ref[IO, Vector[LexicalSearchQuery]]
  )

  private def harness(
      users: List[User],
      jobs: List[Job],
      embedding: Either[EmbeddingError, EmbeddingVector] = providerDown,
      vector: Either[RepositoryError, Unit] = Right(()),
      lexical: Either[RepositoryError, Unit] = Right(()),
      lexicalJobIds: List[JobId] = Nil,
      lexicalCandidateIds: List[UserId] = Nil,
      afterLexicalRetrieval: Ref[IO, Map[JobId, Job]] => IO[Unit] = _ => IO.unit
  ): IO[Harness] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(u => u.id -> u).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](jobs.map(j => j.id -> j).toMap)
      calls <- Ref.of[IO, Int](0)
      queries <- Ref.of[IO, Vector[LexicalSearchQuery]](Vector.empty)
      embedder = new EmbeddingService[IO] {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          calls.update(_ + 1).as(embedding)
      }
      store = new FakeStore(
        usersRef,
        jobsRef,
        vector,
        lexical,
        lexicalJobIds,
        lexicalCandidateIds,
        afterLexicalRetrieval(jobsRef),
        queries
      )
    } yield Harness(
      SemanticSearchService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef), embedder, store, model),
      calls,
      queries
    )

  private val skilledCandidate = candidate.copy(profile =
    Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Backend engineer"), None)))
  )

  test("AIR-05.1 provider failure answers from lexical retrieval: only open jobs, filter honoured, degraded") {
    val kyiv = jobIn(1, "Kyiv")
    val lviv = jobIn(2, "Lviv")
    val closed = jobIn(3, "Kyiv", JobStatus.Closed)
    for {
      h <- harness(
        List(skilledCandidate),
        List(kyiv, lviv, closed),
        lexicalJobIds = List(kyiv.id, lviv.id, closed.id)
      )
      result <- h.service
        .semanticJobSearch(
          candidateActor,
          "scala backend",
          JobSearchFilter(Some("Kyiv"), Set.empty, None),
          pageSize,
          searchId
        )
        .value
      queries <- h.lexicalQueries.get
    } yield {
      val results = result.fold(error => fail(error.toString), identity)
      assertEquals(results.degraded, Some(DegradedSearch(SearchError.ProviderUnavailable)))
      assertEquals(results.hits.map(_.job.id), List(kyiv.id))
      assertEquals(results.hits.map(_.mode), List(SearchMode.LEXICAL))
      assertEquals(results.hits.map(_.model), List(None))
      assertEquals(results.hits.map(_.matchedSkills), List(List("Scala")))
      assertEquals(queries.map(_.text), Vector("scala backend"))
    }
  }

  test("AIR-05.1 vector search failure with a working provider is degraded as VectorSearchUnavailable") {
    val kyiv = jobIn(1, "Kyiv")
    for {
      h <- harness(
        List(skilledCandidate),
        List(kyiv),
        embedding = providerUp,
        vector = Left(RepositoryError.VectorSearchUnavailable),
        lexicalJobIds = List(kyiv.id)
      )
      result <- h.service
        .semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId)
        .value
    } yield {
      val results = result.fold(error => fail(error.toString), identity)
      assertEquals(results.degraded, Some(DegradedSearch(SearchError.VectorSearchUnavailable)))
      assertEquals(results.hits.map(_.job.id), List(kyiv.id))
    }
  }

  test("AIR-05.1 a healthy provider and vector search is not degraded and never reads the lexical index") {
    for {
      h <- harness(List(skilledCandidate), List(jobIn(1, "Kyiv")), embedding = providerUp)
      result <- h.service
        .semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId)
        .value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(result, Right(SearchResults[SearchedJob](Nil, None)))
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-05.3 a job closed between lexical retrieval and the live re-check is dropped") {
    val stays = jobIn(1, "Kyiv")
    val closes = jobIn(2, "Kyiv")
    for {
      h <- harness(
        List(skilledCandidate),
        List(stays, closes),
        lexicalJobIds = List(closes.id, stays.id),
        afterLexicalRetrieval = jobs => jobs.update(_.updated(closes.id, closes.copy(status = JobStatus.Closed)))
      )
      result <- h.service
        .semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId)
        .value
    } yield assertEquals(result.map(_.hits.map(_.job.id)), Right(List(stays.id)))
  }

  test("AIR-06.1 lexical retrieval also unavailable returns VectorSearchUnavailable") {
    for {
      h <- harness(List(skilledCandidate), List(jobIn(1, "Kyiv")), lexical = Left(RepositoryError.Unavailable))
      result <- h.service
        .semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId)
        .value
    } yield assertEquals(result, Left(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
  }

  test("AIR-06.1 a blank query has no lexical fallback and keeps the original provider failure") {
    for {
      h <- harness(List(skilledCandidate), List(jobIn(1, "Kyiv")))
      result <- h.service
        .semanticJobSearch(candidateActor, "   ", noFilter, pageSize, searchId)
        .value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(result, Left(UseCaseError.Search(SearchError.ProviderUnavailable)))
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-05.2 unauthorized callers get no provider call and no fallback") {
    val kyiv = jobIn(1, "Kyiv")
    for {
      h <- harness(List(skilledCandidate, recruiter), List(kyiv), lexicalJobIds = List(kyiv.id))
      recruiterJobs <- h.service
        .semanticJobSearch(recruiterActor, "scala", noFilter, pageSize, searchId)
        .value
      tooLong <- h.service
        .semanticJobSearch(
          candidateActor,
          "x" * (SearchableText.QueryMaxChars + 1),
          noFilter,
          pageSize,
          searchId
        )
        .value
      candidateMatches <- h.service
        .candidateMatches(
          candidateActor,
          kyiv.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      calls <- h.embedCalls.get
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(recruiterJobs.left.toOption, Some(UseCaseError.Domain(DomainError.CandidateRequired)))
      assertEquals(
        tooLong.left.toOption,
        Some(UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)))
      )
      assertEquals(candidateMatches.left.toOption, Some(UseCaseError.Domain(DomainError.RecruiterRequired)))
      assertEquals(calls, 0)
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-05.2 a recruiter who does not own the job or a closed job gets no provider call and no fallback") {
    val other = recruiter.copy(id = userNumber(90))
    val owned = jobWithEmbedding(jobIn(1, "Kyiv"))
    val closed = jobWithEmbedding(jobIn(2, "Kyiv", JobStatus.Closed))
    for {
      h <- harness(List(other, recruiter), List(owned, closed), lexicalCandidateIds = List(candidateId))
      notOwner <- h.service
        .candidateMatches(
          ActorContext(other.id, UserRole.Recruiter),
          owned.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      onClosed <- h.service
        .candidateMatches(
          recruiterActor,
          closed.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      calls <- h.embedCalls.get
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(notOwner.left.toOption, Some(UseCaseError.Domain(DomainError.Forbidden)))
      assertEquals(onClosed.left.toOption, Some(UseCaseError.Domain(DomainError.JobMustBeOpen)))
      assertEquals(calls, 0)
      assertEquals(queries, Vector.empty)
    }
  }

  private val cyprus = CandidateResidence("Cyprus", Some("Nicosia"))
  private val sweden = CandidateResidence("Sweden", Some("Malmo"))
  private def profileIn(residence: CandidateResidence, optIn: Boolean) =
    CandidateProfile(Set("Scala"), Some("Backend engineer"), None, Some(residence), None, optIn)

  test("AIR-05.2 candidate fallback excludes deleted and non-Candidate users and honours consent-gated filters") {
    val job = jobWithEmbedding(jobIn(1, "Kyiv"))
    val matching = candidateUser(1, profileIn(cyprus, optIn = true))
    val elsewhere = candidateUser(2, profileIn(sweden, optIn = true))
    val deleted = candidateUser(3, profileIn(cyprus, optIn = true), status = AccountStatus.Deleted)
    val notCandidate = candidateUser(4, profileIn(cyprus, optIn = true), role = UserRole.Recruiter)
    val optedOut = candidateUser(5, profileIn(sweden, optIn = false))
    val ids = List(matching, elsewhere, deleted, notCandidate, optedOut).map(_.id)
    for {
      h <- harness(
        List(recruiter, matching, elsewhere, deleted, notCandidate, optedOut),
        List(job),
        lexicalCandidateIds = ids
      )
      result <- h.service
        .candidateMatches(
          recruiterActor,
          job.id,
          Some("scala"),
          CandidateMatchFilters(Nil, Some("Cyprus"), None, None),
          pageSize,
          searchId
        )
        .value
    } yield {
      val results = result.fold(error => fail(error.toString), identity)
      assertEquals(results.degraded, Some(DegradedSearch(SearchError.ProviderUnavailable)))
      // Private filters apply only to candidates who opted in; an opted-out profile keeps normal inclusion and is
      // never matched on its private residence, exactly as in the vector path.
      assertEquals(results.hits.map(_.candidate.id), List(matching.id, optedOut.id))
      assertEquals(results.hits.map(_.mode).distinct, List(SearchMode.LEXICAL))
      assertEquals(results.hits.map(_.model).distinct, List(None))
    }
  }

  test("AIR-05.2 candidate fallback derives bounded lexical text from the job skills when no query text is given") {
    val job = jobWithEmbedding(jobIn(1, "Kyiv").copy(skills = Set("Scala", "Cats")))
    val matching = candidateUser(1)
    for {
      h <- harness(
        List(recruiter, matching),
        List(job),
        vector = Left(RepositoryError.VectorSearchUnavailable),
        lexicalCandidateIds = List(matching.id)
      )
      result <- h.service
        .candidateMatches(
          recruiterActor,
          job.id,
          None,
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(result.map(_.hits.map(_.candidate.id)), Right(List(matching.id)))
      assertEquals(queries.map(_.text), Vector("Cats Scala"))
      assert(queries.forall(_.text.length <= SearchableText.QueryMaxChars))
    }
  }

  test("AIR-06.1 candidate fallback without query text and without job skills returns the original failure") {
    val job = jobWithEmbedding(jobIn(1, "Kyiv").copy(skills = Set.empty))
    for {
      h <- harness(List(recruiter), List(job), vector = Left(RepositoryError.Unavailable), embedding = providerUp)
      result <- h.service
        .candidateMatches(
          recruiterActor,
          job.id,
          None,
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-06.1 candidate fallback with lexical retrieval also unavailable returns VectorSearchUnavailable") {
    val job = jobWithEmbedding(jobIn(1, "Kyiv"))
    for {
      h <- harness(List(recruiter), List(job), lexical = Left(RepositoryError.Unavailable))
      result <- h.service
        .candidateMatches(
          recruiterActor,
          job.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
    } yield assertEquals(result.left.toOption, Some(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
  }

  test("AIR-01.2 a missing or stale job embedding is not degradable and never falls back") {
    val missing = jobIn(1, "Kyiv")
    val stale = jobWithEmbedding(jobIn(2, "Kyiv")).copy(description = "changed after embedding")
    for {
      h <- harness(List(recruiter), List(missing, stale))
      absent <- h.service
        .candidateMatches(
          recruiterActor,
          missing.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      old <- h.service
        .candidateMatches(
          recruiterActor,
          stale.id,
          Some("scala"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      calls <- h.embedCalls.get
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(absent.left.toOption, Some(UseCaseError.Search(SearchError.MissingEmbedding("job"))))
      assertEquals(old.left.toOption, Some(UseCaseError.Search(SearchError.StaleEmbedding("job"))))
      assertEquals(calls, 0)
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-05.4 a vector query cut by its time budget or refused by admission never falls back") {
    val kyiv = jobIn(1, "Kyiv")
    val owned = jobWithEmbedding(jobIn(2, "Kyiv"))
    for {
      h <- harness(
        List(skilledCandidate, recruiter),
        List(kyiv, owned),
        embedding = providerUp,
        vector = Left(RepositoryError.Overloaded),
        lexicalJobIds = List(kyiv.id)
      )
      jobs <- h.service.semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId).value
      candidates <- h.service
        .candidateMatches(recruiterActor, owned.id, Some("scala"), CandidateMatchFilters.empty, pageSize, searchId)
        .value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(jobs.left.toOption, Some(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      assertEquals(candidates.left.toOption, Some(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-05.4 a whole-store outage reported as plain Unavailable does not fall back either") {
    val kyiv = jobIn(1, "Kyiv")
    for {
      h <- harness(
        List(skilledCandidate),
        List(kyiv),
        embedding = providerUp,
        vector = Left(RepositoryError.Unavailable),
        lexicalJobIds = List(kyiv.id)
      )
      jobs <- h.service.semanticJobSearch(candidateActor, "scala", noFilter, pageSize, searchId).value
      queries <- h.lexicalQueries.get
    } yield {
      assertEquals(jobs.left.toOption, Some(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      assertEquals(queries, Vector.empty)
    }
  }

  test("AIR-06.2 a fallback session with model None and results is a valid search event with the same payload") {
    val results = List(SearchSessionResult(jobNumber(1).value.toString, 1, 9.5d))
    def session(model: Option[String]) = SearchSession(
      searchId,
      candidateId,
      OperationalEventPayload.SearchKind.SemanticJobSearch,
      None,
      SearchFilterSnapshot.Unfiltered,
      model,
      results,
      now,
      now.plusSeconds(60)
    )
    val fallback = OperationalEvents.searchPerformed(UUID.randomUUID(), session(None))
    val vector = OperationalEvents.searchPerformed(fallback.toOption.get.eventId, session(Some(model)))
    assert(fallback.isRight)
    assertEquals(fallback.map(_.payload), vector.map(_.payload))
  }
}
