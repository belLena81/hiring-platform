package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.AccountValueFixtures.email
import cats.effect.IO
import cats.effect.{Ref, Deferred}
import cats.syntax.all.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.{ActorContext, AuthenticatedActor, SearchError, UseCaseError}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.search.{
  CandidateMatchFilters,
  CandidateSearchHit,
  JobSearchFilter,
  RankedCandidate,
  RankedJob,
  VectorSearchQuery
}
import munit.CatsEffectSuite
import java.util.UUID

final class SemanticSearchServiceSpec extends CatsEffectSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume://candidate"))
  private val candidateWithProfile = candidate.copy(profile = Some(UserProfile.Candidate(profile)))
  private val meta = EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.candidate(profile)), now)
  private val jobMeta = EmbeddingMeta("voyage-4-lite", SourceHash.sha256(SearchableText.job(openJob)), now)
  private val embedding = EntityEmbedding(List(0.1f, 0.2f), meta)
  private val jobEmbedding = EntityEmbedding(List(0.1f, 0.2f), jobMeta)
  private val pageSize = PageSize.fromInt(5).toOption.get
  private val configuredModel = "voyage-4-lite"

  test("VHS-AC02 semantic job search requires a candidate actor and returns ranked open jobs") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(candidateId -> candidateWithProfile, recruiterId -> recruiter)
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))),
        FakeSearchRepository(jobs =
          List(RankedJob(openJob.copy(embedding = Some(jobEmbedding)), 0.95, SearchMode.HYBRID, jobMeta, searchId))
        )
      )
      accepted <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
      rejected <- service
        .semanticJobSearch(
          ActorContext(recruiterId, UserRole.Recruiter),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
    } yield {
      assertEquals(accepted.map(_.map(_.job.id)), Right(List(jobId)))
      assertEquals(rejected.left.toOption, Some(UseCaseError.Domain(DomainError.CandidateRequired)))
    }
  }

  test("recruiter candidate matching normalizes direct service filters before retrieval") {
    val filters =
      CandidateMatchFilters(
        List(" Scala ", "MongoDB", "SCALA"),
        Some(" CYPRUS "),
        Some(" Nicosia "),
        Some("AVAILABLE_NOW")
      )
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(recruiterId -> recruiter, candidateId -> candidateWithProfile.copy(embedding = Some(embedding)))
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
      queries <- Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.3f, 0.4f), configuredModel, 2))),
        RecordingSearchRepository(queries)
      )
      result <- service
        .candidateMatches(
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          Some("  Scala backend  "),
          filters,
          pageSize,
          searchId
        )
        .value
      recorded <- queries.get
    } yield {
      assertEquals(result, Right(Nil))
      assertEquals(recorded.map(_.lexicalQuery), Vector(Some("Scala backend")))
      assertEquals(recorded.map(_.candidateQueryVector), Vector(Some(List(0.3f, 0.4f))))
      assertEquals(recorded.map(_.candidateFilters.requiredSkills), Vector(List("mongodb", "scala")))
      assertEquals(recorded.map(_.candidateFilters.countryCanonical), Vector(Some("cyprus")))
      assertEquals(recorded.map(_.candidateFilters.cityCanonical), Vector(Some("nicosia")))
    }
  }

  test("a mismatched provider model cannot reach job or candidate retrieval") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(candidateId -> candidateWithProfile, recruiterId -> recruiter)
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
      queries <- Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite-2026-09", 2))),
        RecordingSearchRepository(queries)
      )
      result <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
      recorded <- queries.get
      candidateResult <- service
        .candidateMatches(
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          Some("scala backend"),
          CandidateMatchFilters.empty,
          pageSize,
          searchId
        )
        .value
      candidateQueries <- queries.get
    } yield {
      assertEquals(result, Left(UseCaseError.Search(SearchError.ProviderUnavailable)))
      assertEquals(candidateResult, Left(UseCaseError.Search(SearchError.ProviderUnavailable)))
      assertEquals(recorded, Vector.empty)
      assertEquals(candidateQueries, Vector.empty)
    }
  }

  test("VHS-AC02 semantic job search resolves stored actor before calling provider") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      calls <- Ref.of[IO, Int](0)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        CountingEmbeddingService(calls),
        FakeSearchRepository()
      )
      result <- service
        .semanticJobSearch(
          ActorContext(recruiterId, UserRole.Candidate),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
      callCount <- calls.get
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.Domain(DomainError.Forbidden)))
      assertEquals(callCount, 0)
    }
  }

  test("VHS-AC08 oversized semantic query does not call provider") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      calls <- Ref.of[IO, Int](0)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        CountingEmbeddingService(calls),
        FakeSearchRepository()
      )
      result <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "x" * (SearchableText.QueryMaxChars + 1),
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
      callCount <- calls.get
    } yield {
      assertEquals(
        result.left.toOption,
        Some(UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)))
      )
      assertEquals(callCount, 0)
    }
  }

  test("VHS-AC08 candidate searchable text excludes resume references sent to providers") {
    val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume://secret-ref"))
    val text = SearchableText.candidate(profile)
    assert(text.contains("Backend engineer"))
    assert(text.contains("Scala"))
    assert(!text.contains("resume://secret-ref"))
  }

  test("VHS-AC03 recommended jobs report missing and stale candidate embeddings") {
    val stale = embedding.copy(meta = meta.copy(sourceHash = "stale-hash"))
    val staleModel = embedding.copy(meta = meta.copy(model = "voyage-4-lite-2026-09"))
    for {
      missingUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      staleUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(candidateId -> candidateWithProfile.copy(embedding = Some(stale)))
      )
      staleModelUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(candidateId -> candidateWithProfile.copy(embedding = Some(staleModel)))
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      missingService = semanticService(
        new InMemoryUsers(missingUsers),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository()
      )
      staleService = semanticService(
        new InMemoryUsers(staleUsers),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository()
      )
      staleModelService = semanticService(
        new InMemoryUsers(staleModelUsers),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository()
      )
      missing <- missingService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId).value
      staleResult <- staleService
        .recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
        .value
      staleModelResult <- staleModelService
        .recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
        .value
    } yield {
      assertEquals(missing.left.toOption, Some(UseCaseError.Search(SearchError.MissingEmbedding("candidate"))))
      assertEquals(staleResult.left.toOption, Some(UseCaseError.Search(SearchError.StaleEmbedding("candidate"))))
      assertEquals(staleModelResult.left.toOption, Some(UseCaseError.Search(SearchError.StaleEmbedding("candidate"))))
    }
  }

  test("VHS-AC04 candidate matching requires recruiter ownership before exposing candidates") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val otherRecruiter = Identifiers.UserId(UUID.fromString("00000000-0000-0000-0000-000000000088"))
    val otherRecruiterUser = recruiter.copy(id = otherRecruiter, email = Some(email("other-recruiter@example.com")))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(
          candidateId -> candidateWithProfile.copy(embedding = Some(embedding)),
          recruiterId -> recruiter,
          otherRecruiter -> otherRecruiterUser
        )
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository(candidates =
          List(
            RankedCandidate(
              com.example.graphQL.cats.service.search.CandidateSearchHit(
                candidateWithProfile.id,
                candidateWithProfile.name,
                candidateWithProfile.candidateProfile.get.skills,
                candidateWithProfile.candidateProfile.get.experienceSummary
              ),
              0.90,
              SearchMode.VECTOR,
              meta,
              searchId
            )
          )
        )
      )
      accepted <- service
        .candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
        .value
      rejected <- service
        .candidateMatches(ActorContext(otherRecruiter, UserRole.Recruiter), jobId, pageSize, searchId)
        .value
    } yield {
      assertEquals(accepted.map(_.map(_.candidate.id)), Right(List(candidateId)))
      assertEquals(rejected.left.toOption, Some(UseCaseError.Domain(DomainError.Forbidden)))
    }
  }

  test("forbidden actors, unowned jobs and closed jobs reject query matching before provider or retrieval work") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val unowned = owned.copy(recruiterId = candidateId)
    val scenarios: List[(ActorContext, Job, DomainError)] = List(
      (ActorContext(candidateId, UserRole.Candidate), owned, DomainError.RecruiterRequired),
      (ActorContext(recruiterId, UserRole.Recruiter), unowned, DomainError.Forbidden),
      (ActorContext(recruiterId, UserRole.Recruiter), owned.copy(status = JobStatus.Closed), DomainError.JobMustBeOpen)
    )
    scenarios.traverse_ { case (actor, job, expected) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
          Map(recruiterId -> recruiter, candidateId -> candidateWithProfile.copy(embedding = Some(embedding)))
        )
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> job))
        providerCalls <- Ref.of[IO, Int](0)
        retrievalQueries <- Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty)
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          CountingEmbeddingService(providerCalls),
          RecordingSearchRepository(retrievalQueries)
        )
        result <- service
          .candidateMatches(
            actor,
            jobId,
            Some("Scala backend"),
            CandidateMatchFilters(Nil, None, None, Some("invalid")),
            pageSize,
            searchId
          )
          .value
        calls <- providerCalls.get
        queries <- retrievalQueries.get
      } yield {
        assertEquals(result.left.toOption, Some(UseCaseError.Domain(expected)))
        assertEquals(calls, 0)
        assertEquals(queries, Vector.empty)
      }
    }
  }

  test("invalid candidate filters reject before provider or retrieval and report every violated field") {
    val scenarios = List(
      (
        CandidateMatchFilters(Nil, None, None, Some("invalid")),
        "Scala",
        SearchError.InvalidFilter("availabilityStatus")
      ),
      (
        CandidateMatchFilters(Nil, Some(" "), None, Some("invalid")),
        "Scala",
        SearchError.InvalidFilters(cats.data.NonEmptyList.of("residence", "availabilityStatus"))
      ),
      (
        CandidateMatchFilters(List(" "), Some(" "), None, Some("invalid")),
        "Scala",
        SearchError.InvalidFilters(cats.data.NonEmptyList.of("requiredSkills", "residence", "availabilityStatus"))
      ),
      (
        CandidateMatchFilters(Nil, None, None, Some("invalid")),
        "a" * (SearchableText.QueryMaxChars + 1),
        SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)
      )
    )
    scenarios.traverse_ { case (filters, query, expected) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
        providerCalls <- Ref.of[IO, Int](0)
        retrievalQueries <- Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty)
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          CountingEmbeddingService(providerCalls),
          RecordingSearchRepository(retrievalQueries)
        )
        result <- service
          .candidateMatches(
            ActorContext(recruiterId, UserRole.Recruiter),
            jobId,
            Some(query),
            filters,
            pageSize,
            searchId
          )
          .value
        calls <- providerCalls.get
        queries <- retrievalQueries.get
      } yield {
        assertEquals(result, Left(UseCaseError.Search(expected)))
        assertEquals(calls, 0)
        assertEquals(queries, Vector.empty)
      }
    }
  }

  test("candidate matching rechecks recruiter ownership and open status after retrieval") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val hit = RankedCandidate(
      CandidateSearchHit(candidateWithProfile.id, candidateWithProfile.name, profile.skills, profile.experienceSummary),
      0.9d,
      SearchMode.VECTOR,
      meta,
      searchId
    )
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(recruiterId -> recruiter, candidateId -> candidateWithProfile.copy(embedding = Some(embedding)))
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(
            jobsRef
              .update(_.updated(jobId, owned.copy(status = JobStatus.Closed)))
              .as(Right(List(hit).map(_.retrieval)))
          )
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      result <- service
        .candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
        .value
    } yield assertEquals(result.left.toOption, Some(UseCaseError.Domain(DomainError.JobMustBeOpen)))
  }

  test("candidate hit validation applies consent and private filters while preserving survivor order") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val country = CandidateResidence("Cyprus", Some("Nicosia"))
    val optedOut = profile.copy(currentResidence = None, availabilityStatus = None, recruiterSearchOptIn = false)
    val optedIn = profile.copy(
      currentResidence = Some(country),
      availabilityStatus = Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      recruiterSearchOptIn = true
    )
    val wrongCity = optedIn.copy(currentResidence = Some(CandidateResidence("Cyprus", Some("Limassol"))))
    val missingPrivateFields = optedIn.copy(currentResidence = None, availabilityStatus = None)
    def candidateUser(suffix: Int, value: CandidateProfile, status: AccountStatus = AccountStatus.Active): User = {
      val id = Identifiers.UserId(UUID.fromString(f"00000000-0000-0000-0000-${suffix}%012d"))
      val candidateEmbedding = EntityEmbedding(
        List(0.1f, 0.2f),
        EmbeddingMeta(configuredModel, SourceHash.sha256(SearchableText.candidate(value)), now)
      )
      candidateWithProfile.copy(
        id = id,
        name = s"Candidate $suffix",
        profile = Some(UserProfile.Candidate(value)),
        embedding = Some(candidateEmbedding),
        accountStatus = status
      )
    }
    val orderedUsers = List(
      candidateUser(21, wrongCity),
      candidateUser(22, optedOut),
      candidateUser(23, optedIn),
      candidateUser(24, profile),
      candidateUser(25, missingPrivateFields),
      candidateUser(26, optedIn, AccountStatus.Deleted),
      candidateUser(27, optedIn)
    )
    val rankedHits = orderedUsers.map { user =>
      val userEmbedding = user.embedding.getOrElse(fail("candidate fixture needs an embedding"))
      RankedCandidate(
        CandidateSearchHit(user.id, user.name, profile.skills, profile.experienceSummary),
        0.9d,
        SearchMode.VECTOR,
        userEmbedding.meta,
        searchId
      )
    }
    val filters = CandidateMatchFilters(
      requiredSkills = List("scala"),
      countryCanonical = Some("cyprus"),
      cityCanonical = Some("nicosia"),
      availabilityStatus = Some("AVAILABLE_NOW")
    )
    val requestedSize = PageSize.fromInt(7).toOption.getOrElse(fail("invalid candidate page size"))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        (recruiter +: orderedUsers).map(user => user.id -> user).toMap
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      staleSourceCandidate = orderedUsers.last
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(
            usersRef
              .update(
                _.updated(
                  staleSourceCandidate.id,
                  staleSourceCandidate.copy(
                    profile =
                      Some(UserProfile.Candidate(profile.copy(experienceSummary = Some("Changed profile summary"))))
                  )
                )
              )
              .as(Right(rankedHits.map(_.retrieval)))
          )
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      result <- service
        .candidateMatches(
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          None,
          filters,
          requestedSize,
          searchId
        )
        .value
    } yield assertEquals(
      result.map(_.map(_.candidate.id)),
      Right(List(orderedUsers(1).id, orderedUsers(2).id, orderedUsers(3).id))
    )
  }

  test("job search drops a result whose job closes after retrieval") {
    val embeddedJob = openJob.copy(embedding = Some(jobEmbedding))
    val hit = RankedJob(embeddedJob, 0.9d, SearchMode.HYBRID, jobMeta, searchId)
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> embeddedJob))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(
            jobsRef
              .update(_.updated(jobId, embeddedJob.copy(status = JobStatus.Closed)))
              .as(Right(List(hit).map(_.retrieval)))
          )
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2))),
        search
      )
      result <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
    } yield assertEquals(result.map(_.map(_.job.id)), Right(Nil))
  }

  test("job search considers lower ranked hits when a retrieved job is no longer eligible") {
    val firstJobId = Identifiers.JobId(UUID.fromString("00000000-0000-0000-0000-000000000031"))
    val secondJobId = Identifiers.JobId(UUID.fromString("00000000-0000-0000-0000-000000000032"))
    val firstJob = openJob.copy(id = firstJobId, embedding = Some(jobEmbedding))
    val secondJob = openJob.copy(id = secondJobId, embedding = Some(jobEmbedding))
    val firstHit = RankedJob(firstJob, 0.99d, SearchMode.HYBRID, jobMeta, searchId)
    val secondHit = RankedJob(secondJob, 0.9d, SearchMode.HYBRID, jobMeta, searchId)
    val requestedSize = PageSize.fromInt(1).toOption.getOrElse(fail("invalid job page size"))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(firstJobId -> firstJob, secondJobId -> secondJob))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(
            jobsRef
              .update(_.updated(firstJobId, firstJob.copy(status = JobStatus.Closed)))
              .as(Right(List(firstHit, secondHit).map(_.retrieval)))
          )
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2))),
        search
      )
      result <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "scala backend",
          JobSearchFilter(None, Set.empty, None),
          requestedSize,
          searchId
        )
        .value
    } yield assertEquals(result.map(_.map(_.job.id)), Right(List(secondJobId)))
  }

  test("candidate matching considers lower ranked candidates when the first hit fails current filters") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val ineligibleProfile = profile.copy(
      currentResidence = Some(CandidateResidence("Cyprus", Some("Limassol"))),
      availabilityStatus = Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      recruiterSearchOptIn = true
    )
    val eligibleProfile = profile.copy(
      currentResidence = Some(CandidateResidence("Cyprus", Some("Nicosia"))),
      availabilityStatus = Some(CandidateAvailabilityStatus.AVAILABLE_NOW),
      recruiterSearchOptIn = true
    )
    def candidateUser(suffix: Int, value: CandidateProfile): User = {
      val id = Identifiers.UserId(UUID.fromString(f"00000000-0000-0000-0000-${suffix}%012d"))
      val candidateEmbedding = EntityEmbedding(
        List(0.1f, 0.2f),
        EmbeddingMeta(configuredModel, SourceHash.sha256(SearchableText.candidate(value)), now)
      )
      candidateWithProfile.copy(
        id = id,
        name = s"Candidate $suffix",
        profile = Some(UserProfile.Candidate(value)),
        embedding = Some(candidateEmbedding)
      )
    }
    val ineligible = candidateUser(41, ineligibleProfile)
    val eligible = candidateUser(42, eligibleProfile)
    val nextEligible = candidateUser(43, eligibleProfile)
    def hit(user: User, score: Double): RankedCandidate =
      RankedCandidate(
        CandidateSearchHit(user.id, user.name, profile.skills, profile.experienceSummary),
        score,
        SearchMode.VECTOR,
        user.embedding.map(_.meta).getOrElse(fail("candidate fixture needs embedding metadata")),
        searchId
      )
    val rankedHits = List(hit(ineligible, 0.99d), hit(eligible, 0.9d), hit(nextEligible, 0.8d))
    val requestedSize = PageSize.fromInt(1).toOption.getOrElse(fail("invalid candidate page size"))
    val filters = CandidateMatchFilters(Nil, Some("cyprus"), Some("nicosia"), Some("AVAILABLE_NOW"))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        List(recruiter, ineligible, eligible, nextEligible).map(user => user.id -> user).toMap
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(rankedHits.map(_.retrieval))))
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      result <- service
        .candidateMatches(
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          None,
          filters,
          requestedSize,
          searchId
        )
        .value
    } yield assertEquals(result.map(_.map(_.candidate.id)), Right(List(eligible.id)))
  }

  test("search rechecks the persisted actor when the request carries a cached authenticated viewer") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val cachedRecruiter = new AuthenticatedActor(ActorContext(recruiterId, UserRole.Recruiter), recruiter)
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(
            usersRef
              .update(_.updated(recruiterId, recruiter.copy(accountStatus = AccountStatus.Deleted)))
              .as(Right(Nil))
          )
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      cachedResult <- service
        .candidateMatches(cachedRecruiter, jobId, pageSize, searchId)
        .value
    } yield {
      assertEquals(
        cachedResult.left.toOption,
        Some(UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized))
      )
    }
  }

  test("recommendations reject results when the query candidate profile changes during retrieval") {
    val embeddedJob = openJob.copy(embedding = Some(jobEmbedding))
    val hit = RankedJob(embeddedJob, 0.9d, SearchMode.VECTOR, jobMeta, searchId)
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
        Map(candidateId -> candidateWithProfile.copy(embedding = Some(embedding)))
      )
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> embeddedJob))
      search = new RetrievalSearchRepository {
        override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
        override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromIOEither(
            usersRef
              .update(
                _.updated(
                  candidateId,
                  candidateWithProfile.copy(
                    profile = Some(UserProfile.Candidate(profile.copy(skills = Set("Python")))),
                    embedding = Some(embedding)
                  )
                )
              )
              .as(Right(List(hit).map(_.retrieval)))
          )
        override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromIOEither(IO.pure(Right(Nil)))
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      result <- service.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId).value
    } yield assertEquals(
      result.left.toOption,
      Some(UseCaseError.Search(SearchError.StaleEmbedding("candidate")))
    )
  }

  test("VHS-AC06 candidate matching reports same-version stale job embeddings by source hash or model") {
    val staleJob = openJob.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(sourceHash = "stale-job-hash"))))
    val staleModelJob =
      openJob.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(model = "voyage-4-lite-2026-09"))))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> staleJob))
      staleModelJobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> staleModelJob))
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository()
      )
      staleModelService = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(staleModelJobsRef),
        FakeEmbeddingService.unused,
        FakeSearchRepository()
      )
      result <- service.candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId).value
      staleModelResult <- staleModelService
        .candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
        .value
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.Search(SearchError.StaleEmbedding("job"))))
      assertEquals(staleModelResult.left.toOption, Some(UseCaseError.Search(SearchError.StaleEmbedding("job"))))
    }
  }

  test("candidate validation observes synchronized deletion, private changes and embedding changes") {
    val original = candidateWithProfile.copy(embedding = Some(embedding))
    val mutations: List[(String, Map[Identifiers.UserId, User] => Map[Identifiers.UserId, User])] = List(
      "deletion" -> (_.removed(candidateId)),
      "consent" -> (_.updated(
        candidateId,
        original.copy(profile = Some(UserProfile.Candidate(profile.copy(recruiterSearchOptIn = true))))
      )),
      "private fields" -> (_.updated(
        candidateId,
        original.copy(profile =
          Some(
            UserProfile.Candidate(
              profile.copy(
                recruiterSearchOptIn = true,
                currentResidence = Some(CandidateResidence("Cyprus", Some("Limassol")))
              )
            )
          )
        )
      )),
      "source" -> (_.updated(
        candidateId,
        original.copy(profile = Some(UserProfile.Candidate(profile.copy(experienceSummary = Some("changed")))))
      )),
      "model" -> (_.updated(
        candidateId,
        original.copy(embedding = Some(embedding.copy(meta = meta.copy(model = "different"))))
      ))
    )
    mutations.traverse_ { case (label, mutate) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter, candidateId -> original))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        hit = RankedCandidate(
          CandidateSearchHit(candidateId, original.name, profile.skills, profile.experienceSummary),
          0.9,
          SearchMode.VECTOR,
          meta,
          searchId
        )
        search = new RetrievalSearchRepository {
          def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
            RepositoryIO.fromEither(Right(Nil))
          def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
            RepositoryIO.fromEither(Right(Nil))
          def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
            RepositoryIO.lift(entered.complete(()).void *> release.get.as(List(hit).map(_.retrieval)))
        }
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          FakeEmbeddingService.unused,
          search
        )
        fiber <- service
          .candidateMatches(
            ActorContext(recruiterId, UserRole.Recruiter),
            jobId,
            None,
            CandidateMatchFilters(Nil, Some("cyprus"), Some("nicosia"), None),
            pageSize,
            searchId
          )
          .value
          .start
        _ <- entered.get
        _ <- usersRef.update(mutate)
        _ <- release.complete(())
        result <- fiber.joinWithNever
      } yield assertEquals(result, Right(Nil), label)
    }
  }

  test("job validation observes synchronized deletion and source or model changes") {
    val original = openJob.copy(embedding = Some(jobEmbedding))
    val mutations: List[Map[Identifiers.JobId, Job] => Map[Identifiers.JobId, Job]] = List(
      _.removed(jobId),
      _.updated(jobId, original.copy(description = "changed source")),
      _.updated(jobId, original.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(model = "different")))))
    )
    mutations.traverse_ { mutate =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> original))
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        hit = RankedJob(original, 0.9, SearchMode.HYBRID, jobMeta, searchId)
        search = new RetrievalSearchRepository {
          def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
            RepositoryIO.lift(entered.complete(()).void *> release.get.as(List(hit).map(_.retrieval)))
          def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
            RepositoryIO.fromEither(Right(Nil))
          def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
            RepositoryIO.fromEither(Right(Nil))
        }
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2))),
          search
        )
        fiber <- service
          .semanticJobSearch(
            ActorContext(candidateId, UserRole.Candidate),
            "Scala",
            JobSearchFilter(None, Set.empty, None),
            pageSize,
            searchId
          )
          .value
          .start
        _ <- entered.get
        _ <- jobsRef.update(mutate)
        _ <- release.complete(())
        result <- fiber.joinWithNever
      } yield assertEquals(result, Right(Nil))
    }
  }

  test("candidate validation rechecks synchronized ownership reassignment") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      search = new RetrievalSearchRepository {
        def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromEither(Right(Nil))
        def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromEither(Right(Nil))
        def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.lift(entered.complete(()).void *> release.get.as(Nil))
      }
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService.unused,
        search
      )
      fiber <- service
        .candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
        .value
        .start
      _ <- entered.get
      _ <- jobsRef.update(_.updated(jobId, owned.copy(recruiterId = Identifiers.UserId(UUID.randomUUID()))))
      _ <- release.complete(())
      result <- fiber.joinWithNever
    } yield assertEquals(result, Left(UseCaseError.Domain(DomainError.Forbidden)))
  }

  test("authoritative eligibility failures are sanitized and canceled reads release their resources") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      entered <- Deferred[IO, Unit]
      released <- Ref.of[IO, Boolean](false)
      idsSeen <- Ref.of[IO, List[Identifiers.JobId]](Nil)
      hit = RankedJob(openJob.copy(embedding = Some(jobEmbedding)), 0.9, SearchMode.HYBRID, jobMeta, searchId)
      repository = new SemanticSearchRepository {
        def authorizedJobEligibility(
            scope: HiringReadScope,
            ids: List[Identifiers.JobId],
            expected: Option[CandidateSearchEligibility]
        ): RepositoryIO[List[JobSearchEligibility]] = jobEligibility(ids)
        def authorizedCandidateEligibility(
            scope: HiringReadScope,
            expected: JobSearchEligibility,
            ids: List[Identifiers.UserId]
        ): RepositoryIO[List[CandidateSearchEligibility]] = candidateEligibility(ids)

        def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromEither(Right(List(hit, hit).map(_.retrieval)))
        def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          RepositoryIO.fromEither(Right(Nil))
        def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          RepositoryIO.fromEither(Right(Nil))
        def candidateEligibility(ids: List[Identifiers.UserId]): RepositoryIO[List[CandidateSearchEligibility]] =
          RepositoryIO.fromEither(Right(Nil))
        def jobEligibility(ids: List[Identifiers.JobId]): RepositoryIO[List[JobSearchEligibility]] = RepositoryIO.lift(
          idsSeen.set(ids) *> (entered.complete(()).void *> IO.never[List[JobSearchEligibility]])
            .guarantee(released.set(true))
        )
      }
      users = new InMemoryUsers(usersRef)
      jobs = new InMemoryJobs(jobsRef)
      embedder = FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2)))
      service = SemanticSearchService(users, jobs, embedder, repository, configuredModel)
      fiber <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "Scala",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
        .start
      _ <- entered.get
      _ <- fiber.cancel
      cleanup <- released.get
      selected <- idsSeen.get
      failing = new SemanticSearchRepository {
        def authorizedJobEligibility(
            scope: HiringReadScope,
            ids: List[Identifiers.JobId],
            expected: Option[CandidateSearchEligibility]
        ): RepositoryIO[List[JobSearchEligibility]] = jobEligibility(ids)
        def authorizedCandidateEligibility(
            scope: HiringReadScope,
            expected: JobSearchEligibility,
            ids: List[Identifiers.UserId]
        ): RepositoryIO[List[CandidateSearchEligibility]] = candidateEligibility(ids)

        def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] = repository.searchJobs(query)
        def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          repository.recommendedJobs(query)
        def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          repository.candidateMatches(query)
        def candidateEligibility(ids: List[Identifiers.UserId]): RepositoryIO[List[CandidateSearchEligibility]] =
          RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
        def jobEligibility(ids: List[Identifiers.JobId]): RepositoryIO[List[JobSearchEligibility]] =
          RepositoryIO.fromEither(Left(RepositoryError.InvalidStoredData))
      }
      result <- SemanticSearchService(users, jobs, embedder, failing, configuredModel)
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "Scala",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
    } yield {
      assert(cleanup)
      assertEquals(selected, List(jobId))
      assertEquals(result, Left(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
    }
  }

  test("final candidate selection gates reassignment, deactivation and query changes after service prechecks") {
    val original = openJob.copy(embedding = Some(jobEmbedding))
    List("ownership", "actor", "source", "model", "closed").traverse_ { mutation =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](
          Map(recruiterId -> recruiter, candidateId -> candidateWithProfile.copy(embedding = Some(embedding)))
        )
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> original))
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        hit = RankedCandidate(
          CandidateSearchHit(candidateId, candidate.name, profile.skills, profile.experienceSummary),
          0.9,
          SearchMode.VECTOR,
          meta,
          searchId
        )
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          FakeEmbeddingService.unused,
          FakeSearchRepository(candidates = List(hit)),
          beforeCandidateSelection = entered.complete(()).void *> release.get
        )
        fiber <- service
          .candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
          .value
          .start
        _ <- entered.get
        _ <- mutation match {
          case "actor" =>
            usersRef.update(
              _.updated(recruiterId, recruiter.copy(accountStatus = AccountStatus.Deleted, profile = None))
            )
          case "ownership" =>
            jobsRef.update(_.updated(jobId, original.copy(recruiterId = Identifiers.UserId(UUID.randomUUID()))))
          case "source" => jobsRef.update(_.updated(jobId, original.copy(description = "Changed query source")))
          case "model"  =>
            jobsRef.update(
              _.updated(
                jobId,
                original.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(model = "different"))))
              )
            )
          case _ => jobsRef.update(_.updated(jobId, original.copy(status = JobStatus.Closed)))
        }
        _ <- release.complete(())
        result <- fiber.joinWithNever
      } yield assertEquals(result, Right(Nil), mutation)
    }
  }

  test("final job selection gates actor deactivation after service precheck") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      hit = RankedJob(openJob.copy(embedding = Some(jobEmbedding)), 0.9, SearchMode.HYBRID, jobMeta, searchId)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2))),
        FakeSearchRepository(jobs = List(hit)),
        beforeJobSelection = entered.complete(()).void *> release.get
      )
      fiber <- service
        .semanticJobSearch(
          ActorContext(candidateId, UserRole.Candidate),
          "Scala",
          JobSearchFilter(None, Set.empty, None),
          pageSize,
          searchId
        )
        .value
        .start
      _ <- entered.get
      _ <- usersRef.update(
        _.updated(candidateId, candidateWithProfile.copy(accountStatus = AccountStatus.Deleted, profile = None))
      )
      _ <- release.complete(())
      result <- fiber.joinWithNever
    } yield assertEquals(result, Right(Nil))
  }

  test(
    "search store failures keep revoked authority forbidden and conflicts conflicting; other faults are unavailable"
  ) {
    List(
      RepositoryError.AuthorityRevoked -> UseCaseError.Domain(DomainError.Forbidden),
      RepositoryError.Conflict -> UseCaseError.Repository(RepositoryError.Conflict),
      RepositoryError.Unavailable -> UseCaseError.Search(SearchError.VectorSearchUnavailable)
    ).traverse_ { case (failure, expected) =>
      for {
        usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
        jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob))
        service = semanticService(
          new InMemoryUsers(usersRef),
          new InMemoryJobs(jobsRef),
          FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), configuredModel, 2))),
          new RetrievalSearchRepository {
            def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
              RepositoryIO.fromEither(Left(failure))
            def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
              RepositoryIO.fromEither(Left(failure))
            def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
              RepositoryIO.fromEither(Left(failure))
          }
        )
        result <- service
          .semanticJobSearch(
            ActorContext(candidateId, UserRole.Candidate),
            "scala",
            JobSearchFilter(None, Set.empty, None),
            pageSize,
            searchId
          )
          .value
      } yield assertEquals(result, Left(expected))
    }
  }

  private def semanticService(
      users: UserRepository,
      jobs: JobRepository,
      embeddings: EmbeddingService,
      search: SemanticSearchRepository,
      beforeJobSelection: IO[Unit] = IO.unit,
      beforeCandidateSelection: IO[Unit] = IO.unit
  ): SemanticSearchService =
    SemanticSearchService(
      users,
      jobs,
      embeddings,
      new SemanticSearchRepository {
        private def persistedActor(scope: HiringReadScope): RepositoryIO[Option[User]] =
          users
            .find(scope.userId)
            .map(_.filter(user => user.role == scope.role && user.accountStatus == AccountStatus.Active))
        def authorizedJobEligibility(
            scope: HiringReadScope,
            ids: List[Identifiers.JobId],
            expected: Option[CandidateSearchEligibility]
        ): RepositoryIO[List[JobSearchEligibility]] =
          RepositoryIO.lift(beforeJobSelection).flatMap(_ => persistedActor(scope)).flatMap {
            case Some(user)
                if user.role == UserRole.Candidate && expected.forall(_ == CandidateSearchEligibility.fromUser(user)) =>
              jobEligibility(ids)
            case _ => RepositoryIO.fromEither(Right(Nil))
          }
        def authorizedCandidateEligibility(
            scope: HiringReadScope,
            expected: JobSearchEligibility,
            ids: List[Identifiers.UserId]
        ): RepositoryIO[List[CandidateSearchEligibility]] =
          RepositoryIO
            .lift(beforeCandidateSelection)
            .flatMap(_ =>
              (persistedActor(scope), jobs.find(expected.job.id))
                .mapN((actor, job) =>
                  actor.exists(user =>
                    user.role == UserRole.Admin || user.role == UserRole.Recruiter && job
                      .exists(_.recruiterId == user.id)
                  ) &&
                    job.exists(current =>
                      current.status == JobStatus.Open && JobSearchEligibility.fromJob(current) == expected
                    )
                )
                .flatMap(allowed => if (allowed) candidateEligibility(ids) else RepositoryIO.fromEither(Right(Nil)))
            )
        def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] = search.searchJobs(query)
        def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
          search.recommendedJobs(query)
        def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
          search.candidateMatches(query)
        def jobEligibility(ids: List[Identifiers.JobId]): RepositoryIO[List[JobSearchEligibility]] =
          jobs.findMany(ids).map(_.map(JobSearchEligibility.fromJob))
        def candidateEligibility(ids: List[Identifiers.UserId]): RepositoryIO[List[CandidateSearchEligibility]] =
          users.findMany(ids).map(_.map(CandidateSearchEligibility.fromUser))
      },
      embeddingModel = configuredModel
    )

  private trait RetrievalSearchRepository extends SemanticSearchRepository {
    def authorizedJobEligibility(
        scope: HiringReadScope,
        ids: List[Identifiers.JobId],
        expected: Option[CandidateSearchEligibility]
    ): RepositoryIO[List[JobSearchEligibility]] = RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    def authorizedCandidateEligibility(
        scope: HiringReadScope,
        expected: JobSearchEligibility,
        ids: List[Identifiers.UserId]
    ): RepositoryIO[List[CandidateSearchEligibility]] = RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    def jobEligibility(ids: List[Identifiers.JobId]): RepositoryIO[List[JobSearchEligibility]] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    def candidateEligibility(ids: List[Identifiers.UserId]): RepositoryIO[List[CandidateSearchEligibility]] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
  }

  private final case class FakeEmbeddingService(result: Either[EmbeddingError, EmbeddingVector])
      extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(result)
  }

  private object FakeEmbeddingService {
    val unused: FakeEmbeddingService = FakeEmbeddingService(Left(EmbeddingError.ProviderUnavailable))
  }

  private final case class CountingEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
  }

  private final case class FakeSearchRepository(
      jobs: List[RankedJob] = Nil,
      candidates: List[RankedCandidate] = Nil
  ) extends RetrievalSearchRepository {
    override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(jobs.map(_.retrieval))))

    override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(jobs.map(_.retrieval))))

    override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Right(candidates.map(_.retrieval))))
  }

  private final case class RecordingSearchRepository(
      queries: Ref[IO, Vector[VectorSearchQuery]]
  ) extends RetrievalSearchRepository {
    override def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(queries.update(_ :+ query).as(Right(Nil)))

    override def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(queries.update(_ :+ query).as(Right(Nil)))

    override def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] =
      com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(queries.update(_ :+ query).as(Right(Nil)))
  }
}
