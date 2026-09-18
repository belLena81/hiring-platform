package com.example.graphQL.cats.service.search

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.PageSize
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import munit.CatsEffectSuite
import java.util.UUID

final class SemanticSearchServiceSpec extends CatsEffectSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume://candidate"))
  private val candidateWithProfile = candidate.copy(profile = Some(UserProfile.Candidate(profile)))
  private val meta = EmbeddingMeta("voyage-4-lite", 1, SourceHash.sha256(SearchableText.candidate(profile)), now)
  private val jobMeta = EmbeddingMeta("voyage-4-lite", 1, SourceHash.sha256(SearchableText.job(openJob)), now)
  private val embedding = EntityEmbedding(List(0.1f, 0.2f), meta)
  private val jobEmbedding = EntityEmbedding(List(0.1f, 0.2f), jobMeta)
  private val pageSize = PageSize.fromInt(5).toOption.get
  private val configuredModel = "voyage-4-lite"

  test("VHS-AC02 semantic job search requires a candidate actor and returns ranked open jobs") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile, recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> openJob.copy(embedding = Some(jobEmbedding))))
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2))),
        FakeSearchRepository(jobs = List(RankedJob(openJob.copy(embedding = Some(jobEmbedding)), 0.95, SearchMode.HYBRID, jobMeta, searchId))))
      accepted <- service.semanticJobSearch(ActorContext(candidateId, UserRole.Candidate), "scala backend",
        JobSearchFilter(None, Set.empty, None), pageSize, searchId)
      rejected <- service.semanticJobSearch(ActorContext(recruiterId, UserRole.Recruiter), "scala backend",
        JobSearchFilter(None, Set.empty, None), pageSize, searchId)
    } yield {
      assertEquals(accepted.map(_.map(_.job.id)), Right(List(jobId)))
      assertEquals(rejected.left.toOption, Some(UseCaseError.domain(DomainError.CandidateRequired)))
    }
  }

  test("VHS-AC02 semantic job search filters with the configured model when provider returns a canonical model") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      queries <- Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty)
      service = semanticService(
        new InMemoryUsers(usersRef),
        new InMemoryJobs(jobsRef),
        FakeEmbeddingService(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite-2026-09", 2))),
        RecordingSearchRepository(queries)
      )
      result <- service.semanticJobSearch(ActorContext(candidateId, UserRole.Candidate), "scala backend",
        JobSearchFilter(None, Set.empty, None), pageSize, searchId)
      recorded <- queries.get
    } yield {
      assertEquals(result, Right(Nil))
      assertEquals(recorded.map(_.model), Vector(configuredModel))
      assertEquals(recorded.map(_.mode), Vector(SearchMode.HYBRID))
      assertEquals(recorded.map(_.lexicalQuery), Vector(Some("scala backend")))
    }
  }

  test("VHS-AC02 semantic job search resolves stored actor before calling provider") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      calls <- Ref.of[IO, Int](0)
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef),
        CountingEmbeddingService(calls),
        FakeSearchRepository())
      result <- service.semanticJobSearch(ActorContext(recruiterId, UserRole.Candidate), "scala backend",
        JobSearchFilter(None, Set.empty, None), pageSize, searchId)
      callCount <- calls.get
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.domain(DomainError.Forbidden)))
      assertEquals(callCount, 0)
    }
  }

  test("VHS-AC08 oversized semantic query does not call provider") {
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      calls <- Ref.of[IO, Int](0)
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef),
        CountingEmbeddingService(calls),
        FakeSearchRepository())
      result <- service.semanticJobSearch(ActorContext(candidateId, UserRole.Candidate), "x" * (SearchableText.QueryMaxChars + 1),
        JobSearchFilter(None, Set.empty, None), pageSize, searchId)
      callCount <- calls.get
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars))))
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
      staleUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile.copy(embedding = Some(stale))))
      staleModelUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile.copy(embedding = Some(staleModel))))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      missingService = semanticService(new InMemoryUsers(missingUsers), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused, FakeSearchRepository())
      staleService = semanticService(new InMemoryUsers(staleUsers), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused, FakeSearchRepository())
      staleModelService = semanticService(new InMemoryUsers(staleModelUsers), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused, FakeSearchRepository())
      missing <- missingService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
      staleResult <- staleService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
      staleModelResult <- staleModelService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
    } yield {
      assertEquals(missing.left.toOption, Some(UseCaseError.search(SearchError.MissingEmbedding("candidate"))))
      assertEquals(staleResult.left.toOption, Some(UseCaseError.search(SearchError.StaleEmbedding("candidate"))))
      assertEquals(staleModelResult.left.toOption, Some(UseCaseError.search(SearchError.StaleEmbedding("candidate"))))
    }
  }

  test("VHS-AC04 candidate matching requires recruiter ownership before exposing candidates") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val otherRecruiter = Identifiers.UserId(UUID.fromString("00000000-0000-0000-0000-000000000088"))
    val otherRecruiterUser = recruiter.copy(id = otherRecruiter, email = Some("other-recruiter@example.com"))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(
        candidateId -> candidateWithProfile.copy(embedding = Some(embedding)),
        recruiterId -> recruiter,
        otherRecruiter -> otherRecruiterUser
      ))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> owned))
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused,
        FakeSearchRepository(candidates = List(RankedCandidate(candidateWithProfile.copy(embedding = Some(embedding)), 0.90, SearchMode.VECTOR, meta, searchId))))
      accepted <- service.candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
      rejected <- service.candidateMatches(ActorContext(otherRecruiter, UserRole.Recruiter), jobId, pageSize, searchId)
    } yield {
      assertEquals(accepted.map(_.map(_.candidate.id)), Right(List(candidateId)))
      assertEquals(rejected.left.toOption, Some(UseCaseError.domain(DomainError.Forbidden)))
    }
  }

  test("VHS-AC06 candidate matching reports same-version stale job embeddings by source hash or model") {
    val staleJob = openJob.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(sourceHash = "stale-job-hash"))))
    val staleModelJob = openJob.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(model = "voyage-4-lite-2026-09"))))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> staleJob))
      staleModelJobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> staleModelJob))
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused,
        FakeSearchRepository())
      staleModelService = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(staleModelJobsRef), FakeEmbeddingService.unused,
        FakeSearchRepository())
      result <- service.candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
      staleModelResult <- staleModelService.candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
    } yield {
      assertEquals(result.left.toOption, Some(UseCaseError.search(SearchError.StaleEmbedding("job"))))
      assertEquals(staleModelResult.left.toOption, Some(UseCaseError.search(SearchError.StaleEmbedding("job"))))
    }
  }

  private def semanticService(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      embeddings: EmbeddingService[IO],
      search: SemanticSearchRepository[IO]
  ): SemanticSearchService[IO] =
    SemanticSearchService[IO](users, jobs, embeddings, search, embeddingModel = configuredModel, embeddingVersion = 1)

  private final case class FakeEmbeddingService(result: Either[EmbeddingError, EmbeddingVector]) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(result)
  }

  private object FakeEmbeddingService {
    val unused: FakeEmbeddingService = FakeEmbeddingService(Left(EmbeddingError.ProviderUnavailable))
  }

  private final case class CountingEmbeddingService(calls: Ref[IO, Int]) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      calls.update(_ + 1).as(Right(EmbeddingVector(List(0.1f, 0.2f), "voyage-4-lite", 2)))
  }

  private final case class FakeSearchRepository(
      jobs: List[RankedJob] = Nil,
      candidates: List[RankedCandidate] = Nil
  ) extends SemanticSearchRepository[IO] {
    override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      IO.pure(Right(jobs))

    override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      IO.pure(Right(jobs))

    override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] =
      IO.pure(Right(candidates))
  }

  private final case class RecordingSearchRepository(
      queries: Ref[IO, Vector[VectorSearchQuery]]
  ) extends SemanticSearchRepository[IO] {
    override def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      queries.update(_ :+ query).as(Right(Nil))

    override def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]] =
      queries.update(_ :+ query).as(Right(Nil))

    override def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]] =
      queries.update(_ :+ query).as(Right(Nil))
  }
}
