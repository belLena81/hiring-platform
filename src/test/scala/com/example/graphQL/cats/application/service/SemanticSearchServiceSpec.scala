package com.example.graphQL.cats.application.service

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.application.{ActorContext, SearchError}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.application.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import munit.CatsEffectSuite
import java.util.UUID

final class SemanticSearchServiceSpec extends CatsEffectSuite {
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val profile = CandidateProfile(Set("Scala"), Some("Backend engineer"), Some("resume://candidate"))
  private val candidateWithProfile = candidate.copy(profile = Some(profile))
  private val meta = EmbeddingMeta("voyage-4-lite", 1, SourceHash.sha256(SearchableText.candidate(profile)), now)
  private val jobMeta = EmbeddingMeta("voyage-4-lite", 1, SourceHash.sha256(SearchableText.job(openJob)), now)
  private val embedding = EntityEmbedding(List(0.1f, 0.2f), meta)
  private val jobEmbedding = EntityEmbedding(List(0.1f, 0.2f), jobMeta)
  private val pageSize = PageSize.fromInt(5).toOption.get

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
      assertEquals(rejected.left.toOption, Some(DomainError.CandidateRequired))
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
      assertEquals(result.left.toOption, Some(DomainError.Forbidden))
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
      assertEquals(result.left.toOption, Some(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)))
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
    for {
      missingUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile))
      staleUsers <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(candidateId -> candidateWithProfile.copy(embedding = Some(stale))))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map.empty)
      missingService = semanticService(new InMemoryUsers(missingUsers), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused, FakeSearchRepository())
      staleService = semanticService(new InMemoryUsers(staleUsers), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused, FakeSearchRepository())
      missing <- missingService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
      staleResult <- staleService.recommendedJobs(ActorContext(candidateId, UserRole.Candidate), pageSize, searchId)
    } yield {
      assertEquals(missing.left.toOption, Some(SearchError.MissingEmbedding("candidate")))
      assertEquals(staleResult.left.toOption, Some(SearchError.StaleEmbedding("candidate")))
    }
  }

  test("VHS-AC04 candidate matching requires recruiter ownership before exposing candidates") {
    val owned = openJob.copy(embedding = Some(jobEmbedding))
    val otherRecruiter = Identifiers.UserId(UUID.fromString("00000000-0000-0000-0000-000000000088"))
    val otherRecruiterUser = recruiter.copy(id = otherRecruiter, email = "other-recruiter@example.com")
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
      assertEquals(rejected.left.toOption, Some(DomainError.Forbidden))
    }
  }

  test("VHS-AC06 candidate matching reports same-version stale job embeddings by source hash") {
    val staleJob = openJob.copy(embedding = Some(jobEmbedding.copy(meta = jobMeta.copy(sourceHash = "stale-job-hash"))))
    for {
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](Map(recruiterId -> recruiter))
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](Map(jobId -> staleJob))
      service = semanticService(new InMemoryUsers(usersRef), new InMemoryJobs(jobsRef), FakeEmbeddingService.unused,
        FakeSearchRepository())
      result <- service.candidateMatches(ActorContext(recruiterId, UserRole.Recruiter), jobId, pageSize, searchId)
    } yield assertEquals(result.left.toOption, Some(SearchError.StaleEmbedding("job")))
  }

  private def semanticService(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      embeddings: EmbeddingService[IO],
      search: SemanticSearchRepository[IO]
  ): SemanticSearchService[IO] =
    SemanticSearchService[IO](users, jobs, embeddings, search, embeddingVersion = 1)

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
}
