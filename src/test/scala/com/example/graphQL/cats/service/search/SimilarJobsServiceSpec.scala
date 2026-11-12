package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.{ActorContext, AuthenticationError, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.shared.crypto.SourceHash
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite

/** GDF-03: jobs similar to a job's stored embedding. The embedding provider is never called, authorization and the
  * source job's visibility precede any vector call, and a hidden source job is indistinguishable from an unknown one.
  */
final class SimilarJobsServiceSpec extends CatsEffectSuite {
  private val model = "voyage-4-lite"
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000099")
  private val first = PageSize.fromInt(5).toOption.get
  private val actor = ActorContext(candidateId, UserRole.Candidate)
  private val candidateUser =
    candidate.copy(profile = Some(UserProfile.Candidate(CandidateProfile(Set("Scala", "Kafka"), None, None))))
  private val sourceVector = List(0.5f, 0.5f)

  private def jobNumber(n: Int): Identifiers.JobId =
    Identifiers.JobId(UUID.fromString(f"00000000-0000-0000-0001-$n%012d"))

  private def embedded(job: Job, vector: List[Float] = sourceVector, embeddedModel: String = model): Job =
    job.copy(embedding =
      Some(EntityEmbedding(vector, EmbeddingMeta(embeddedModel, SourceHash.sha256(SearchableText.job(job)), now)))
    )

  private val source = embedded(openJob)
  private def neighbour(n: Int, skills: Set[String] = Set("Scala")): Job =
    embedded(openJob.copy(id = jobNumber(n), title = s"Neighbour $n", skills = skills))
  private def closed(job: Job): Job = job.copy(state = JobState.Closed(Instant.EPOCH))

  private def hit(job: Job, score: Double): JobRetrievalHit =
    SearchRetrievalHit(job.id, score, SearchMode.VECTOR, job.embedding.get.meta, searchId)

  private final class Probe(
      val vectorQueries: Ref[IO, Vector[VectorSearchQuery]],
      val eligibilityReads: Ref[IO, Int],
      val providerCalls: Ref[IO, Int]
  )

  private def probe = (Ref.of[IO, Vector[VectorSearchQuery]](Vector.empty), Ref.of[IO, Int](0), Ref.of[IO, Int](0))
    .mapN(new Probe(_, _, _))

  /** Eligibility is the live job store read for the candidate; `similar` is the vector branch. */
  private def run(
      stored: List[Job],
      hits: List[JobRetrievalHit],
      requestedJob: Identifiers.JobId = jobId,
      users: List[User] = List(candidateUser, recruiter),
      caller: ActorContext = actor,
      size: PageSize = first,
      vectorFailure: Option[RepositoryError] = None,
      afterRetrieval: Ref[IO, Map[Identifiers.JobId, Job]] => IO[Unit] = _ => IO.unit,
      afterEligibility: Ref[IO, Map[Identifiers.JobId, Job]] => IO[Unit] = _ => IO.unit
  ): IO[(Either[UseCaseError, List[RankedJob]], Probe)] =
    for {
      p <- probe
      usersRef <- Ref.of[IO, Map[Identifiers.UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[Identifiers.JobId, Job]](stored.map(job => job.id -> job).toMap)
      userRepository = new InMemoryUsers(usersRef)
      jobRepository = new InMemoryJobs(jobsRef)
      embeddings = new EmbeddingService[IO] {
        def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
          p.providerCalls.update(_ + 1).as(Left(EmbeddingError.ProviderUnavailable))
      }
      search = new SemanticSearchRepository[IO] {
        def authorizedJobEligibility(
            scope: HiringReadScope,
            ids: List[Identifiers.JobId],
            expected: Option[CandidateSearchEligibility]
        ): RepositoryF[IO, List[JobSearchEligibility]] =
          RepositoryF.lift(p.eligibilityReads.update(_ + 1)) *>
            userRepository
              .find(scope.userId)
              .flatMap(found =>
                if (found.exists(_.role == UserRole.Candidate))
                  ids.traverse(jobRepository.find).map(_.flatten.map(JobSearchEligibility.fromJob))
                else RepositoryF.at[IO].pure(Nil)
              )
              .flatTap(_ => RepositoryF.lift(afterEligibility(jobsRef)))
        def authorizedCandidateEligibility(
            scope: HiringReadScope,
            queryJob: JobSearchEligibility,
            ids: List[Identifiers.UserId]
        ): RepositoryF[IO, List[CandidateSearchEligibility]] = RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        def searchJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
          RepositoryF.lift(p.vectorQueries.update(_ :+ query)) *> RepositoryF.at[IO].pure(Nil)
        def recommendedJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
          RepositoryF.lift(p.vectorQueries.update(_ :+ query)) *> RepositoryF.at[IO].pure(Nil)
        def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        def similarJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
          RepositoryF.lift(p.vectorQueries.update(_ :+ query) *> afterRetrieval(jobsRef)) *>
            vectorFailure.fold(RepositoryF.at[IO].pure(hits))(RepositoryF.at[IO].raise(_))
        def lexicalJobs(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalJobHit]] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
        def lexicalCandidates(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalCandidateHit]] =
          RepositoryF.at[IO].raise(RepositoryError.Unavailable)
      }
      service = SemanticSearchService(userRepository, jobRepository, embeddings, search, model)
      result <- service.similarJobs(caller, requestedJob, size, searchId).value
    } yield (result, p)

  private def assertNoVectorNoProvider(p: Probe): IO[Unit] =
    (p.vectorQueries.get, p.providerCalls.get).mapN { (queries, providerCalls) =>
      assertEquals(queries, Vector.empty[VectorSearchQuery])
      assertEquals(providerCalls, 0)
    }

  test("GDF-03.1 similar jobs are nearest first, exclude the job itself and closed jobs, and use the stored vector") {
    val near = neighbour(1)
    val shut = closed(neighbour(2))
    val far = neighbour(3, Set("Scala", "Rust"))
    run(
      List(source, near, shut, far),
      List(hit(source, 0.99), hit(near, 0.9), hit(shut, 0.8), hit(far, 0.7))
    ).flatMap { case (result, p) =>
      (p.vectorQueries.get, p.providerCalls.get).mapN { (queries, providerCalls) =>
        assertEquals(result.map(_.map(_.job.id)), Right(List(near.id, far.id)))
        assertEquals(result.map(_.map(_.score)), Right(List(0.9, 0.7)))
        assertEquals(queries.map(_.vector), Vector(sourceVector))
        assertEquals(queries.map(_.model), Vector(model))
        assertEquals(providerCalls, 0)
        // Fit reasons come from the caller's own profile and the neighbour's skills only.
        assertEquals(result.map(_.map(_.matchedSkills)), Right(List(List("Scala"), List("Scala"))))
        assertEquals(result.map(_.map(_.missingSkills)), Right(List(Nil, List("Rust"))))
      }
    }
  }

  test("GDF-03.1 similar jobs return at most the requested page size") {
    val neighbours = (1 to 4).toList.map(neighbour(_))
    run(
      source :: neighbours,
      hit(source, 1d) :: neighbours.map(hit(_, 0.5)),
      size = PageSize.fromInt(2).toOption.get
    ).map { case (result, _) =>
      assertEquals(result.map(_.map(_.job.id)), Right(neighbours.take(2).map(_.id)))
    }
  }

  test("GDF-03.1 a neighbour closed after retrieval is dropped by the live eligibility recheck") {
    val near = neighbour(1)
    val soon = neighbour(2)
    run(
      List(source, near, soon),
      List(hit(near, 0.9), hit(soon, 0.8)),
      afterRetrieval = _.update(_.updated(soon.id, closed(soon)))
    ).map { case (result, _) => assertEquals(result.map(_.map(_.job.id)), Right(List(near.id))) }
  }

  test("GDF-03.2 a closed source job gives the same outcome as an unknown job, with no vector call") {
    val unknown = jobNumber(404)
    for {
      hidden <- run(List(closed(source)), Nil)
      missing <- run(Nil, Nil, requestedJob = unknown)
      _ <- assertNoVectorNoProvider(hidden._2)
      _ <- assertNoVectorNoProvider(missing._2)
      hiddenReads <- hidden._2.eligibilityReads.get
      missingReads <- missing._2.eligibilityReads.get
    } yield {
      assertEquals(hidden._1, Left(UseCaseError.Domain(DomainError.NotFound("job"))))
      assertEquals(missing._1, hidden._1)
      // Both end after the same read work, so timing or read counts do not tell them apart.
      assertEquals(hiddenReads, missingReads)
    }
  }

  test("GDF-03.2 a draft source job gives the same outcome as an unknown job, with no vector call") {
    for {
      draft <- run(List(embedded(openJob.copy(state = JobState.Draft))), Nil)
      missing <- run(Nil, Nil, requestedJob = jobNumber(404))
      _ <- assertNoVectorNoProvider(draft._2)
    } yield assertEquals(draft._1, missing._1)
  }

  test("GDF-03.2 a hidden source job with a stale embedding is NotFound, not StaleEmbedding") {
    val staleHidden = closed(source.copy(title = "Edited after embedding"))
    for {
      hidden <- run(List(staleHidden), Nil)
      _ <- assertNoVectorNoProvider(hidden._2)
    } yield assertEquals(hidden._1, Left(UseCaseError.Domain(DomainError.NotFound("job"))))
  }

  test("GDF-03.2 a source job closed between the eligibility and stored reads is NotFound with no vector call") {
    run(
      List(source),
      List(hit(source, 1d)),
      afterEligibility = _.update(_.updated(source.id, closed(source)))
    ).flatMap { case (result, p) =>
      assertNoVectorNoProvider(p).as(assertEquals(result, Left(UseCaseError.Domain(DomainError.NotFound("job")))))
    }
  }

  test("GDF-03.2 a source job deleted between the eligibility and stored reads is NotFound with no vector call") {
    run(
      List(source),
      List(hit(source, 1d)),
      afterEligibility = _.update(_ - source.id)
    ).flatMap { case (result, p) =>
      assertNoVectorNoProvider(p).as(assertEquals(result, Left(UseCaseError.Domain(DomainError.NotFound("job")))))
    }
  }

  test("GDF-03.2 a hidden source job without an embedding is not told apart from an unknown job") {
    // The embedding state of a job the caller may not see must not leak through MissingEmbedding.
    for {
      hidden <- run(List(closed(openJob)), Nil)
      missing <- run(Nil, Nil, requestedJob = jobNumber(404))
    } yield assertEquals(hidden._1, missing._1)
  }

  test("GDF-03.3 a source job without a stored embedding is MissingEmbedding and makes no vector call") {
    run(List(openJob), Nil).flatMap { case (result, p) =>
      assertNoVectorNoProvider(p).as(
        assertEquals(result, Left(UseCaseError.Search(SearchError.MissingEmbedding("job"))))
      )
    }
  }

  test("GDF-03.3 a stale source embedding (changed text or other model) is StaleEmbedding with no vector call") {
    val edited = source.copy(title = "A different title after embedding")
    val otherModel = embedded(openJob, embeddedModel = "voyage-3-old")
    for {
      text <- run(List(edited), Nil)
      model <- run(List(otherModel), Nil)
      _ <- assertNoVectorNoProvider(text._2)
      _ <- assertNoVectorNoProvider(model._2)
    } yield {
      assertEquals(text._1, Left(UseCaseError.Search(SearchError.StaleEmbedding("job"))))
      assertEquals(model._1, Left(UseCaseError.Search(SearchError.StaleEmbedding("job"))))
    }
  }

  test("GDF-03.4 a vector store failure is a sanitized search failure") {
    run(List(source), Nil, vectorFailure = Some(RepositoryError.Unavailable)).map { case (result, _) =>
      assertEquals(result, Left(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
    }
  }

  private def refusedBeforeAnyRead(label: String, users: List[User], caller: ActorContext, expected: UseCaseError)(using
      munit.Location
  ): Unit =
    test(s"GDF-08 $label is refused before any read, vector call or provider call") {
      run(List(source), List(hit(source, 1d)), users = users, caller = caller).flatMap { case (result, p) =>
        assertNoVectorNoProvider(p) *>
          p.eligibilityReads.get.map { reads =>
            assertEquals(reads, 0)
            assertEquals(result, Left(expected))
          }
      }
    }

  private val unknownActor =
    ActorContext(Identifiers.UserId(UUID.fromString("00000000-0000-0000-0000-0000000000ff")), UserRole.Candidate)

  refusedBeforeAnyRead(
    "a recruiter",
    List(candidateUser, recruiter),
    ActorContext(recruiterId, UserRole.Recruiter),
    UseCaseError.Domain(DomainError.CandidateRequired)
  )
  refusedBeforeAnyRead(
    "an Admin",
    List(candidateUser, recruiter, admin),
    ActorContext(adminId, UserRole.Admin),
    UseCaseError.Domain(DomainError.CandidateRequired)
  )
  refusedBeforeAnyRead(
    "a deleted Candidate",
    List(candidateUser.copy(profile = None, state = AccountState.Deleted(now)), recruiter),
    actor,
    UseCaseError.Authentication(AuthenticationError.Unauthorized)
  )
  refusedBeforeAnyRead(
    "an unknown actor",
    List(candidateUser, recruiter),
    unknownActor,
    UseCaseError.Authentication(AuthenticationError.Unauthorized)
  )
}
