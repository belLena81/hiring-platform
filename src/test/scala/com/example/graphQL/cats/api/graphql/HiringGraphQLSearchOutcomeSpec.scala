package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{
  ActorContext,
  HiringReadService,
  ProbeResult,
  RepositoryError,
  SearchError,
  ServiceFixtures,
  UseCaseError
}
import com.example.graphQL.cats.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.service.port.{
  EmbeddingError,
  EmbeddingInput,
  EmbeddingService,
  EmbeddingVector,
  RepositoryF,
  SemanticSearchRepository
}
import com.example.graphQL.cats.service.search.{
  CandidateRetrievalHit,
  LexicalCandidateHit,
  LexicalJobHit,
  LexicalRetrievalHit,
  LexicalSearchQuery,
  JobRetrievalHit,
  SemanticSearchService,
  VectorSearchQuery
}
import com.example.graphQL.cats.shared.crypto.SourceHash
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** The AI search roots return a typed outcome, so one unavailable search never blanks sibling fields. */
final class HiringGraphQLSearchOutcomeSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-17T08:00:00Z")
  private val model = "voyage-4-lite"
  private val candidate = ServiceFixtures.candidate
  private val recruiter = ServiceFixtures.recruiter
  private val otherRecruiter =
    recruiter.copy(id = UserId(UUID.fromString("20000000-0000-0000-0000-000000000002")), name = "Other recruiter")
  private val currentJob = ServiceFixtures.openJob.copy(embedding =
    Some(EntityEmbedding(List(0.1f, 0.2f), EmbeddingMeta(model, "stale-on-purpose", now)))
  )
  private val freshJob = ServiceFixtures.openJob.copy(embedding =
    Some(
      EntityEmbedding(
        List(0.1f, 0.2f),
        EmbeddingMeta(model, SourceHash.sha256(SearchableText.job(ServiceFixtures.openJob)), now)
      )
    )
  )
  private val vector = EmbeddingVector(List(0.1f, 0.2f), model, 2)

  private final class CountingEmbeddings(result: Either[EmbeddingError, EmbeddingVector]) extends EmbeddingService[IO] {
    val calls = new AtomicInteger(0)
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO(calls.incrementAndGet()).as(result)
  }

  /** Returns no vector hits; lexical hits are the given jobs (eligible for Candidates) or a store failure. */
  private final class FakeRepository(lexical: Either[RepositoryError, List[Job]]) extends SemanticSearchRepository[IO] {
    override def authorizedJobEligibility(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        ids: List[JobId],
        queryCandidate: Option[com.example.graphQL.cats.service.search.CandidateSearchEligibility]
    ) =
      RepositoryF
        .at[IO]
        .fromEither(
          Right(
            lexical.toList.flatten
              .filter(job => ids.contains(job.id))
              .map(com.example.graphQL.cats.service.search.JobSearchEligibility.fromJob)
          )
        )
    override def authorizedCandidateEligibility(
        scope: com.example.graphQL.cats.service.read.HiringReadScope,
        queryJob: com.example.graphQL.cats.service.search.JobSearchEligibility,
        ids: List[UserId]
    ) =
      RepositoryF
        .at[IO]
        .fromEither(Right(List.empty[com.example.graphQL.cats.service.search.CandidateSearchEligibility]))
    override def searchJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
      RepositoryF.at[IO].fromEither(Right(Nil))
    override def recommendedJobs(query: VectorSearchQuery): RepositoryF[IO, List[JobRetrievalHit]] =
      RepositoryF.at[IO].fromEither(Right(Nil))
    override def candidateMatches(query: VectorSearchQuery): RepositoryF[IO, List[CandidateRetrievalHit]] =
      RepositoryF.at[IO].fromEither(Right(Nil))
    override def lexicalJobs(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalJobHit]] =
      RepositoryF
        .at[IO]
        .fromEither(lexical.map(_.map(job => LexicalRetrievalHit(job.id, 1.5d, query.searchId))))
    override def lexicalCandidates(query: LexicalSearchQuery): RepositoryF[IO, List[LexicalCandidateHit]] =
      RepositoryF.at[IO].fromEither(Right(Nil))
  }

  private def run(
      query: String,
      actor: Option[ActorContext],
      embeddings: CountingEmbeddings,
      users: List[User] = List(candidate, recruiter, otherRecruiter),
      job: Job = freshJob,
      searchConfigured: Boolean = true,
      lexical: Either[RepositoryError, List[Job]] = Right(Nil),
      costSink: Option[AtomicInteger] = None
  ): IO[Json] =
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(job.id -> job))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      createError <- Ref.of[IO, Option[RepositoryError]](None)
      userRepository = new InMemoryUsers(usersRef)
      jobRepository = new InMemoryJobs(jobsRef)
      applications = new InMemoryApplications(applicationsRef, eventsRef, createError)
      service = SemanticSearchService(userRepository, jobRepository, embeddings, new FakeRepository(lexical), model)
      services = TestGraphQLSupport.emptyServices.copy(
        readModel = new HiringReadService(userRepository, jobRepository, applications),
        semanticSearchService = Option.when(searchConfigured)(service)
      )
      result <- RequestContextFactory.resource
        .flatMap(
          _.resource(
            RequestContextParameters(
              actor,
              services,
              IO.pure(ProbeResult.Ready),
              diagnostics = com.example.graphQL.cats.service.Diagnostics.noop,
              costBudget = (_, cost) => {
                costSink.foreach(_.addAndGet(cost));
                Right(com.example.graphQL.cats.api.admission.FixedWindowRateLimiter.Grant.Shared)
              }
            )
          )
        )
        .use { context =>
          TestGraphQLSupport
            .parseAndExecute(GraphQLRequest(query, Json.obj(), None), context)
            .map(_.fold(failure => fail(failure.toString), identity))
        }
    } yield result

  private val outcome = "__typename ... on UserError { code message } ... on SearchUnavailable { retryable } " +
    "... on ValidationError { field }"
  private def candidateActor(user: User) = Some(ActorContext(user.id, user.role))

  private def field(json: Json, name: String) = json.hcursor.downField("data").downField(name)
  private def topLevelCode(json: Json) =
    json.hcursor.downField("errors").downArray.downField("extensions").get[String]("code")

  test(
    "AIR-01.1 a provider outage with the fallback also down returns SearchUnavailable and the sibling field survives"
  ) {
    val embeddings = new CountingEmbeddings(Left(EmbeddingError.ProviderUnavailable))
    run(
      s"""query { sibling: accountDeletionStatus(receiptId: "00000000-0000-0000-0000-000000000001") semanticJobSearch(query: "scala", first: 5) { $outcome } }""",
      candidateActor(candidate),
      embeddings,
      lexical = Left(RepositoryError.Unavailable)
    ).map { json =>
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(field(json, "sibling").focus.flatMap(_.asString), Some("NOT_FOUND"))
      val search = field(json, "semanticJobSearch")
      assertEquals(search.get[String]("__typename"), Right("SearchUnavailable"))
      assertEquals(search.get[String]("code"), Right("VECTOR_SEARCH_UNAVAILABLE"))
      assertEquals(search.get[Boolean]("retryable"), Right(true))
    }
  }

  test("AIR-01.1 an unconfigured vector search is reported as retryable SearchUnavailable") {
    run(
      s"""query { semanticJobSearch(query: "scala", first: 5) { $outcome } }""",
      candidateActor(candidate),
      new CountingEmbeddings(Right(vector)),
      searchConfigured = false
    ).map { json =>
      assertEquals(field(json, "semanticJobSearch").get[String]("code"), Right("VECTOR_SEARCH_UNAVAILABLE"))
      assertEquals(field(json, "semanticJobSearch").get[Boolean]("retryable"), Right(true))
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
    }
  }

  test("AIR-01.2 a stale candidate embedding makes recommendedJobs return SearchUnavailable") {
    val stale = candidate.copy(embedding = Some(EntityEmbedding(List(0.1f), EmbeddingMeta(model, "stale", now))))
    run(
      s"""query { sibling: accountDeletionStatus(receiptId: "00000000-0000-0000-0000-000000000001") recommendedJobs(first: 5) { $outcome } }""",
      candidateActor(stale),
      new CountingEmbeddings(Right(vector)),
      users = List(stale, recruiter)
    ).map { json =>
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(field(json, "recommendedJobs").get[String]("__typename"), Right("SearchUnavailable"))
      assertEquals(field(json, "recommendedJobs").get[String]("code"), Right("STALE_EMBEDDING"))
      assertEquals(field(json, "sibling").focus.flatMap(_.asString), Some("NOT_FOUND"))
    }
  }

  test("AIR-01.3 a query over the size limit returns ValidationError for the query field") {
    val embeddings = new CountingEmbeddings(Right(vector))
    run(
      s"""query { semanticJobSearch(query: "${"a" * 2049}", first: 5) { $outcome } }""",
      candidateActor(candidate),
      embeddings
    ).map { json =>
      val search = field(json, "semanticJobSearch")
      assertEquals(search.get[String]("__typename"), Right("ValidationError"))
      assertEquals(search.get[String]("code"), Right("INPUT_TOO_LARGE"))
      assertEquals(search.get[String]("field"), Right("query"))
      assertEquals(embeddings.calls.get, 0)
    }
  }

  test("AIR-01.4 a recruiter on another recruiter's job gets a top-level FORBIDDEN, a null field and intact siblings") {
    val embeddings = new CountingEmbeddings(Right(vector))
    run(
      s"""query { sibling: accountDeletionStatus(receiptId: "00000000-0000-0000-0000-000000000001") candidateMatches(jobId: "${freshJob.id.value}", first: 5) { $outcome } }""",
      candidateActor(otherRecruiter),
      embeddings
    ).map { json =>
      assertEquals(topLevelCode(json), Right("FORBIDDEN"))
      assertEquals(field(json, "candidateMatches").focus, Some(Json.Null))
      assertEquals(field(json, "sibling").focus.flatMap(_.asString), Some("NOT_FOUND"))
      assertEquals(embeddings.calls.get, 0)
    }
  }

  test("AIR-01.4 a stale job embedding is SearchUnavailable for the owning recruiter") {
    run(
      s"""query { candidateMatches(jobId: "${currentJob.id.value}", first: 5) { $outcome } }""",
      candidateActor(recruiter),
      new CountingEmbeddings(Right(vector)),
      job = currentJob
    ).map(json => assertEquals(field(json, "candidateMatches").get[String]("code"), Right("STALE_EMBEDDING")))
  }

  test("AIR-01.4 a missing job is a DomainError, not a top-level error") {
    run(
      s"""query { candidateMatches(jobId: "${UUID.randomUUID()}", first: 5) { $outcome } }""",
      candidateActor(recruiter),
      new CountingEmbeddings(Right(vector))
    ).map { json =>
      assertEquals(field(json, "candidateMatches").get[String]("__typename"), Right("DomainError"))
      assertEquals(field(json, "candidateMatches").get[String]("code"), Right("NOT_FOUND"))
    }
  }

  test("AIR-05.1 a provider outage returns degraded LEXICAL results and the sibling field survives") {
    run(
      s"""query { sibling: accountDeletionStatus(receiptId: "00000000-0000-0000-0000-000000000001")
         |  semanticJobSearch(query: "scala", first: 5) {
         |    __typename
         |    ... on RankedJobResults { degraded degradedReason results { searchMode model job { id } } }
         |    ... on UserError { code } } }""".stripMargin,
      candidateActor(candidate),
      new CountingEmbeddings(Left(EmbeddingError.ProviderUnavailable)),
      lexical = Right(List(freshJob))
    ).map { json =>
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      assertEquals(field(json, "sibling").focus.flatMap(_.asString), Some("NOT_FOUND"))
      val search = field(json, "semanticJobSearch")
      assertEquals(search.get[String]("__typename"), Right("RankedJobResults"))
      assertEquals(search.get[Boolean]("degraded"), Right(true))
      assertEquals(search.get[String]("degradedReason"), Right("PROVIDER_UNAVAILABLE"))
      val hit = search.downField("results").downArray
      assertEquals(hit.get[String]("searchMode"), Right("LEXICAL"))
      assertEquals(hit.downField("model").focus, Some(Json.Null))
      assertEquals(hit.downField("job").get[String]("id"), Right(freshJob.id.value.toString))
    }
  }

  test("AIR-05.1 a healthy search is not degraded") {
    run(
      s"""query { semanticJobSearch(query: "scala", first: 5) {
         |  ... on RankedJobResults { degraded degradedReason } ... on UserError { code } } }""".stripMargin,
      candidateActor(candidate),
      new CountingEmbeddings(Right(vector))
    ).map { json =>
      val search = field(json, "semanticJobSearch")
      assertEquals(search.get[Boolean]("degraded"), Right(false))
      assertEquals(search.downField("degradedReason").focus, Some(Json.Null))
    }
  }

  test("AIR-06.1 when the lexical fallback also fails the search is SearchUnavailable VECTOR_SEARCH_UNAVAILABLE") {
    run(
      s"""query { semanticJobSearch(query: "scala", first: 5) { $outcome } }""",
      candidateActor(candidate),
      new CountingEmbeddings(Left(EmbeddingError.ProviderUnavailable)),
      lexical = Left(RepositoryError.Unavailable)
    ).map { json =>
      assert(!json.hcursor.downField("errors").succeeded, clue(json))
      val search = field(json, "semanticJobSearch")
      assertEquals(search.get[String]("__typename"), Right("SearchUnavailable"))
      assertEquals(search.get[String]("code"), Right("VECTOR_SEARCH_UNAVAILABLE"))
      assertEquals(search.get[Boolean]("retryable"), Right(true))
    }
  }

  test("AIR-02.1 an unauthenticated search stays a top-level UNAUTHORIZED error") {
    val embeddings = new CountingEmbeddings(Right(vector))
    run(
      s"""query { semanticJobSearch(query: "scala", first: 5) { $outcome } }""",
      None,
      embeddings
    ).map { json =>
      assertEquals(topLevelCode(json), Right("UNAUTHORIZED"))
      assertEquals(field(json, "semanticJobSearch").focus, Some(Json.Null))
      assertEquals(embeddings.calls.get, 0)
    }
  }

  test("AIR-01.4 a candidate asking for candidateMatches gets a DomainError naming the missing role") {
    run(
      s"""query { candidateMatches(jobId: "${freshJob.id.value}", first: 5) { $outcome } }""",
      candidateActor(candidate),
      new CountingEmbeddings(Right(vector))
    ).map { json =>
      assertEquals(field(json, "candidateMatches").get[String]("__typename"), Right("DomainError"))
      assertEquals(field(json, "candidateMatches").get[String]("code"), Right("RECRUITER_REQUIRED"))
    }
  }

  test("AIR-03.1 a union selection with a UserError fragment costs the same as the ranked selection alone") {
    def cost(selection: String): IO[Int] = {
      val sink = new AtomicInteger(0)
      run(
        s"""query { semanticJobSearch(query: "scala", first: 5) { $selection } }""",
        candidateActor(candidate),
        new CountingEmbeddings(Right(vector)),
        costSink = Some(sink)
      ).map(_ => sink.get)
    }
    // results { score } weighs 2; code weighs 1, so the maximum alternative is the ranked list.
    val ranked = "... on RankedJobResults { results { score } }"
    for {
      alone <- cost(ranked)
      withErrors <- cost(s"$ranked ... on UserError { code }")
      withAllAlternatives <- cost(
        s"$ranked ... on UserError { code } ... on SearchUnavailable { retryable } ... on ValidationError { field }"
      )
    } yield {
      assertEquals(alone, 100 + 5 * (1 + 2))
      assertEquals(withErrors, alone)
      assertEquals(withAllAlternatives, alone)
    }
  }

  test("AIR-02.1 the classification keeps authentication, readiness and repository failures top-level") {
    val topLevel = List(
      UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized),
      UseCaseError.Domain(com.example.graphQL.cats.domain.error.DomainError.Forbidden),
      UseCaseError.Availability(com.example.graphQL.cats.service.AvailabilityError.ServiceNotReady),
      UseCaseError.Repository(RepositoryError.Unavailable)
    )
    val unavailable = List(
      SearchError.ProviderUnavailable,
      SearchError.VectorSearchUnavailable,
      SearchError.FacetsUnavailable,
      SearchError.MissingEmbedding("job"),
      SearchError.StaleEmbedding("job")
    )
    val validation = List(
      SearchError.InputTooLarge("query", 2048) -> Some("query"),
      SearchError.InvalidFilter("city") -> Some("filter.city"),
      SearchError.InvalidFilter("pageSize") -> Some("first")
    )
    def classify(error: UseCaseError) =
      HiringGraphQLResolverSupport
        .searchResult(
          cats.data.EitherT.leftT[IO, Unit](HiringGraphQLFailure.UseCase(error): HiringGraphQLFailure),
          InputFieldPaths.semanticJobSearch
        )
        .value
    for {
      kept <- topLevel.traverse(error => classify(error).map(_.isLeft))
      data <- unavailable.traverse(error => classify(UseCaseError.Search(error)))
      invalid <- validation.traverse { case (error, _) => classify(UseCaseError.Search(error)) }
    } yield {
      assertEquals(kept, List.fill(topLevel.size)(true))
      assert(data.forall(_.exists(_.isInstanceOf[HiringGraphQLModel.SearchUnavailable])), clue(data))
      assertEquals(
        invalid.map(_.map {
          case HiringGraphQLModel.ValidationError(code, _, field) => code -> field; case _ => ("", None)
        }),
        List(
          Right("INPUT_TOO_LARGE" -> Some("query")),
          Right("INVALID_SEARCH_FILTER" -> Some("filter.city")),
          Right("INVALID_SEARCH_FILTER" -> Some("first"))
        )
      )
    }
  }
}
