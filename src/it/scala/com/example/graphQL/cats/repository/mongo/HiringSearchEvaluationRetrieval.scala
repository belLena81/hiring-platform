package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.search.*
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.util.UUID
import scala.jdk.CollectionConverters.*

/** Captures IDs through real service authorization and current Mongo eligibility, never fixture prefiltering. */
private[mongo] final class HiringSearchEvaluationRetrieval(database: MongoDatabase[IO], policy: DiscoveryQueryPolicy)
    extends SearchEvaluationCapture.Retrieval {
  private val indexes = HiringSearchEvaluationAtlasRunner.indexes
  private val repository = new MongoSemanticSearchRepository(
    database,
    indexes.jobVectorIndex,
    indexes.candidateVectorIndex,
    indexes.jobLexicalIndex,
    indexes.candidateLexicalIndex,
    8,
    8,
    diagnostics = Diagnostics.noop,
    discoveryPolicy = Some(policy)
  )
  private val users = new MongoUserRepository(
    database,
    MongoRepositoryTestSupport.noTransaction,
    MongoEmbeddingWorkEnqueuer.disabled,
    Diagnostics.noop
  )
  private val jobs = new MongoJobRepository(
    database,
    MongoRepositoryTestSupport.noTransaction,
    MongoEmbeddingWorkEnqueuer.disabled,
    Diagnostics.noop
  )
  private val page = PageSize
    .fromInt(SearchEvaluationFixtures.K)
    .fold(_ => throw new IllegalStateException("Invalid fixed fixture page size"), identity)

  private def documents(collection: String, pipeline: List[Document]): RepositoryIO[List[Document]] =
    RepositoryIO.fromIOEither(
      policy
        .run(
          Mongo4catsCollections
            .documents(database, collection)
            .flatMap { source =>
              MongoRepositoryTestSupport
                .collectWithin(source.aggregate[Document](pipeline).maxTime(policy.maxTime).stream, 8)
            }
        )
        .attempt
        .map(_.leftMap(_ => RepositoryError.Unavailable))
    )

  private def branch(strategy: SearchEvaluationStrategy): SemanticSearchRepository = new SemanticSearchRepository {
    def authorizedJobEligibility(
        scope: HiringReadScope,
        ids: List[JobId],
        candidate: Option[CandidateSearchEligibility]
    ) =
      repository.authorizedJobEligibility(scope, ids, candidate)
    def authorizedCandidateEligibility(scope: HiringReadScope, job: JobSearchEligibility, ids: List[UserId]) =
      repository.authorizedCandidateEligibility(scope, job, ids)
    def jobEligibility(ids: List[JobId]) = repository.jobEligibility(ids)
    def candidateEligibility(ids: List[UserId]) = repository.candidateEligibility(ids)
    def recommendedJobs(query: VectorSearchQuery) = repository.recommendedJobs(query)
    def searchJobs(query: VectorSearchQuery): RepositoryIO[List[JobRetrievalHit]] = strategy match {
      case SearchEvaluationStrategy.ApplicationRrf => repository.searchJobs(query)
      case SearchEvaluationStrategy.Vector         =>
        repository.searchJobs(query.copy(lexicalQuery = None, mode = SearchMode.VECTOR))
      case SearchEvaluationStrategy.Lexical =>
        val pipeline = repository
          .lexicalJobStages(query.lexicalQuery.getOrElse(""), repository.jobLexicalFilter(query, query.filter))
          .asScala
          .toList :+
          new Document("$set", new Document(MongoFields.Score, new Document("$meta", "searchScore"))) :+
          repository.retrievalProjection
        documents(MongoCollections.Jobs, pipeline).subflatMap(
          MongoSemanticSearchResult.jobHits(_, query).map(_.flatten)
        )
      case SearchEvaluationStrategy.AtlasAnn => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    }
    def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[CandidateRetrievalHit]] = strategy match {
      case SearchEvaluationStrategy.ApplicationRrf => repository.candidateMatches(query)
      case SearchEvaluationStrategy.Vector         =>
        repository.candidateMatches(query.copy(lexicalQuery = None, candidateQueryVector = None))
      case SearchEvaluationStrategy.Lexical =>
        val pipeline =
          repository.candidateLexicalPipeline(query, repository.candidateLexicalFilter(query)).asScala.toList
        documents(MongoCollections.Users, pipeline).subflatMap(
          MongoSemanticSearchResult.candidateHits(_, query).map(_.flatten)
        )
      case SearchEvaluationStrategy.AtlasAnn => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
    }
  }

  override def retrieve(
      query: SearchEvaluationFixtureQuery,
      strategy: SearchEvaluationStrategy
  ): IO[Either[SearchEvaluationFailure, List[String]]] = {
    val embeddings = new EmbeddingService {
      def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] = IO.pure(
        Right(
          EmbeddingVector(
            HiringSearchEvaluationCorpus.vector(HiringSearchEvaluationCorpus.querySkills(query)),
            HiringSearchEvaluationCorpus.Model,
            HiringSearchEvaluationCorpus.Dimensions
          )
        )
      )
    }
    val service =
      new SemanticSearchService(users, jobs, embeddings, branch(strategy), HiringSearchEvaluationCorpus.Model)
    val id = UUID.nameUUIDFromBytes(query.queryId.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    val result = query.useCase match {
      case SearchEvaluationUseCase.JobSearch =>
        service
          .semanticJobSearch(
            ActorContext(HiringSearchEvaluationCorpus.candidates.head.id, UserRole.Candidate),
            HiringSearchEvaluationCorpus.text(query),
            HiringSearchEvaluationCorpus.jobFilter(query),
            page,
            id
          )
          .map(_.map(_.job.id.value.toString))
          .value
      case SearchEvaluationUseCase.Recommendations =>
        val actorId =
          if (query.queryId.endsWith("-2")) SearchEvaluationFixtures.candidateId(4)
          else SearchEvaluationFixtures.candidateId(1)
        service
          .recommendedJobs(ActorContext(UserId(UUID.fromString(actorId)), UserRole.Candidate), page, id)
          .map(_.map(_.job.id.value.toString))
          .value
      case SearchEvaluationUseCase.RecruiterMatching =>
        val jobId =
          if (query.queryId.endsWith("-2")) SearchEvaluationFixtures.jobId(4) else SearchEvaluationFixtures.jobId(1)
        service
          .candidateMatches(
            ActorContext(HiringSearchEvaluationCorpus.recruiter.id, UserRole.Recruiter),
            JobId(UUID.fromString(jobId)),
            Some(HiringSearchEvaluationCorpus.text(query)),
            HiringSearchEvaluationCorpus.candidateFilter(query),
            page,
            id
          )
          .map(_.map(_.candidate.id.value.toString))
          .value
    }
    result.map(_.leftMap(_ => SearchEvaluationFailure.RetrievalFailed))
  }
}
