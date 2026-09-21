package com.example.graphQL.cats.service.search

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.{JobStatus, SearchMode, SearchableText, User, UserRole}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.SearchUseCases
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.PageSize
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.util.UUID

final class SemanticSearchService(
    users: UserRepository[IO],
    jobs: JobRepository[IO],
    embeddings: EmbeddingService[IO],
    search: SemanticSearchRepository[IO],
    embeddingModel: String
) extends SearchUseCases {
  private val authorization = ActorAuthorization(users)

  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): IO[Either[UseCaseError, List[RankedJob]]] =
    resolveCandidate(actor).flatMap {
      case Left(error) => IO.pure(error.asLeft[List[RankedJob]])
      case Right(_) if text.length > SearchableText.QueryMaxChars =>
        IO.pure(UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)).asLeft[List[RankedJob]])
      case Right(_) => embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)).flatMap {
        case Left(_) => IO.pure(UseCaseError.Search(SearchError.ProviderUnavailable).asLeft[List[RankedJob]])
        case Right(vector) =>
          search.searchJobs(VectorSearchQuery(
            vector.values,
            Some(text),
            filter,
            first,
            SearchMode.HYBRID,
            embeddingModel,
            searchId
          )).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      }
    }

  def recommendedJobs(
      actor: ActorContext,
      first: PageSize,
      searchId: UUID
  ): IO[Either[UseCaseError, List[RankedJob]]] =
    resolveCandidate(actor).flatMap {
      case Left(error) => IO.pure(error.asLeft[List[RankedJob]])
      case Right(user) =>
        (user.candidateProfile, user.embedding) match {
          case (Some(profile), Some(embedding)) if embedding.meta.model == embeddingModel &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile)) =>
            val query = VectorSearchQuery(
              embedding.values,
              None,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              searchId
            )
            search.recommendedJobs(query).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
          case (Some(_), Some(_)) => IO.pure(UseCaseError.Search(SearchError.StaleEmbedding("candidate")).asLeft[List[RankedJob]])
          case _ => IO.pure(UseCaseError.Search(SearchError.MissingEmbedding("candidate")).asLeft[List[RankedJob]])
        }
    }

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): IO[Either[UseCaseError, List[RankedCandidate]]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => IO.pure(error.asLeft[List[RankedCandidate]])
      case Right(user) if user.role == UserRole.Candidate =>
        IO.pure(UseCaseError.Domain(DomainError.RecruiterRequired).asLeft[List[RankedCandidate]])
      case Right(user) =>
        jobs.find(jobId).flatMap {
      case Left(error) => IO.pure(UseCaseError.Repository(error).asLeft[List[RankedCandidate]])
      case Right(None) => IO.pure(UseCaseError.Domain(DomainError.NotFound("job")).asLeft[List[RankedCandidate]])
      case Right(Some(job)) if user.role == UserRole.Recruiter && job.recruiterId != user.id =>
        IO.pure(UseCaseError.Domain(DomainError.Forbidden).asLeft[List[RankedCandidate]])
      case Right(Some(job)) if job.status != JobStatus.Open =>
        IO.pure(UseCaseError.Domain(DomainError.JobMustBeOpen).asLeft[List[RankedCandidate]])
      case Right(Some(job)) =>
        job.embedding match {
          case Some(embedding) if embedding.meta.model == embeddingModel &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job)) =>
            val query = VectorSearchQuery(
              embedding.values,
              None,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              searchId
            )
            search.candidateMatches(query).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
          case Some(_) => IO.pure(UseCaseError.Search(SearchError.StaleEmbedding("job")).asLeft[List[RankedCandidate]])
          case None => IO.pure(UseCaseError.Search(SearchError.MissingEmbedding("job")).asLeft[List[RankedCandidate]])
        }
        }
    }

  private def resolveCandidate(actor: ActorContext): IO[Either[UseCaseError, User]] =
    authorization.resolve(actor).map(_.flatMap { user =>
      if (user.role == UserRole.Candidate) user.asRight[UseCaseError]
      else UseCaseError.Domain(DomainError.CandidateRequired).asLeft[User]
    })
}

object SemanticSearchService {
  def apply(
      users: UserRepository[IO],
      jobs: JobRepository[IO],
      embeddings: EmbeddingService[IO],
      search: SemanticSearchRepository[IO],
      embeddingModel: String
  ): SemanticSearchService =
    new SemanticSearchService(users, jobs, embeddings, search, embeddingModel)
}
