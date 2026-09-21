package com.example.graphQL.cats.service.search

import cats.Monad
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

final class SemanticSearchService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddings: EmbeddingService[F],
    search: SemanticSearchRepository[F],
    embeddingModel: String,
    embeddingVersion: Int
) extends SearchUseCases[F] {
  private val authorization = ActorAuthorization(users)

  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): F[Either[UseCaseError, List[RankedJob]]] =
    resolveCandidate(actor).flatMap {
      case Left(error) => error.asLeft[List[RankedJob]].pure[F]
      case Right(_) if text.length > SearchableText.QueryMaxChars =>
        UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)).asLeft[List[RankedJob]].pure[F]
      case Right(_) => embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)).flatMap {
        case Left(_) => UseCaseError.Search(SearchError.ProviderUnavailable).asLeft[List[RankedJob]].pure[F]
        case Right(vector) =>
          search.searchJobs(VectorSearchQuery(
            vector.values,
            Some(text),
            filter,
            first,
            SearchMode.HYBRID,
            embeddingModel,
            embeddingVersion,
            searchId
          )).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      }
    }

  def recommendedJobs(
      actor: ActorContext,
      first: PageSize,
      searchId: UUID
  ): F[Either[UseCaseError, List[RankedJob]]] =
    resolveCandidate(actor).flatMap {
      case Left(error) => error.asLeft[List[RankedJob]].pure[F]
      case Right(user) =>
        (user.candidateProfile, user.embedding) match {
          case (Some(profile), Some(embedding)) if embedding.meta.model == embeddingModel &&
              embedding.meta.version == embeddingVersion &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile)) =>
            val query = VectorSearchQuery(
              embedding.values,
              None,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              embedding.meta.version,
              searchId
            )
            search.recommendedJobs(query).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
          case (Some(_), Some(_)) => UseCaseError.Search(SearchError.StaleEmbedding("candidate")).asLeft[List[RankedJob]].pure[F]
          case _ => UseCaseError.Search(SearchError.MissingEmbedding("candidate")).asLeft[List[RankedJob]].pure[F]
        }
    }

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): F[Either[UseCaseError, List[RankedCandidate]]] =
    authorization.resolve(actor).flatMap {
      case Left(error) => error.asLeft[List[RankedCandidate]].pure[F]
      case Right(user) if user.role == UserRole.Candidate =>
        UseCaseError.Domain(DomainError.RecruiterRequired).asLeft[List[RankedCandidate]].pure[F]
      case Right(user) =>
        jobs.find(jobId).flatMap {
      case Left(error) => UseCaseError.Repository(error).asLeft[List[RankedCandidate]].pure[F]
      case Right(None) => UseCaseError.Domain(DomainError.NotFound("job")).asLeft[List[RankedCandidate]].pure[F]
      case Right(Some(job)) if user.role == UserRole.Recruiter && job.recruiterId != user.id =>
        UseCaseError.Domain(DomainError.Forbidden).asLeft[List[RankedCandidate]].pure[F]
      case Right(Some(job)) if job.status != JobStatus.Open =>
        UseCaseError.Domain(DomainError.JobMustBeOpen).asLeft[List[RankedCandidate]].pure[F]
      case Right(Some(job)) =>
        job.embedding match {
          case Some(embedding) if embedding.meta.model == embeddingModel &&
              embedding.meta.version == embeddingVersion &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job)) =>
            val query = VectorSearchQuery(
              embedding.values,
              None,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              embedding.meta.version,
              searchId
            )
            search.candidateMatches(query).map(_.leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable)))
          case Some(_) => UseCaseError.Search(SearchError.StaleEmbedding("job")).asLeft[List[RankedCandidate]].pure[F]
          case None => UseCaseError.Search(SearchError.MissingEmbedding("job")).asLeft[List[RankedCandidate]].pure[F]
        }
        }
    }

  private def resolveCandidate(actor: ActorContext): F[Either[UseCaseError, User]] =
    authorization.resolve(actor).map(_.flatMap { user =>
      if (user.role == UserRole.Candidate) user.asRight[UseCaseError]
      else UseCaseError.Domain(DomainError.CandidateRequired).asLeft[User]
    })
}

object SemanticSearchService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      embeddings: EmbeddingService[F],
      search: SemanticSearchRepository[F],
      embeddingModel: String,
      embeddingVersion: Int
  ): SemanticSearchService[F] =
    new SemanticSearchService(users, jobs, embeddings, search, embeddingModel, embeddingVersion)
}
