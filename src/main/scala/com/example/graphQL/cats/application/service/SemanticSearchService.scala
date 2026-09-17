package com.example.graphQL.cats.application.service

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.application.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.application.port.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.{JobStatus, SearchMode, SearchableText, User, UserRole}
import java.util.UUID

final class SemanticSearchService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddings: EmbeddingService[F],
    search: SemanticSearchRepository[F],
    embeddingVersion: Int
) {
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
        SearchError.InputTooLarge("query", SearchableText.QueryMaxChars).asLeft[List[RankedJob]].pure[F]
      case Right(_) => embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)).flatMap {
        case Left(_) => SearchError.ProviderUnavailable.asLeft[List[RankedJob]].pure[F]
        case Right(vector) =>
          search.searchJobs(VectorSearchQuery(
            vector.values,
            filter,
            first,
            SearchMode.HYBRID,
            vector.model,
            embeddingVersion,
            searchId
          )).map(_.leftMap(_ => SearchError.VectorSearchUnavailable: UseCaseError))
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
        (user.profile, user.embedding) match {
          case (Some(profile), Some(embedding)) if embedding.meta.version == embeddingVersion &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile)) =>
            val query = VectorSearchQuery(
              embedding.values,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              embedding.meta.version,
              searchId
            )
            search.recommendedJobs(query).map(_.leftMap(_ => SearchError.VectorSearchUnavailable: UseCaseError))
          case (Some(_), Some(_)) => SearchError.StaleEmbedding("candidate").asLeft[List[RankedJob]].pure[F]
          case _ => SearchError.MissingEmbedding("candidate").asLeft[List[RankedJob]].pure[F]
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
        DomainError.RecruiterRequired.asLeft[List[RankedCandidate]].pure[F]
      case Right(user) =>
        jobs.find(jobId).flatMap {
      case None => DomainError.NotFound("job").asLeft[List[RankedCandidate]].pure[F]
      case Some(job) if user.role == UserRole.Recruiter && job.recruiterId != user.id =>
        DomainError.Forbidden.asLeft[List[RankedCandidate]].pure[F]
      case Some(job) if job.status != JobStatus.Open =>
        DomainError.JobMustBeOpen.asLeft[List[RankedCandidate]].pure[F]
      case Some(job) =>
        job.embedding match {
          case Some(embedding) if embedding.meta.version == embeddingVersion &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job)) =>
            val query = VectorSearchQuery(
              embedding.values,
              JobSearchFilter(None, Set.empty, None),
              first,
              SearchMode.VECTOR,
              embedding.meta.model,
              embedding.meta.version,
              searchId
            )
            search.candidateMatches(query).map(_.leftMap(_ => SearchError.VectorSearchUnavailable: UseCaseError))
          case Some(_) => SearchError.StaleEmbedding("job").asLeft[List[RankedCandidate]].pure[F]
          case None => SearchError.MissingEmbedding("job").asLeft[List[RankedCandidate]].pure[F]
        }
        }
    }

  private def resolveCandidate(actor: ActorContext): F[Either[UseCaseError, User]] =
    authorization.resolve(actor).map(_.flatMap { user =>
      if (user.role == UserRole.Candidate) user.asRight[UseCaseError]
      else DomainError.CandidateRequired.asLeft[User]
    })
}

object SemanticSearchService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      embeddings: EmbeddingService[F],
      search: SemanticSearchRepository[F],
      embeddingVersion: Int
  ): SemanticSearchService[F] =
    new SemanticSearchService(users, jobs, embeddings, search, embeddingVersion)
}
