package com.example.graphQL.cats.service.search

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.{JobStatus, SearchMode, SearchableText, User, UserRole}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.{SearchUseCases, UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.PageSize
import com.example.graphQL.cats.shared.search.{
  CandidateMatchFilters,
  JobSearchFilter,
  RankedCandidate,
  RankedJob,
  SkillMatching,
  VectorSearchQuery
}
import java.util.UUID

final class SemanticSearchService(
    users: UserRepository,
    jobs: JobRepository,
    embeddings: EmbeddingService,
    search: SemanticSearchRepository,
    embeddingModel: String
) extends SearchUseCases {
  private val authorization = ActorAuthorization(users)

  def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedJob]] =
    for {
      candidate <- resolveCandidate(actor)
      _ <- UseCase.fromEither(
        Either.cond(
          text.length <= SearchableText.QueryMaxChars,
          (),
          UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars))
        )
      )
      vector <- embedQuery(text)
      results <- vectorSearch(
        search.searchJobs(
          VectorSearchQuery(
            vector.values,
            Some(text),
            filter,
            first,
            SearchMode.HYBRID,
            embeddingModel,
            searchId
          )
        )
      )
    } yield results.map(value =>
      value.copy(matchedSkills =
        SkillMatching.matched(candidate.candidateProfile.fold(Set.empty[String])(_.skills), value.job.skills)
      )
    )

  def recommendedJobs(
      actor: ActorContext,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedJob]] =
    resolveCandidate(actor).flatMap { user =>
      (user.candidateProfile, user.embedding) match {
        case (Some(profile), Some(embedding))
            if embedding.meta.model == embeddingModel &&
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
          vectorSearch(search.recommendedJobs(query)).map(values =>
            values.map(value => value.copy(matchedSkills = SkillMatching.matched(profile.skills, value.job.skills)))
          )
        case (Some(_), Some(_)) => UseCase.left(UseCaseError.Search(SearchError.StaleEmbedding("candidate")))
        case _                  => UseCase.left(UseCaseError.Search(SearchError.MissingEmbedding("candidate")))
      }
    }

  def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedCandidate]] =
    candidateMatches(actor, jobId, None, CandidateMatchFilters.empty, first, searchId)

  override def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      queryText: Option[String],
      filters: CandidateMatchFilters,
      first: PageSize,
      searchId: UUID
  ): UseCaseIO[List[RankedCandidate]] =
    for {
      user <- authorization.resolve(actor)
      _ <- UseCase.fromEither(
        Either.cond(user.role != UserRole.Candidate, (), UseCaseError.Domain(DomainError.RecruiterRequired))
      )
      job <- UseCase
        .repository(jobs.find(jobId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- UseCase.fromEither(
        Either.cond(
          user.role != UserRole.Recruiter || job.recruiterId == user.id,
          (),
          UseCaseError.Domain(DomainError.Forbidden)
        )
      )
      _ <- UseCase.fromEither(
        Either.cond(job.status == JobStatus.Open, (), UseCaseError.Domain(DomainError.JobMustBeOpen))
      )
      normalizedQuery <- UseCase.fromEither(
        queryText.map(_.trim).filter(_.nonEmpty) match {
          case Some(text) if text.length > SearchableText.QueryMaxChars =>
            Left(UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars)))
          case value => Right(value)
        }
      )
      _ <- UseCase.fromEither(validateCandidateFilters(filters))
      results <- job.embedding match {
        case Some(embedding)
            if embedding.meta.model == embeddingModel &&
              embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job)) =>
          val query = VectorSearchQuery(
            embedding.values,
            None,
            JobSearchFilter(None, Set.empty, None),
            first,
            SearchMode.VECTOR,
            embedding.meta.model,
            searchId,
            candidateFilters = filters
          )
          normalizedQuery match {
            case Some(text) =>
              embedQuery(text).flatMap(vector =>
                vectorSearch(
                  search.candidateMatches(
                    query.copy(
                      lexicalQuery = Some(text),
                      candidateQueryVector = Some(vector.values)
                    )
                  )
                ).map(values =>
                  values.map(value =>
                    value.copy(
                      matchedSkills = SkillMatching.matched(value.candidate.skills, job.skills)
                    )
                  )
                )
              )
            case None =>
              vectorSearch(search.candidateMatches(query)).map(values =>
                values.map(value =>
                  value.copy(
                    matchedSkills = SkillMatching.matched(value.candidate.skills, job.skills)
                  )
                )
              )
          }
        case Some(_) => UseCase.left(UseCaseError.Search(SearchError.StaleEmbedding("job")))
        case None    => UseCase.left(UseCaseError.Search(SearchError.MissingEmbedding("job")))
      }
    } yield results

  private def validateCandidateFilters(filters: CandidateMatchFilters): Either[UseCaseError, Unit] =
    val validSkills = filters.requiredSkills.size <= 100 && filters.requiredSkills.forall(skill =>
      skill.trim.nonEmpty && skill.trim.length <= 256
    )
    val validLocations = List(filters.countryCanonical, filters.cityCanonical).flatten.forall(value =>
      value.trim.nonEmpty && value.trim.length <= 256
    ) && (filters.cityCanonical.isEmpty || filters.countryCanonical.nonEmpty)
    if (!validSkills)
      Left(UseCaseError.Search(SearchError.InvalidFilter("requiredSkills")))
    else if (!validLocations)
      Left(UseCaseError.Search(SearchError.InvalidFilter("residence")))
    else Right(())

  private def resolveCandidate(actor: ActorContext): UseCaseIO[User] =
    authorization.resolve(actor).subflatMap { user =>
      if (user.role == UserRole.Candidate) user.asRight[UseCaseError]
      else UseCaseError.Domain(DomainError.CandidateRequired).asLeft[User]
    }

  private def embedQuery(text: String): UseCaseIO[EmbeddingVector] =
    UseCase
      .liftIO(embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)))
      .subflatMap(_.leftMap(_ => UseCaseError.Search(SearchError.ProviderUnavailable)))

  private def vectorSearch[A](result: IO[Either[RepositoryError, A]]): UseCaseIO[A] =
    UseCase.repository(result).leftMap(_ => UseCaseError.Search(SearchError.VectorSearchUnavailable))
}

object SemanticSearchService {
  def apply(
      users: UserRepository,
      jobs: JobRepository,
      embeddings: EmbeddingService,
      search: SemanticSearchRepository,
      embeddingModel: String
  ): SemanticSearchService =
    new SemanticSearchService(users, jobs, embeddings, search, embeddingModel)
}
