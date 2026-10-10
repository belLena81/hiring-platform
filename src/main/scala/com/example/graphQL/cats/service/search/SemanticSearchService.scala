package com.example.graphQL.cats.service.search

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.{
  CandidateProfile,
  EmbeddingMeta,
  EntityEmbedding,
  Job,
  JobStatus,
  SearchMode,
  SearchableText,
  User,
  UserRole
}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.protocol.{SearchUseCases, UseCaseIO}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.search.{
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
      _ <- requireQueryLength(text)
      vector <- embedQuery(text)
      results <- searchRead(
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
      eligible <- validateJobHits(actor, filter, results)
    } yield eligible
      .take(first.value)
      .map(value =>
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
      user.candidateProfile.fold(EitherT.leftT[IO, List[RankedJob]](missingEmbedding("candidate")))(profile =>
        currentEmbedding(user.embedding, "candidate", candidateEmbeddingIsCurrent(profile, _)).flatMap { embedding =>
          val noFilter = JobSearchFilter(None, Set.empty, None)
          val query = VectorSearchQuery(
            embedding.values,
            None,
            noFilter,
            first,
            SearchMode.VECTOR,
            embedding.meta.model,
            searchId
          )
          searchRead(search.recommendedJobs(query))
            .flatMap(validateJobHits(actor, noFilter, _, Some(embedding)))
            .map(
              _.take(first.value)
                .map(value => value.copy(matchedSkills = SkillMatching.matched(profile.skills, value.job.skills)))
            )
        }
      )
    }

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
      job <- authorizeJobForMatching(user, UseCaseIO.found(jobs.find(jobId), "job"))
      normalizedQuery = queryText.map(_.trim).filter(_.nonEmpty)
      _ <- normalizedQuery.traverse_(requireQueryLength)
      validatedFilters <- EitherT.fromEither[IO](
        ValidatedCandidateMatchFilters
          .from(filters)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      embedding <- currentEmbedding(job.embedding, "job", jobEmbeddingIsCurrent(job, _))
      queryVector <- normalizedQuery.traverse(text => embedQuery(text).map(text -> _))
      query = VectorSearchQuery(
        embedding.values,
        queryVector.map(_._1),
        JobSearchFilter(None, Set.empty, None),
        first,
        SearchMode.VECTOR,
        embedding.meta.model,
        searchId,
        candidateQueryVector = queryVector.map(_._2.values),
        candidateFilters = validatedFilters
      )
      hits <- searchRead(search.candidateMatches(query))
      results <- validateCandidateHits(actor, jobId, embedding.meta, validatedFilters, hits)
    } yield results
      .take(first.value)
      .map(value => value.copy(matchedSkills = SkillMatching.matched(value.candidate.skills, job.skills)))

  private def validateJobHits(
      actor: ActorContext,
      filter: JobSearchFilter,
      hits: List[JobRetrievalHit],
      expectedActorEmbedding: Option[EntityEmbedding] = None
  ): UseCaseIO[List[RankedJob]] =
    for {
      (currentActor, scope) <- currentCandidate(actor)
      _ <- UseCaseIO.ensure(
        expectedActorEmbedding.forall(expected =>
          currentActor.candidateProfile.exists(profile =>
            currentActor.embedding.contains(expected) && candidateEmbeddingIsCurrent(profile, expected)
          )
        ),
        UseCaseError.Search(SearchError.StaleEmbedding("candidate"))
      )
      currentJobs <- searchRead(
        search.authorizedJobEligibility(
          scope,
          hits.map(_.id).distinct,
          expectedActorEmbedding.map(_ => CandidateSearchEligibility.fromUser(currentActor))
        )
      )
    } yield liveHits(hits, currentJobs)(_.id, _.job.id) { (hit, value) =>
      Option
        .when(SearchEligibilityPolicy.job(value, hit.meta, embeddingModel, filter))(
          RankedJob(value.job, hit.score, hit.mode, hit.meta, hit.searchId, retrievalScore = hit.retrievalScore)
        )
    }

  /** Re-validates the search hits against the live actor and job, since both may change during the search. */
  private def validateCandidateHits(
      actor: ActorContext,
      jobId: JobId,
      queryMeta: EmbeddingMeta,
      filters: ValidatedCandidateMatchFilters,
      hits: List[CandidateRetrievalHit]
  ): UseCaseIO[List[RankedCandidate]] =
    for {
      (currentActor, scope) <- authorization.readScope(actor).leftMap(searchFailure)
      currentJob <- authorizeJobForMatching(
        currentActor,
        UseCaseIO.found(jobs.find(jobId), "job").leftMap(searchFailure)
      )
      _ <- UseCaseIO.ensure(
        currentJob.embedding.exists(embedding =>
          embedding.meta == queryMeta && jobEmbeddingIsCurrent(currentJob, embedding)
        ),
        UseCaseError.Search(SearchError.StaleEmbedding("job"))
      )
      currentUsers <- searchRead(
        search.authorizedCandidateEligibility(scope, JobSearchEligibility.fromJob(currentJob), hits.map(_.id).distinct)
      )
    } yield liveHits(hits, currentUsers)(_.id, _.id) { (hit, value) =>
      for {
        _ <- Option.when(SearchEligibilityPolicy.candidate(value, hit.meta, embeddingModel, filters))(())
        profile <- value.profile
        name <- value.name
      } yield RankedCandidate(
        CandidateSearchHit(value.id, name, profile.skills, profile.experienceSummary),
        hit.score,
        hit.mode,
        hit.meta,
        hit.searchId,
        retrievalScore = hit.retrievalScore
      )
    }

  /** A non-Candidate user matching candidates against an open job it may manage. */
  private def authorizeJobForMatching(user: User, job: UseCaseIO[Job]): UseCaseIO[Job] =
    for {
      _ <- UseCaseIO.ensure(user.role != UserRole.Candidate, UseCaseError.Domain(DomainError.RecruiterRequired))
      found <- job
      _ <- UseCaseIO.ensure(
        user.role != UserRole.Recruiter || found.recruiterId == user.id,
        UseCaseError.Domain(DomainError.Forbidden)
      )
      _ <- UseCaseIO.ensure(found.status == JobStatus.Open, UseCaseError.Domain(DomainError.JobMustBeOpen))
    } yield found

  /** Keeps each distinct hit whose entity still exists, ranking it only if it is still eligible. */
  private def liveHits[Id, H, E, R](hits: List[H], current: List[E])(hitId: H => Id, entityId: E => Id)(
      rank: (H, E) => Option[R]
  ): List[R] = {
    val byId = current.iterator.map(entity => entityId(entity) -> entity).toMap
    hits.distinctBy(hitId).flatMap(hit => byId.get(hitId(hit)).flatMap(rank(hit, _)))
  }

  private def missingEmbedding(entity: String): UseCaseError = UseCaseError.Search(SearchError.MissingEmbedding(entity))

  /** An embedding must exist and match the current source text and model. */
  private def currentEmbedding(
      embedding: Option[EntityEmbedding],
      entity: String,
      isCurrent: EntityEmbedding => Boolean
  ): UseCaseIO[EntityEmbedding] =
    EitherT.fromEither[IO](
      embedding
        .toRight(missingEmbedding(entity))
        .filterOrElse(isCurrent, UseCaseError.Search(SearchError.StaleEmbedding(entity)))
    )

  private def requireQueryLength(text: String): UseCaseIO[Unit] =
    UseCaseIO.ensure(
      text.length <= SearchableText.QueryMaxChars,
      UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars))
    )

  private def jobEmbeddingIsCurrent(job: Job, embedding: EntityEmbedding): Boolean =
    embedding.meta.model == embeddingModel && embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))

  private def candidateEmbeddingIsCurrent(profile: CandidateProfile, embedding: EntityEmbedding): Boolean =
    embedding.meta.model == embeddingModel &&
      embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))

  private def requireCandidate(user: User): UseCaseIO[Unit] =
    UseCaseIO.ensure(user.role == UserRole.Candidate, UseCaseError.Domain(DomainError.CandidateRequired))

  private def resolveCandidate(actor: ActorContext): UseCaseIO[User] =
    authorization.resolve(actor).flatTap(requireCandidate)

  private def currentCandidate(actor: ActorContext): UseCaseIO[(User, HiringReadScope)] =
    authorization.readScope(actor).leftMap(searchFailure).flatTap((user, _) => requireCandidate(user))

  private def embedQuery(text: String): UseCaseIO[EmbeddingVector] =
    EitherT
      .liftF(embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)))
      .subflatMap(
        _.flatMap(EmbeddingVector.validateModel(_, embeddingModel))
          .leftMap(_ => UseCaseError.Search(SearchError.ProviderUnavailable))
      )

  /** A revoked authority stays forbidden and a write conflict stays a conflict; other store failures mean the vector
    * search backend is unavailable.
    */
  private def searchFailure(error: UseCaseError): UseCaseError =
    error match {
      case UseCaseError.Repository(RepositoryError.Conflict) | UseCaseError.Domain(DomainError.Forbidden) => error
      case UseCaseError.Repository(_) => UseCaseError.Search(SearchError.VectorSearchUnavailable)
      case other                      => other
    }

  private def searchRead[A](result: RepositoryIO[A]): UseCaseIO[A] =
    UseCaseIO.repository(result).leftMap(searchFailure)
}
