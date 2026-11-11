package com.example.graphQL.cats.service.search

import cats.data.EitherT
import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
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
import com.example.graphQL.cats.service.protocol.{SearchUseCases, UseCaseF}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.service.search.SearchFailure.*
import com.example.graphQL.cats.service.search.{
  CandidateMatchFilters,
  JobSearchFilter,
  RankedCandidate,
  RankedJob,
  SkillMatching,
  VectorSearchQuery
}
import java.util.UUID

final class SemanticSearchService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddings: EmbeddingService[F],
    search: SemanticSearchRepository[F],
    embeddingModel: String
) extends SearchUseCases[F] {
  private val authorization = ActorAuthorization(users)

  /** Ranked open jobs for a query. It answers from open-job lexical retrieval, marked degraded, when only the query
    * embedding provider or `$vectorSearch` is unavailable. Authorization and input checks precede any provider call.
    */
  override def semanticJobSearch(
      actor: ActorContext,
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, SearchResults[SearchedJob]] =
    for {
      (candidate, scope) <- currentCandidate(actor)
      _ <- requireQueryLength(text)
      retrieved <- degradable(vectorJobRetrieval(text, filter, first, searchId))
      results <- retrieved match {
        case Right(hits) =>
          validateJobHits(actor, filter, hits).map(ranked => SearchResults(ranked.map(SearchedJob.from), None))
        case Left(degraded) =>
          lexicalFallbackText(Some(text)).fold[UseCaseF[F, SearchResults[SearchedJob]]](
            EitherT.leftT(UseCaseError.Search(degraded.reason))
          )(lexicalText =>
            lexicalJobHits(scope, LexicalSearchQuery(lexicalText, filter, first, searchId))
              .map(hits => SearchResults(hits, Some(degraded)))
          )
      }
    } yield results.copy(hits = results.hits.take(first.value).map(value => withJobFitReasons(candidate, value)))

  def recommendedJobs(
      actor: ActorContext,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, List[RankedJob]] =
    resolveCandidate(actor).flatMap { user =>
      user.candidateProfile.fold(EitherT.leftT[F, List[RankedJob]](missingEmbedding("candidate")))(profile =>
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
                .map { value =>
                  val (matched, missing) = SkillMatching.fitReasons(profile.skills, value.job.skills)
                  value.copy(matchedSkills = matched, missingSkills = missing)
                }
            )
        }
      )
    }

  /** Ranked candidates for a job the actor manages. It answers from lexical retrieval, marked degraded, when only the
    * query embedding provider or `$vectorSearch` is unavailable. A missing or stale job embedding is not degradable.
    * The fallback text is the query, else the job's skills; with neither, the original failure is returned.
    */
  override def candidateMatches(
      actor: ActorContext,
      jobId: JobId,
      queryText: Option[String],
      filters: CandidateMatchFilters,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, SearchResults[SearchedCandidate]] =
    for {
      prepared <- prepareCandidateMatch(actor, jobId, queryText, filters)
      retrieved <- degradable(vectorCandidateRetrieval(prepared, first, searchId))
      results <- retrieved match {
        case Right(hits) =>
          validateCandidateHits(actor, jobId, prepared.embedding.meta, prepared.filters, hits)
            .map(ranked => SearchResults(ranked.map(SearchedCandidate.from), None))
        case Left(degraded) =>
          lexicalFallbackText(prepared.queryText.orElse(Some(jobSkillsText(prepared.job)))).fold[
            UseCaseF[F, SearchResults[SearchedCandidate]]
          ](EitherT.leftT(UseCaseError.Search(degraded.reason)))(lexicalText =>
            lexicalCandidateHits(
              actor,
              jobId,
              prepared.embedding.meta,
              LexicalSearchQuery(lexicalText, JobSearchFilter(None, Set.empty, None), first, searchId, prepared.filters)
            ).map(hits => SearchResults(hits, Some(degraded)))
          )
      }
    } yield results.copy(hits =
      results.hits
        .take(first.value)
        .map(value => withCandidateFitReasons(prepared.job, value))
    )

  /** The authorized job, normalized query and filters, and the job's current embedding, validated before any provider
    * call.
    */
  private def prepareCandidateMatch(
      actor: ActorContext,
      jobId: JobId,
      queryText: Option[String],
      filters: CandidateMatchFilters
  ): UseCaseF[F, PreparedCandidateMatch] =
    for {
      user <- authorization.resolve(actor)
      job <- authorizeJobForMatching(user, withEmbedding(jobId))
      normalizedQuery = queryText.map(_.trim).filter(_.nonEmpty)
      _ <- normalizedQuery.traverse_(requireQueryLength)
      validatedFilters <- EitherT.fromEither[F](
        ValidatedCandidateMatchFilters
          .from(filters)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      embedding <- currentEmbedding(job.embedding, "job", jobEmbeddingIsCurrent(job, _))
    } yield PreparedCandidateMatch(job, normalizedQuery, validatedFilters, embedding)

  private def vectorCandidateRetrieval(
      prepared: PreparedCandidateMatch,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, List[CandidateRetrievalHit]] =
    for {
      queryVector <- prepared.queryText.traverse(text => embedQuery(text).map(text -> _))
      query = VectorSearchQuery(
        prepared.embedding.values,
        queryVector.map(_._1),
        JobSearchFilter(None, Set.empty, None),
        first,
        SearchMode.VECTOR,
        prepared.embedding.meta.model,
        searchId,
        candidateQueryVector = queryVector.map(_._2.values),
        candidateFilters = prepared.filters
      )
      hits <- UseCaseF.repository(search.candidateMatches(query))
    } yield hits

  private def vectorJobRetrieval(
      text: String,
      filter: JobSearchFilter,
      first: PageSize,
      searchId: UUID
  ): UseCaseF[F, List[JobRetrievalHit]] =
    embedQuery(text).flatMap(vector =>
      UseCaseF.repository(
        search.searchJobs(
          VectorSearchQuery(vector.values, Some(text), filter, first, SearchMode.HYBRID, embeddingModel, searchId)
        )
      )
    )

  /** Fit reasons for a candidate viewing a job: derived only from the job's skills and the caller's own profile. */
  private def withJobFitReasons(candidate: User, hit: SearchedJob): SearchedJob = {
    val own = candidate.candidateProfile.fold(Set.empty[String])(_.skills)
    val (matched, missing) = SkillMatching.fitReasons(own, hit.job.skills)
    hit.copy(matchedSkills = matched, missingSkills = missing)
  }

  /** Fit reasons for a recruiter viewing a candidate of an owned job: only the job's skills and the skills the hit
    * already carries to the recruiter.
    */
  private def withCandidateFitReasons(job: Job, hit: SearchedCandidate): SearchedCandidate = {
    val (matched, missing) = SkillMatching.fitReasons(hit.candidate.skills, job.skills)
    hit.copy(matchedSkills = matched, missingSkills = missing)
  }

  /** Turns exactly the degradable retrieval failures (`SearchFailure.degradation`) into a value. The retrieval passes
    * in unmapped, so a generic `Unavailable` (possibly the whole store), an `Overloaded` refusal (answering again
    * through another path would repeat the load) and corrupt stored data are told apart from a lone vector failure:
    * they are not degraded and keep their own reading through `asSearchFailure`.
    */
  private def degradable[A](retrieval: UseCaseF[F, A]): UseCaseF[F, Either[DegradedSearch, A]] =
    retrieval.transform {
      case Right(value) => Right(Right(value))
      case Left(error)  => error.degradation.map(Left(_)).toRight(error.asSearchFailure)
    }

  /** A usable, bounded lexical query: trimmed, within the query limit, and non-empty. */
  private def lexicalFallbackText(text: Option[String]): Option[String] =
    text.map(_.trim.take(SearchableText.QueryMaxChars).trim).filter(_.nonEmpty)

  private def jobSkillsText(job: Job): String = job.skills.toList.sorted.mkString(" ")

  /** Open jobs by text relevance, re-checked against the live job, filter and actor; no embedding is needed. */
  private def lexicalJobHits(scope: HiringReadScope, query: LexicalSearchQuery): UseCaseF[F, List[SearchedJob]] =
    for {
      hits <- searchRead(search.lexicalJobs(query))
      current <- searchRead(search.authorizedJobEligibility(scope, hits.map(_.id).distinct, None))
    } yield liveHits(hits, current)(_.id, _.job.id) { (hit, value) =>
      Option.when(SearchEligibilityPolicy.jobLexical(value, query.filter))(
        SearchedJob(value.job, hit.score, SearchMode.LEXICAL, None, hit.searchId)
      )
    }

  /** Candidates by text relevance, re-checked against the live actor, open job and consent-gated filters. */
  private def lexicalCandidateHits(
      actor: ActorContext,
      jobId: JobId,
      queryMeta: EmbeddingMeta,
      query: LexicalSearchQuery
  ): UseCaseF[F, List[SearchedCandidate]] =
    for {
      hits <- searchRead(search.lexicalCandidates(query))
      current <- currentCandidateEligibility(actor, jobId, queryMeta, hits.map(_.id))
    } yield liveHits(hits, current)(_.id, _.id) { (hit, value) =>
      for {
        _ <- Option.when(SearchEligibilityPolicy.candidateLexical(value, query.candidateFilters))(())
        profile <- value.profile
        name <- value.name
      } yield SearchedCandidate(
        CandidateSearchHit(value.id, name, profile.skills, profile.experienceSummary),
        hit.score,
        SearchMode.LEXICAL,
        None,
        hit.searchId
      )
    }

  private def validateJobHits(
      actor: ActorContext,
      filter: JobSearchFilter,
      hits: List[JobRetrievalHit],
      expectedActorEmbedding: Option[EntityEmbedding] = None
  ): UseCaseF[F, List[RankedJob]] =
    for {
      (currentActor, scope) <- currentCandidate(actor)
      _ <- UseCaseF.ensure[F](
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
  ): UseCaseF[F, List[RankedCandidate]] =
    currentCandidateEligibility(actor, jobId, queryMeta, hits.map(_.id)).map { currentUsers =>
      liveHits(hits, currentUsers)(_.id, _.id) { (hit, value) =>
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
    }

  /** The live eligibility of the hit candidates, read only while the actor may still manage the same open job. */
  private def currentCandidateEligibility(
      actor: ActorContext,
      jobId: JobId,
      queryMeta: EmbeddingMeta,
      ids: List[UserId]
  ): UseCaseF[F, List[CandidateSearchEligibility]] =
    for {
      (currentActor, scope) <- authorization.currentReadScope(actor).leftMap(_.asSearchFailure)
      // Read again after retrieval on purpose: the job may have been edited, closed or re-embedded meanwhile.
      currentJob <- authorizeJobForMatching(currentActor, withEmbedding(jobId).leftMap(_.asSearchFailure))
      _ <- UseCaseF.ensure[F](
        currentJob.embedding.exists(embedding =>
          embedding.meta == queryMeta && jobEmbeddingIsCurrent(currentJob, embedding)
        ),
        UseCaseError.Search(SearchError.StaleEmbedding("job"))
      )
      currentUsers <- searchRead(
        search.authorizedCandidateEligibility(scope, JobSearchEligibility.fromJob(currentJob), ids.distinct)
      )
    } yield currentUsers

  /** A non-Candidate user matching candidates against an open job it may manage. */
  private def authorizeJobForMatching(user: ActorRecord, job: UseCaseF[F, Job]): UseCaseF[F, Job] =
    for {
      _ <- UseCaseF.ensure[F](user.role != UserRole.Candidate, UseCaseError.Domain(DomainError.RecruiterRequired))
      found <- job
      _ <- UseCaseF.ensure[F](
        user.role != UserRole.Recruiter || found.recruiterId == user.id,
        UseCaseError.Domain(DomainError.NotFound("job"))
      )
      _ <- UseCaseF.ensure[F](found.status == JobStatus.Open, UseCaseError.Domain(DomainError.JobMustBeOpen))
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
  ): UseCaseF[F, EntityEmbedding] =
    EitherT.fromEither[F](
      embedding
        .toRight(missingEmbedding(entity))
        .filterOrElse(isCurrent, UseCaseError.Search(SearchError.StaleEmbedding(entity)))
    )

  private def requireQueryLength(text: String): UseCaseF[F, Unit] =
    UseCaseF.ensure[F](
      text.length <= SearchableText.QueryMaxChars,
      UseCaseError.Search(SearchError.InputTooLarge("query", SearchableText.QueryMaxChars))
    )

  private def jobEmbeddingIsCurrent(job: Job, embedding: EntityEmbedding): Boolean =
    embedding.meta.model == embeddingModel && embedding.meta.sourceHash == SourceHash.sha256(SearchableText.job(job))

  private def candidateEmbeddingIsCurrent(profile: CandidateProfile, embedding: EntityEmbedding): Boolean =
    embedding.meta.model == embeddingModel &&
      embedding.meta.sourceHash == SourceHash.sha256(SearchableText.candidate(profile))

  /** The whole job including its embedding: ranking compares the stored embedding with the current job text. */
  private def withEmbedding(jobId: JobId): UseCaseF[F, Job] =
    UseCaseF.found(jobs.findVersioned(jobId).map(_.map(_.value)), "job")

  private def requireCandidate(user: User): UseCaseF[F, Unit] =
    UseCaseF.ensure[F](user.role == UserRole.Candidate, UseCaseError.Domain(DomainError.CandidateRequired))

  private def resolveCandidate(actor: ActorContext): UseCaseF[F, User] =
    authorization.resolveWithEmbedding(actor).flatTap(requireCandidate)

  private def currentCandidate(actor: ActorContext): UseCaseF[F, (User, HiringReadScope)] =
    authorization.readScopeWithEmbedding(actor).leftMap(_.asSearchFailure).flatTap((user, _) => requireCandidate(user))

  private def embedQuery(text: String): UseCaseF[F, EmbeddingVector] =
    EitherT
      .liftF(embeddings.embed(EmbeddingInput(text, EmbeddingInputType.Query)))
      .subflatMap(
        _.flatMap(EmbeddingVector.validateModel(_, embeddingModel))
          .leftMap(_ => UseCaseError.Search(SearchError.ProviderUnavailable))
      )

  private def searchRead[A](result: RepositoryF[F, A]): UseCaseF[F, A] =
    UseCaseF.repository(result).leftMap(_.asSearchFailure)
}

/** Candidate matching inputs validated before any provider call. */
private final case class PreparedCandidateMatch(
    job: Job,
    queryText: Option[String],
    filters: ValidatedCandidateMatchFilters,
    embedding: EntityEmbedding
)
