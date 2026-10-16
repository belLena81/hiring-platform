package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.service.search.{DegradedSearch, SearchResults, SearchedCandidate, SearchedJob}
import com.example.graphQL.cats.service.search.{JobFacetQuery, NearbyRadius, NearbyJobCursor, NearbyJobsQuery}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.SearchFilterSnapshot
import com.example.graphQL.cats.domain.model.GeoPoint
import com.example.graphQL.cats.service.search.CandidateMatchFilters
import java.time.Instant
import sangria.schema.Context

private[graphql] object HiringGraphQLSearchResolvers {
  def nearbyJobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[NearbyJobsConnection] =
    authenticated(context) { case (actor, hiring) =>
      given CursorCodec.CursorKey = hiring.cursorKey
      val center = context.arg(nearbyCenterArgument)
      val inputFilter = context.arg(jobFilterArgument)
      for {
        now <- EitherT.liftF[IO, HiringGraphQLFailure, Instant](IO.realTimeInstant)
        limit <- inputResult(pageSize(context.arg(firstArgument)))
        filter <- inputResult(discoveryFilter(inputFilter))
        baseQuery = NearbyJobsQuery(GeoPoint(center.latitude, center.longitude), context.arg(radiusKmArgument), filter)
        cursor <- inputResult(context.arg(afterArgument).traverse(decodeNearbyCursor(_, baseQuery, now)))
        query = baseQuery.copy(after = cursor)
        values <- raiseOnUseCaseError(
          hiring.jobService.nearbyJobs(
            actor,
            query,
            limit.value
          )
        )
      } yield {
        val edges = values
          .take(limit.value)
          .map(value =>
            NearbyJobEdge(
              value.job,
              value.distanceKm,
              CursorCodec.encode(NearbyJobCursor(value.distanceKm, value.job.id, baseQuery.fingerprint), now)
            )
          )
        NearbyJobsConnection(edges, PageInfo(values.size > limit.value, edges.lastOption.map(_.cursor)))
      }
    }

  private def decodeNearbyCursor(value: String, query: NearbyJobsQuery, now: Instant)(using
      CursorCodec.CursorKey
  ): Either[GraphQLFailure, NearbyJobCursor] =
    CursorCodec
      .decode[NearbyJobCursor](value, now)
      .flatMap(cursor =>
        Either.cond(cursor.hasValidDistance, cursor, CursorCodec.CursorError.Malformed("Invalid cursor distance"))
      )
      .flatMap(cursor => Either.cond(cursor.isBoundTo(query), cursor, CursorCodec.CursorError.CriteriaMismatch))
      .leftMap(GraphQLFailureCatalog.classifyCursor)

  def jobDiscoveryFacets(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[Option[FacetsOutcome[com.example.graphQL.cats.service.search.JobDiscoveryFacets]]] =
    facetsResult(jobDiscoveryFacetsBody(context), InputFieldPaths.jobDiscoveryFacets).map(Some(_))

  private def jobDiscoveryFacetsBody(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[com.example.graphQL.cats.service.search.JobDiscoveryFacets] =
    authenticated(context) { case (actor, hiring) =>
      val inputFilter = context.arg(jobFilterArgument)
      val radiusArgs = (context.arg(optionalNearbyCenterArgument), context.arg(optionalRadiusKmArgument)) match {
        case (None, None)                 => Right(None)
        case (Some(center), Some(radius)) =>
          Right(Some(NearbyRadius(GeoPoint(center.latitude, center.longitude), radius)))
        case _ =>
          Left(GraphQLFailureCatalog.inconsistentArguments("Center and radiusKm must be supplied together"))
      }
      for {
        filter <- inputResult(discoveryFilter(inputFilter))
        radius <- inputResult(radiusArgs)
        facets <- raiseOnUseCaseError(
          hiring.jobService
            .jobDiscoveryFacets(actor, JobFacetQuery(filter, radius, context.arg(optionalFacetQueryArgument)))
        )
      } yield facets
    }

  def semanticJobSearch(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[Option[SearchOutcome[RankedJobResults]]] =
    searchResult(semanticJobSearchBody(context), InputFieldPaths.semanticJobSearch).map(Some(_))

  private def semanticJobSearchBody(context: Context[RequestContext, Unit]): HiringGraphQLResult[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      for {
        filter <- inputResult(discoveryFilter(context.arg(jobFilterArgument)))
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- searchIdFor(hiring, context.arg(searchIdArgument).map(_.value))
        results <- raiseOnUseCaseError(
          service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId)
        )
        _ <- recordSearch(
          context.ctx,
          hiring,
          actor.userId,
          SearchKind.SemanticJobSearch,
          searchId,
          filterSnapshot(filter),
          results.hits.headOption.flatMap(_.model)
        )(results.hits)(_.job.id.value.toString, _.score)
      } yield rankedJobResults(results)
    }

  def recommendedJobs(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[Option[SearchOutcome[RankedJobResults]]] =
    searchResult(recommendedJobsBody(context), InputFieldPaths.recommendedJobs).map(Some(_))

  private def recommendedJobsBody(context: Context[RequestContext, Unit]): HiringGraphQLResult[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      for {
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- searchIdFor(hiring, context.arg(searchIdArgument).map(_.value))
        results <- raiseOnUseCaseError(service.recommendedJobs(actor, size, searchId))
        _ <- recordSearch(
          context.ctx,
          hiring,
          actor.userId,
          SearchKind.RecommendedJobs,
          searchId,
          SearchFilterSnapshot.Unfiltered,
          results.headOption.map(_.meta.model)
        )(results)(_.job.id.value.toString, _.score)
      } yield rankedJobResults(SearchResults(results.map(SearchedJob.from), None))
    }

  def candidateMatches(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[Option[SearchOutcome[RankedCandidateResults]]] =
    searchResult(candidateMatchesBody(context), InputFieldPaths.candidateMatches).map(Some(_))

  private def candidateMatchesBody(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[RankedCandidateResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      val jobId = context.arg(jobIdArgument)
      val matchFilter = context.arg(candidateMatchFilterArgument)
      val candidateFilters = CandidateMatchFilters(
        matchFilter.flatMap(_.requiredSkills).getOrElse(Nil),
        matchFilter.flatMap(_.country),
        matchFilter.flatMap(_.city),
        matchFilter.flatMap(_.availabilityStatus).map(_.toString)
      )
      for {
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- searchIdFor(hiring, context.arg(searchIdArgument).map(_.value))
        results <- raiseOnUseCaseError(
          service.candidateMatches(
            actor,
            jobId,
            context.arg(candidateSearchQueryArgument),
            candidateFilters,
            size,
            searchId
          )
        )
        _ <- recordSearch(
          context.ctx,
          hiring,
          actor.userId,
          SearchKind.CandidateMatches,
          searchId,
          SearchFilterSnapshot.ForJob(jobId),
          results.hits.headOption.flatMap(_.model)
        )(results.hits)(_.candidate.id.value.toString, _.score)
      } yield rankedCandidateResults(results)
    }

  private def rankedJobResults(found: SearchResults[SearchedJob]): RankedJobResults =
    RankedJobResults(
      found.hits.map(value =>
        RankedJobPayload(
          value.job,
          value.score,
          value.mode,
          value.model,
          SearchId(value.searchId),
          value.matchedSkills,
          value.retrievalScore
        )
      ),
      found.degraded.isDefined,
      found.degraded.map(degradedReasonCode)
    )

  private def rankedCandidateResults(found: SearchResults[SearchedCandidate]): RankedCandidateResults =
    RankedCandidateResults(
      found.hits.map(value =>
        RankedCandidatePayload(
          CandidateMatchCandidate(
            value.candidate.id,
            value.candidate.name,
            Some(CandidateMatchProfile(value.candidate.skills, value.candidate.experienceSummary))
          ),
          value.score,
          value.mode,
          value.model,
          SearchId(value.searchId),
          value.matchedSkills,
          value.retrievalScore
        )
      ),
      found.degraded.isDefined,
      found.degraded.map(degradedReasonCode)
    )

  /** The code the degraded answer reports; only the two retrieval failures can degrade a search. */
  private def degradedReasonCode(degraded: DegradedSearch): String =
    GraphQLFailureCatalog.classify(UseCaseError.Search(degraded.reason)).code
}
