package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLInputs.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLResolverSupport.*
import com.example.graphQL.cats.service.search.{RankedCandidate, RankedJob}
import com.example.graphQL.cats.service.search.{
  JobFacetQuery,
  NearbyRadius,
  JobSearchFilter,
  NearbyJobsQuery,
  JobDiscoveryValidation
}
import com.example.graphQL.cats.service.UseCaseError
import com.example.graphQL.cats.domain.model.GeoPoint
import com.example.graphQL.cats.service.search.CandidateMatchFilters
import io.circe.Json
import sangria.schema.Context

private[graphql] object HiringGraphQLSearchResolvers {
  def nearbyJobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[NearbyJobsResults] =
    authenticated(context) { case (actor, hiring) =>
      val center = context.arg(nearbyCenterArgument)
      val inputFilter = context.arg(nearbyFilterArgument)
      for {
        limit <- inputResult(pageSize(context.arg(firstArgument)))
        filter <- inputResult(discoveryFilter(inputFilter))
        baseQuery = NearbyJobsQuery(GeoPoint(center.latitude, center.longitude), context.arg(radiusKmArgument), filter)
        cursor <- inputResult(
          context
            .arg(afterArgument)
            .traverse(value => NearbyJobsQuery.decodeCursor(value, baseQuery))
            .leftMap(error => GraphQLFailure("INVALID_CURSOR", error.message, exceptional = false))
        )
        query = baseQuery.copy(after = cursor)
        values <- raiseOnUseCaseError(
          hiring.jobService.nearbyJobs(
            actor,
            query,
            limit.value
          )
        )
      } yield {
        val page = values.take(limit.value)
        NearbyJobsResults(
          page.map(value =>
            NearbyJobResult(
              value.job,
              value.distanceKm,
              NearbyJobsQuery.encodeCursor(value.distanceKm, value.job.id, baseQuery)
            )
          ),
          values.size > limit.value
        )
      }
    }

  def jobDiscoveryFacets(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[com.example.graphQL.cats.service.search.JobDiscoveryFacets] =
    authenticated(context) { case (actor, hiring) =>
      val inputFilter = context.arg(nearbyFilterArgument)
      val radiusArgs = (context.arg(optionalNearbyCenterArgument), context.arg(optionalRadiusKmArgument)) match {
        case (None, None)                 => Right(None)
        case (Some(center), Some(radius)) =>
          Right(Some(NearbyRadius(GeoPoint(center.latitude, center.longitude), radius)))
        case _ =>
          Left(GraphQLFailure("INVALID_FILTER", "Center and radiusKm must be supplied together", exceptional = false))
      }
      for {
        filter <- inputResult(discoveryFilter(inputFilter))
        radius <- inputResult(radiusArgs)
        facets <- raiseOnUseCaseError(hiring.jobService.jobDiscoveryFacets(actor, JobFacetQuery(filter, radius)))
      } yield facets
    }

  private def discoveryFilter(value: Option[NearbyJobsFilterGraphQLInput]): Either[GraphQLFailure, JobSearchFilter] =
    JobDiscoveryValidation
      .filter(
        JobSearchFilter(
          value.flatMap(_.city),
          value.flatMap(_.skills).getOrElse(Nil).toSet,
          value.flatMap(_.createdAfter)
        )
      )
      .toEither
      .leftMap(errors => toGraphQLFailure(UseCaseError.Search(errors.head)))

  def semanticJobSearch(context: Context[RequestContext, Unit]): HiringGraphQLResult[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      val filter = jobFilter(context.arg(jobFilterArgument))
      for {
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, HiringGraphQLFailure, java.util.UUID](
          context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        )
        results <- raiseOnUseCaseError(
          service.semanticJobSearch(actor, context.arg(queryArgument), filter, size, searchId)
        )
        _ <- EitherT.liftF[IO, HiringGraphQLFailure, Unit](
          saveSearchSession(
            hiring,
            actor.userId,
            "semanticJobSearch",
            searchId,
            filterJson(filter),
            results.headOption.map(_.meta.model)
          )(results)(_.job.id.value.toString, _.score)
        )
      } yield rankedJobResults(results)
    }

  def recommendedJobs(context: Context[RequestContext, Unit]): HiringGraphQLResult[RankedJobResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      for {
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, HiringGraphQLFailure, java.util.UUID](
          context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        )
        results <- raiseOnUseCaseError(service.recommendedJobs(actor, size, searchId))
        _ <- EitherT.liftF[IO, HiringGraphQLFailure, Unit](
          saveSearchSession(
            hiring,
            actor.userId,
            "recommendedJobs",
            searchId,
            Json.obj(),
            results.headOption.map(_.meta.model)
          )(results)(_.job.id.value.toString, _.score)
        )
      } yield rankedJobResults(results)
    }

  def candidateMatches(context: Context[RequestContext, Unit]): HiringGraphQLResult[RankedCandidateResults] =
    authenticatedSearch(context).flatMap { case (actor, hiring, service) =>
      val jobId = context.arg(jobIdArgument)
      val matchFilter = context.arg(candidateMatchFilterArgument)
      val candidateFilters = CandidateMatchFilters(
        matchFilter.flatMap(_.requiredSkills).getOrElse(Nil),
        matchFilter.flatMap(_.country).map(_.trim.toLowerCase(java.util.Locale.ROOT)),
        matchFilter.flatMap(_.city).map(_.trim.toLowerCase(java.util.Locale.ROOT)),
        matchFilter.flatMap(_.availabilityStatus).map(_.toString)
      )
      for {
        size <- inputResult(pageSize(context.arg(firstArgument)))
        searchId <- EitherT.liftF[IO, HiringGraphQLFailure, java.util.UUID](
          context.arg(searchIdArgument).fold(IO.randomUUID)(IO.pure)
        )
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
        _ <- EitherT.liftF[IO, HiringGraphQLFailure, Unit](
          saveSearchSession(
            hiring,
            actor.userId,
            "candidateMatches",
            searchId,
            Json.obj("jobId" -> Json.fromString(jobId.value.toString)),
            results.headOption.map(_.meta.model)
          )(results)(_.candidate.id.value.toString, _.score)
        )
      } yield rankedCandidateResults(results)
    }

  private def rankedJobResults(values: List[RankedJob]): RankedJobResults =
    RankedJobResults(
      values.map(value =>
        RankedJobPayload(
          value.job,
          value.score,
          value.mode,
          value.meta.model,
          value.searchId.toString,
          value.matchedSkills,
          value.retrievalScore
        )
      )
    )

  private def rankedCandidateResults(values: List[RankedCandidate]): RankedCandidateResults =
    RankedCandidateResults(
      values.map(value =>
        RankedCandidatePayload(
          CandidateMatchCandidate(
            value.candidate.id.value.toString,
            value.candidate.name,
            Some(CandidateMatchProfile(value.candidate.skills, value.candidate.experienceSummary))
          ),
          value.score,
          value.mode,
          value.meta.model,
          value.searchId.toString,
          value.matchedSkills,
          value.retrievalScore
        )
      )
    )
}
